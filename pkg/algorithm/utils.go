// MIT License
//
// Copyright (c) Microsoft Corporation. All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE

package algorithm

import (
	"fmt"
	"github.com/microsoft/hivedscheduler/pkg/api"
	"github.com/microsoft/hivedscheduler/pkg/common"
	"github.com/microsoft/hivedscheduler/pkg/internal"
	core "k8s.io/api/core/v1"
	"k8s.io/klog"
)

// generateAffinityGroupBindInfo writes the physical and virtual placements of an affinity group
// into a a series of AffinityGroupMemberBindInfos, and returns the allocated node and GPU addresses
// of the current pod.
func generateAffinityGroupBindInfo(
	groupPhysicalPlacement groupPhysicalPlacement,
	groupVirtualPlacement groupVirtualPlacement,
	cellLevelToType map[CellChain]map[CellLevel]api.CellType,
	currentGpuNum int32,
	currentPodIndex int32,
	group *AlgoAffinityGroup,
	groupName string,
	suggestedNodes common.Set) (
	affinityGroupBindInfo []api.AffinityGroupMemberBindInfo,
	nodesNotInSuggested []string,
	selectedNode string,
	selectedGpuIndices []int32,
	chain string) {

	if groupPhysicalPlacement == nil {
		return
	}
	affinityGroupBindInfo = make([]api.AffinityGroupMemberBindInfo, len(groupPhysicalPlacement))
	groupMemberIndex := 0
	for podGpuNum, podPhysicalPlacements := range groupPhysicalPlacement {
		mbi := api.AffinityGroupMemberBindInfo{
			PodPlacements: make([]api.PodPlacementInfo, len(podPhysicalPlacements)),
		}
		for podIndex := int32(0); podIndex < int32(len(podPhysicalPlacements)); podIndex++ {
			mbi.PodPlacements[podIndex].PhysicalGpuIndices = make([]int32, podGpuNum)
			mbi.PodPlacements[podIndex].PreassignedCellTypes = make([]api.CellType, podGpuNum)
			for gpuIndex := int32(0); gpuIndex < podGpuNum; gpuIndex++ {
				pGpu := podPhysicalPlacements[podIndex][gpuIndex]
				if pGpu == nil {
					if group == nil {
						panic(fmt.Sprintf("The first pod in group %v was allocated invalid resource", groupName))
					}
					// if the physical placement of this pod is not found (e.g., removed due to reconfiguration),
					// we will insist the decision by retrieving it from other pods
					mbi.PodPlacements[podIndex], chain = retrieveMissingPodPlacement(group, podGpuNum, podIndex)
					klog.Warningf(
						"pod placement has been invalid and is retrieved from annotation of other pods: node %v, GPU %v",
						mbi.PodPlacements[podIndex].PhysicalNode, mbi.PodPlacements[podIndex].PhysicalGpuIndices[gpuIndex])
				} else {
					nodes, gpuIndices := pGpu.(*PhysicalCell).GetPhysicalPlacement()
					// here each cell (i.e., pGpu) is only one GPU, hence we takes the first element
					// in its "nodes" and "gpuIndices" as the node and GPU address
					if mbi.PodPlacements[podIndex].PhysicalNode == "" {
						mbi.PodPlacements[podIndex].PhysicalNode = nodes[0]
						if !suggestedNodes.Contains(nodes[0]) {
							nodesNotInSuggested = append(nodesNotInSuggested, nodes[0])
						}
					}
					mbi.PodPlacements[podIndex].PhysicalGpuIndices[gpuIndex] = gpuIndices[0]
					if groupVirtualPlacement != nil {
						vGpu := groupVirtualPlacement[podGpuNum][podIndex][gpuIndex].(*VirtualCell)
						mbi.PodPlacements[podIndex].PreassignedCellTypes[gpuIndex] =
							cellLevelToType[vGpu.GetChain()][vGpu.GetPreAssignedCell().GetLevel()]
					} else {
						mbi.PodPlacements[podIndex].PreassignedCellTypes[gpuIndex] = ""
					}
				}
			}
		}
		if podGpuNum == currentGpuNum {
			selectedNode = mbi.PodPlacements[currentPodIndex].PhysicalNode
			selectedGpuIndices = mbi.PodPlacements[currentPodIndex].PhysicalGpuIndices
			if pGpu := groupPhysicalPlacement[currentGpuNum][currentPodIndex][0]; pGpu != nil {
				chain = string(pGpu.GetChain())
			}
		}
		affinityGroupBindInfo[groupMemberIndex] = mbi
		groupMemberIndex++
	}
	return affinityGroupBindInfo, nodesNotInSuggested, selectedNode, selectedGpuIndices, chain
}

// collectPreemptionVictims collects preemption victims of an affinity group.
// If any of the GPUs allocated for the whole group is still used by a pod,
// we will wait for the preemption, as a group is gang-scheduled.
func collectPreemptionVictims(
	groupPhysicalPlacement groupPhysicalPlacement,
	priority CellPriority,
	groupName string) (map[string]common.Set, []string) {

	preemptionVictims := map[string]common.Set{}
	var nodesHaveVictims []string
	for gpuNum := range groupPhysicalPlacement {
		for podIndex := range groupPhysicalPlacement[gpuNum] {
			for _, gpu := range groupPhysicalPlacement[gpuNum][podIndex] {
				if gpu == nil {
					continue
				}
				pGpu := gpu.(*PhysicalCell)
				if victimGroup := pGpu.GetAffinityGroup(); victimGroup != nil && victimGroup.name != groupName {
					// There are two cases of finding a running pod on the allocated resources:
					// 1. the running pod is a preemption victim.
					// 2. the running pod used resource partially released by the current group,
					// but the group wants to schedule a pod again.
					// Our principle is we allow preemption if the running pod's priority is lower than that
					// of the group to be scheduled (the 2nd case may also satisfy this condition, and we
					// allow such preemption). Otherwise the running pod cannot be preempted, and the pod
					// to be scheduled will wait.
					if pGpu.GetPriority() >= priority {
						panic(fmt.Sprintf(
							"Resources previously allocated (%v) has been allocated to "+
								"another non-preemptible group %v; pod should wait",
							pGpu.GetPhysicalPlacementString(), victimGroup.name))
					}
					// for any victim pod, gang-preempt all the other pods from the same affinity group
					for _, victims := range victimGroup.allocatedPods {
						for _, v := range victims {
							if v != nil {
								if _, ok := preemptionVictims[v.Spec.NodeName]; !ok {
									preemptionVictims[v.Spec.NodeName] = common.NewSet()
									nodesHaveVictims = append(nodesHaveVictims, v.Spec.NodeName)
								}
								preemptionVictims[v.Spec.NodeName].Add(v)
							}
						}
					}
				}
			}
		}
	}
	return preemptionVictims, nodesHaveVictims
}

// retrieveMissingPodPlacement finds the placement of a pod from the annotation of other pods in the same group
// when the pod's placement has been invalid (i.e., not found in the spec).
func retrieveMissingPodPlacement(group *AlgoAffinityGroup, gpuNum int32, podIndex int32) (api.PodPlacementInfo, string) {
	for _, pods := range group.allocatedPods {
		for _, p := range pods {
			if p != nil {
				info := internal.ExtractPodBindInfo(p)
				for _, mbi := range info.AffinityGroupBindInfo {
					if gpuNum == int32(len(mbi.PodPlacements[0].PhysicalGpuIndices)) {
						return mbi.PodPlacements[podIndex], info.CellChain
					}
				}
			}
		}
	}
	panic(fmt.Sprintf(
		"No allocated pod found in an allocated group %v when retrieving placement for pod %v with GPU number %v", group.name, podIndex, gpuNum))
}

// clearPreBindings clears the temporary bindings created during scheduling.
func clearPreBindings(virtualPlacement groupVirtualPlacement) {
	for _, podPlacements := range virtualPlacement {
		for _, podGpus := range podPlacements {
			for _, gpu := range podGpus {
				for gpu != nil {
					vGpu := gpu.(*VirtualCell)
					if pGpu := vGpu.GetPreBoundPhysicalCell(); pGpu != nil {
						pGpu.SetPreBoundVirtualCell(nil)
						vGpu.SetPreBoundPhysicalCell(nil)
						gpu = gpu.GetParent()
					} else {
						break
					}
				}
			}
		}
	}
}

// getPodIndex finds the index of a pod in its group according to its placement.
func getPodIndex(podPlacements []api.PodPlacementInfo, node string, gpuIndex int32) int32 {
	for podIndex, placement := range podPlacements {
		if placement.PhysicalNode == node && common.Int32SliceContains(placement.PhysicalGpuIndices, gpuIndex) {
			return int32(podIndex)
		}
	}
	return -1
}

// allPodsReleased checks if all the pods of an affinity group were released.
func allPodsReleased(allocatedPods map[int32][]*core.Pod) bool {
	for _, pods := range allocatedPods {
		for _, p := range pods {
			if p != nil {
				return false
			}
		}
	}
	return true
}

// inFreeCellList checks if a physical cell (or its ancestor) is in the global free cell list.
func inFreeCellList(c *PhysicalCell) bool {
	for {
		if c.GetVirtualCell() != nil || c.IsSplit() {
			return false
		}
		if c.GetParent() == nil || c.GetParent().(*PhysicalCell).IsSplit() {
			return true
		}
		c = c.GetParent().(*PhysicalCell)
	}
}

// generateOpporVirtualCell generates a fake virtual cell in a VC's API status
// for an opportunistic cell used by the VC.
func generateOpporVirtualCell(pc *api.PhysicalCellStatus) *api.VirtualCellStatus {
	vc := &api.VirtualCellStatus{
		CellStatus: api.CellStatus{
			GpuType:         pc.GpuType,
			CellType:        pc.CellType,
			CellAddress:     pc.CellAddress + "-opp",
			CellState:       api.CellUsed,
			CellHealthiness: pc.CellHealthiness,
			CellPriority:    api.OpportunisticPriority,
		},
		PhysicalCell: pc,
	}
	return vc
}
