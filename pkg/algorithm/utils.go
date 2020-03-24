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
	"math/rand"
)

// generatePodScheduleResult writes the scheduling result into a PodScheduleResult.
func generatePodScheduleResult(
	groupPhysicalPlacement groupPhysicalPlacement,
	groupVirtualPlacement groupVirtualPlacement,
	priority CellPriority,
	preemptionVictims map[string]common.Set,
	cellLevelToType map[CellChain]map[CellLevel]api.CellType,
	currentGpuNum int32,
	currentPodIndex int32,
	group *AlgoAffinityGroup,
	groupName string,
	suggestedNodes common.Set,
	vc api.VirtualClusterName,
	pod *core.Pod) internal.PodScheduleResult {

	klog.V(4).Infof("[%v]: Got K8s suggested nodes: %v", internal.Key(pod), suggestedNodes.ToString())
	klog.Infof("[%v]: Physical placement: %v", internal.Key(pod), groupPhysicalPlacement.toString())
	if groupVirtualPlacement != nil {
		klog.Infof("[%v]: Virtual placement: %v", internal.Key(pod), groupVirtualPlacement.toString())
	}
	if len(preemptionVictims) > 0 {
		var (
			nodesHavingVictims []string
			victimPods         []*core.Pod
			victimKeys         []string
		)
		for node := range preemptionVictims {
			nodesHavingVictims = append(nodesHavingVictims, node)
		}
		// We collect victims on a random node, as K8s preempts victims from only one node once.
		// Random is to let different pods preempt victims on different nodes
		// (note that this randomness is not necessary for the eventual completeness of preemption).
		nodeToPreempt := nodesHavingVictims[rand.Int31n(int32(len(nodesHavingVictims)))]
		for v := range preemptionVictims[nodeToPreempt].Items() {
			victimPods = append(victimPods, v.(*core.Pod))
			victimKeys = append(victimKeys, internal.Key(v.(*core.Pod)))
		}
		klog.Infof("[%v]: need to preempt pods %v", internal.Key(pod), common.ToJson(victimKeys))
		return internal.PodScheduleResult{
			PodPreemptInfo: &internal.PodPreemptInfo{VictimPods: victimPods},
		}
	} else {
		// we find the selected node after the preemption is done, otherwise the preemption victims
		// may cause the selected node to be excluded from the suggested nodes
		affinityGroupBindInfo, nodesNotInSuggested, selectedNode, selectedGpuIndices, cellChain := generateAffinityGroupBindInfo(
			groupPhysicalPlacement, groupVirtualPlacement, cellLevelToType, currentGpuNum, currentPodIndex, group, groupName, suggestedNodes)
		var waitReason string
		if affinityGroupBindInfo == nil {
			waitReason = "insufficient capacity in physical cluster"
			if priority >= minGuaranteedPriority {
				waitReason = fmt.Sprintf("insufficient capacity in VC %v", vc)
			}
		} else if len(nodesNotInSuggested) > 0 {
			if group == nil {
				// for a new group, we will keep it waiting if not all of its pods are scheduled to suggested nodes
				waitReason = fmt.Sprintf(
					"affinity group is scheduled to some nodes not within K8s suggested nodes: %v",
					common.ToJson(nodesNotInSuggested))
			} else {
				// for an existing group, we always insist the previous scheduling decision
				// even if some pods are now not within suggested nodes
				klog.Warningf("Some nodes used by affinity group %v are no longer within K8s suggested nodes: %v",
					group.name, common.ToJson(nodesNotInSuggested))
			}
		}
		if waitReason != "" {
			klog.Infof("[%v]: need to wait because %v", internal.Key(pod), waitReason)
			return internal.PodScheduleResult{PodWaitInfo: &internal.PodWaitInfo{Reason: waitReason}}
		}
		klog.Infof("[%v]: scheduled to node %v, GPUs %v",
			internal.Key(pod), selectedNode, common.ToJson(selectedGpuIndices))
		return internal.PodScheduleResult{
			PodBindInfo: &api.PodBindInfo{
				Node:                  selectedNode,
				GpuIsolation:          selectedGpuIndices,
				CellChain:             cellChain,
				AffinityGroupBindInfo: affinityGroupBindInfo,
			},
		}
	}
}

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
func collectPreemptionVictims(physicalPlacement groupPhysicalPlacement) (victims map[string]common.Set) {
	victims = map[string]common.Set{}
	for gpuNum := range physicalPlacement {
		for podIndex := range physicalPlacement[gpuNum] {
			for _, gpu := range physicalPlacement[gpuNum][podIndex] {
				if gpu == nil {
					continue
				}
				pGpu := gpu.(*PhysicalCell)
				if pGpu.GetState() == cellUsed || pGpu.GetState() == cellAcquiring {
					// for any victim pod, gang-preempt all the other pods from the same affinity group
					for _, victimPods := range pGpu.GetUsingGroup().allocatedPods {
						for _, v := range victimPods {
							if v != nil {
								if _, ok := victims[v.Spec.NodeName]; !ok {
									victims[v.Spec.NodeName] = common.NewSet()
								}
								victims[v.Spec.NodeName].Add(v)
							}
						}
					}
				}
			}
		}
	}
	return victims
}

// retrieveMissingPodPlacement finds the placement of a pod from the annotation of other pods in the same group
// when the pod's placement has been invalid (i.e., not found in the spec).
func retrieveMissingPodPlacement(g *AlgoAffinityGroup, gpuNum int32, podIndex int32) (api.PodPlacementInfo, string) {
	for _, pods := range g.allocatedPods {
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
		"No allocated pod found in an allocated group %v when retrieving placement for pod %v with GPU number %v", g.name, podIndex, gpuNum))
}

func retrieveVirtualCell(
	physicalPlacement groupPhysicalPlacement,
	virtualPlacement groupVirtualPlacement,
	pGpu *PhysicalCell) (vGpu *VirtualCell) {

	for gpuNum := range physicalPlacement {
		for podIndex := range physicalPlacement[gpuNum] {
			for gpuIndex, gpu := range physicalPlacement[gpuNum][podIndex] {
				if gpu != nil && CellEqual(gpu, pGpu) {
					return virtualPlacement[gpuNum][podIndex][gpuIndex].(*VirtualCell)
				}
			}
		}
	}
	return nil
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

// getAllocatedPodIndex assigns a new index for a new pod in an affinity group.
func getNewPodIndex(pods []*core.Pod) int32 {
	podIndex := int32(-1)
	for i, p := range pods {
		if p == nil {
			podIndex = int32(i)
			break
		}
	}
	return podIndex
}

// getAllocatedPodIndex finds the index of an allocated pod in its group according to its placement.
func getAllocatedPodIndex(info *api.PodBindInfo, gpuNum int32) int32 {
	for _, gms := range info.AffinityGroupBindInfo {
		if gpuNumber := int32(len(gms.PodPlacements[0].PhysicalGpuIndices)); gpuNumber == gpuNum {
			for podIndex, placement := range gms.PodPlacements {
				if placement.PhysicalNode == info.Node && common.Int32SliceContains(
					placement.PhysicalGpuIndices, info.GpuIsolation[0]) {
					return int32(podIndex)
				}
			}
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
