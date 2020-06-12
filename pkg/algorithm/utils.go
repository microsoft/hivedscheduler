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
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog"
	"math/rand"
)

// generatePodScheduleResult writes the scheduling result into a PodScheduleResult.
func generatePodScheduleResult(
	groupPhysicalPlacement groupPhysicalPlacement,
	groupVirtualPlacement groupVirtualPlacement,
	preemptionVictims map[string]common.Set,
	waitReason string,
	cellLevelToType map[CellChain]map[CellLevel]api.CellType,
	currentGpuNum int32,
	currentPodIndex int32,
	group *AlgoAffinityGroup,
	groupName string,
	suggestedNodes common.Set,
	pod *core.Pod) internal.PodScheduleResult {

	klog.V(4).Infof("[%v]: Got K8s suggested nodes: %v", internal.Key(pod), suggestedNodes)
	if waitReason != "" {
		klog.Infof("[%v]: Pod needs to wait, reason: %v", internal.Key(pod), waitReason)
		return internal.PodScheduleResult{PodWaitInfo: &internal.PodWaitInfo{Reason: waitReason}}
	}
	klog.Infof("[%v]: Physical placement: %v", internal.Key(pod), groupPhysicalPlacement)
	if groupVirtualPlacement != nil {
		klog.Infof("[%v]: Virtual placement: %v", internal.Key(pod), groupVirtualPlacement)
	}
	if len(preemptionVictims) > 0 {
		return internal.PodScheduleResult{
			PodPreemptInfo: generatePodPreemptInfo(preemptionVictims, pod),
		}
	}
	// we find the selected node after the preemption is done, otherwise the preemption victims
	// may cause the selected node to be excluded from the suggested nodes
	affinityGroupBindInfo, selectedNode, selectedGpuIndices, cellChain := generateAffinityGroupBindInfo(
		groupPhysicalPlacement, groupVirtualPlacement, cellLevelToType, currentGpuNum, currentPodIndex, group, groupName)
	klog.Infof("[%v]: pod is decided to be scheduled to node %v, GPUs %v",
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

// generatePodPreemptInfo writes the preemption victims into a PodPreemptInfo.
func generatePodPreemptInfo(preemptionVictims map[string]common.Set, pod *core.Pod) *internal.PodPreemptInfo {
	klog.Infof("[%v]: Preemption victim candidates: %v",
		internal.Key(pod), victimsToString(preemptionVictims))
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
	return &internal.PodPreemptInfo{VictimPods: victimPods}
}

// generateAffinityGroupBindInfo translates the physical and virtual placements of an affinity group
// into a a series of AffinityGroupMemberBindInfos, and also returns the allocated node and GPU addresses
// of the current pod.
func generateAffinityGroupBindInfo(
	groupPhysicalPlacement groupPhysicalPlacement,
	groupVirtualPlacement groupVirtualPlacement,
	cellLevelToType map[CellChain]map[CellLevel]api.CellType,
	currentGpuNum int32,
	currentPodIndex int32,
	group *AlgoAffinityGroup,
	groupName string) (
	affinityGroupBindInfo []api.AffinityGroupMemberBindInfo,
	selectedNode string,
	selectedGpuIndices []int32,
	chain string) {

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
					if group == nil || group.state == groupPreempting {
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
	return affinityGroupBindInfo, selectedNode, selectedGpuIndices, chain
}

// collectBadOrNonSuggestedNodes collects all the nodes that are not within the suggested nodes
// in the physical placement of an affinity group.
func collectBadOrNonSuggestedNodes(
	placement groupPhysicalPlacement,
	suggestedNodes common.Set,
	avoidSuggestedNodes bool) (
	badOrNonSuggestedNodes common.Set) {

	badOrNonSuggestedNodes = common.NewSet()
	for gpuNum := range placement {
		for podIndex := range placement[gpuNum] {
			for _, gpu := range placement[gpuNum][podIndex] {
				if gpu == nil {
					continue
				}
				nodes, _ := gpu.(*PhysicalCell).GetPhysicalPlacement()
				if !gpu.(*PhysicalCell).IsHealthy() ||
					(avoidSuggestedNodes && !suggestedNodes.Contains(nodes[0])) {
					badOrNonSuggestedNodes.Add(nodes[0])
				}
			}
		}
	}
	return badOrNonSuggestedNodes
}

// collectPreemptionVictims collects preemption victims of an affinity group.
// If any of the GPUs allocated for the whole group is still used by a pod,
// we will wait for the preemption, as a group is gang-scheduled.
func collectPreemptionVictims(placement groupPhysicalPlacement) (
	victimPods map[string]common.Set, overlappingPreemptorGroups common.Set) {

	victimPods = map[string]common.Set{} // node -> pods
	overlappingPreemptorGroups = common.NewSet()
	for gpuNum := range placement {
		for podIndex := range placement[gpuNum] {
			for _, gpu := range placement[gpuNum][podIndex] {
				if gpu == nil {
					continue
				}
				pGpu := gpu.(*PhysicalCell)
				state := pGpu.GetState()
				if state == cellUsed || state == cellReserving {
					// for any victim pod, gang-preempt all the other pods from the same affinity group
					for _, pods := range pGpu.GetUsingGroup().allocatedPods {
						for _, v := range pods {
							if v != nil {
								if _, ok := victimPods[v.Spec.NodeName]; !ok {
									victimPods[v.Spec.NodeName] = common.NewSet()
								}
								victimPods[v.Spec.NodeName].Add(v)
							}
						}
					}
				}
				if state == cellReserving || state == cellReserved {
					overlappingPreemptorGroups.Add(pGpu.GetReservingOrReservedGroup())
				}
			}
		}
	}
	return victimPods, overlappingPreemptorGroups
}

func victimsToString(victimPods map[string]common.Set) string {
	s := map[string][]types.UID{}
	for node, victims := range victimPods {
		s[node] = []types.UID{}
		for v := range victims.Items() {
			s[node] = append(s[node], v.(*core.Pod).UID)
		}
	}
	return common.ToJson(s)
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

// retrieveVirtualCell finds the corresponding virtual cell for a physical cell in the placements of an affinity group.
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

// findPhysicalGpu finds a physical GPU cell in the full list. If the GPU is not found in the chain specified
// in the PodBindInfo (due to reconfiguration), we will try to search in the other chains.
func findPhysicalGpu(
	fullCellList map[CellChain]ChainCellList,
	chain CellChain,
	node string,
	gpuIndex int32) *PhysicalCell {

	if g := findPhysicalGpuInChain(fullCellList, chain, node, gpuIndex); g == nil {
		for c := range fullCellList {
			if c != chain {
				if g = findPhysicalGpuInChain(fullCellList, c, node, gpuIndex); g != nil {
					klog.Warningf("GPU %v on node %v has been moved to chain %v", gpuIndex, node, c)
					return g
				}
			}
		}
		return nil
	} else {
		return g
	}
}

// findPhysicalGpuInChain finds a physical GPU cell in the full list of a given chain. This search is based on
// *one* node and *one* GPU index, assuming there is no resource overlapping among cells at the same level.
func findPhysicalGpuInChain(
	fullCellList map[CellChain]ChainCellList,
	chain CellChain,
	node string,
	gpuIndex int32) *PhysicalCell {

	for _, c := range fullCellList[chain][1] {
		success := false
		cc := c.(*PhysicalCell)
		nodes, gpuIndices := cc.GetPhysicalPlacement()
		for _, n := range nodes {
			if n == node {
				success = true
				break
			}
		}
		if success {
			if gpuIndex < 0 {
				return cc
			} else {
				for _, g := range gpuIndices {
					if g == gpuIndex {
						return cc
					}
				}
			}
		}
	}
	return nil
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

// setCellState sets state for a cell and its parent recursively. A parent cell will be in Used state
// if any of its children is in Used state. For the other states (Free, Reserving, Reserved),
// a parent will be in the state if all of this children are in the state.
// setCellState always starts from the lowest level, i.e., GPU-level cells.
func setCellState(c *PhysicalCell, s CellState) {
	c.SetState(s)
	if c.GetParent() != nil {
		parent := c.GetParent().(*PhysicalCell)
		if s == cellUsed || allChildrenSameState(parent, s) {
			setCellState(parent, s)
		}
	}
}

// allChildrenSameState checks if all of a cell's children are in the same state.
func allChildrenSameState(c *PhysicalCell, s CellState) bool {
	for _, child := range c.GetChildren() {
		if child.(*PhysicalCell).GetState() != s {
			return false
		}
	}
	return true
}

// generateOpporVirtualCell generates a fake virtual cell in a VC's API status
// for an opportunistic cell used by the VC.
func generateOpporVirtualCell(pc *api.PhysicalCellStatus) *api.VirtualCellStatus {
	vc := &api.VirtualCellStatus{
		CellStatus: api.CellStatus{
			GpuType:         pc.GpuType,
			CellType:        pc.CellType,
			CellAddress:     pc.CellAddress + "-opp",
			CellState:       api.CellState(cellUsed),
			CellHealthiness: pc.CellHealthiness,
			CellPriority:    api.OpportunisticPriority,
		},
		PhysicalCell: pc,
	}
	return vc
}

// deleteOpporVirtualCell deletes the fake virtual cell of an opportunistic cell from the VC's API status.
func deleteOpporVirtualCell(s api.VirtualClusterStatus, addr api.CellAddress) api.VirtualClusterStatus {
	var opporVirtualCellIdx int32
	for i, ovc := range s {
		if ovc.PhysicalCell != nil && ovc.PhysicalCell.CellAddress == addr {
			opporVirtualCellIdx = int32(i)
			break
		}
	}
	novc := len(s)
	s[opporVirtualCellIdx] = s[novc-1]
	s[novc-1] = nil
	return s[:novc-1]
}
