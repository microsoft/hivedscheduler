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
	"math/rand"

	"github.com/microsoft/hivedscheduler/pkg/api"
	apiv2 "github.com/microsoft/hivedscheduler/pkg/api/v2"
	"github.com/microsoft/hivedscheduler/pkg/common"
	"github.com/microsoft/hivedscheduler/pkg/internal"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog"
)

// generatePodScheduleResult writes the scheduling result into a PodScheduleResult.
func generatePodScheduleResult(
	physicalPlacement PodGroupPhysicalPlacement,
	virtualPlacement PodGroupVirtualPlacement,
	preemptionVictims map[string]common.Set,
	waitReason string,
	cellLevelToType map[CellChain]map[CellLevel]api.CellType,
	currentCellNum int32,
	currentPodGroupIndex int32,
	currentPodIndex int32,
	podGroupSchedStatus *PodGroupSchedulingStatus,
	suggestedNodes common.Set,
	pod *core.Pod) internal.PodScheduleResult {

	klog.V(4).Infof("[%v]: Got K8s suggested nodes: %v", internal.Key(pod), suggestedNodes)
	if PodGroupPlacement(physicalPlacement).IsEmpty() {
		klog.Infof("[%v]: Pod needs to wait, reason: %v", internal.Key(pod), waitReason)
		return internal.PodScheduleResult{PodWaitInfo: &internal.PodWaitInfo{Reason: waitReason}}
	}
	klog.Infof("[%v]: Physical placement: %v", internal.Key(pod), physicalPlacement)
	if !PodGroupPlacement(virtualPlacement).IsEmpty() {
		klog.Infof("[%v]: Virtual placement: %v", internal.Key(pod), virtualPlacement)
	}
	if len(preemptionVictims) > 0 {
		return internal.PodScheduleResult{
			PodPreemptInfo: generatePodPreemptInfo(preemptionVictims, pod),
		}
	}
	// we find the selected node after the preemption is done, otherwise the preemption victims
	// may cause the selected node to be excluded from the suggested nodes
	podRootGroupBindInfo, selectedNode, selectedLeafCellIndices, cellChain := generatePodGroupBindInfo(
		physicalPlacement, virtualPlacement, cellLevelToType, currentCellNum, currentPodGroupIndex, currentPodIndex, podGroupSchedStatus)
	klog.Infof("[%v]: pod is decided to be scheduled to node %v, leaf cells %v",
		internal.Key(pod), selectedNode, common.ToJson(selectedLeafCellIndices))
	return internal.PodScheduleResult{
		PodBindInfo: &apiv2.PodBindInfo{
			Version:              "v2",
			Node:                 selectedNode,
			LeafCellIsolation:    selectedLeafCellIndices,
			CellChain:            cellChain,
			PodRootGroupBindInfo: podRootGroupBindInfo,
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

// generatePodGroupBindInfo translates the physical and virtual placements of a pod group
// into PodGroupBindInfo, and also returns the allocated node and leaf cell addresses
// of the current pod.
func generatePodGroupBindInfo(
	physicalPlacement PodGroupPhysicalPlacement,
	virtualPlacement PodGroupVirtualPlacement,
	cellLevelToType map[CellChain]map[CellLevel]api.CellType,
	currentLeafCellNum int32,
	currentPodGroupIndex int32,
	currentPodIndex int32,
	podGroupSchedStatus *PodGroupSchedulingStatus) (
	podRootGroupBindInfo *apiv2.PodGroupBindInfo,
	selectedNode string,
	selectedLeafCellIndices []int32,
	chain string) {

	podGroupIndex := int32(0)
	podRootGroupBindInfo = &apiv2.PodGroupBindInfo{}

	physicalPlacementQueue := []*PodGroupPlacement{(*PodGroupPlacement)(&physicalPlacement)}
	virtualPlacementQueue := []*PodGroupPlacement{(*PodGroupPlacement)(&virtualPlacement)}
	podGroupBindInfoQueue := []*apiv2.PodGroupBindInfo{podRootGroupBindInfo}
	for len(physicalPlacementQueue) > 0 {
		newPhysicalPlacementQueue := []*PodGroupPlacement{}
		newVirtualPlacementQueue := []*PodGroupPlacement{}
		newPodGroupBindInfoQueue := []*apiv2.PodGroupBindInfo{}
		for index, placement := range physicalPlacementQueue {
			podGroupBindInfoQueue[index].PodPlacements = make([]apiv2.PodPlacementInfo, len(placement.podsPlacement))
			for podIndex, podPlacement := range placement.podsPlacement {
				podLeafCellNum := len(podPlacement)
				podGroupBindInfoQueue[index].PodPlacements[podIndex].PhysicalLeafCellIndices = make([]int32, podLeafCellNum)
				podGroupBindInfoQueue[index].PodPlacements[podIndex].PreassignedCellTypes = make([]api.CellType, podLeafCellNum)
				for leafCellIndex, pLeafCell := range podPlacement {
					if pLeafCell == nil {
						if podGroupSchedStatus == nil || podGroupSchedStatus.state == podGroupPreempting {
							panic(fmt.Sprintf("The first pod in group %v was allocated invalid resource", podGroupSchedStatus.name))
						}
						// if the physical placement of this pod is not found (e.g., removed due to reconfiguration),
						// we will insist the decision by retrieving it from other pods
						podGroupBindInfoQueue[index].PodPlacements[podIndex], chain =
							retrieveMissingPodPlacement(podGroupSchedStatus, podGroupIndex, int32(podIndex))
						klog.Warningf(
							"pod placement has been invalid and is retrieved from annotation of other pods: node %v, leaf cell %v",
							podGroupBindInfoQueue[index].PodPlacements[podIndex].PhysicalNode,
							podGroupBindInfoQueue[index].PodPlacements[podIndex].PhysicalLeafCellIndices[leafCellIndex])
					} else {
						nodes, leafCellIndices := pLeafCell.(*PhysicalCell).GetPhysicalPlacement()
						// here each cell (i.e., pLeafCell) is only one leaf cell, hence we takes the first element
						// in its "nodes" and "leafCellIndices" as the node and leaf cell address
						if podGroupBindInfoQueue[index].PodPlacements[podIndex].PhysicalNode == "" {
							podGroupBindInfoQueue[index].PodPlacements[podIndex].PhysicalNode = nodes[0]
						}
						podGroupBindInfoQueue[index].PodPlacements[podIndex].PhysicalLeafCellIndices[leafCellIndex] = leafCellIndices[0]
						if !PodGroupPlacement(virtualPlacement).IsEmpty() {
							vLeafCell := virtualPlacementQueue[index].podsPlacement[podIndex][leafCellIndex].(*VirtualCell)
							podGroupBindInfoQueue[index].PodPlacements[podIndex].PreassignedCellTypes[leafCellIndex] =
								cellLevelToType[vLeafCell.GetChain()][vLeafCell.GetPreassignedCell().GetLevel()]
						} else {
							podGroupBindInfoQueue[index].PodPlacements[podIndex].PreassignedCellTypes[leafCellIndex] = ""
						}
					}
				}
			}
			if podGroupIndex == currentPodGroupIndex && len(podGroupBindInfoQueue[index].PodPlacements) > 0 {
				selectedNode = podGroupBindInfoQueue[index].PodPlacements[currentPodIndex].PhysicalNode
				selectedLeafCellIndices = podGroupBindInfoQueue[index].PodPlacements[currentPodIndex].PhysicalLeafCellIndices
				if pLeafCell := physicalPlacementQueue[index].podsPlacement[currentPodIndex][0]; pLeafCell != nil {
					chain = string(pLeafCell.GetChain())
				}
			}
			if placement.childGroupsPlacement != nil {
				podGroupBindInfoQueue[index].ChildGroupBindingInfo = make([]*apiv2.PodGroupBindInfo, len(placement.childGroupsPlacement))
				for childIndex := range placement.childGroupsPlacement {
					podGroupBindInfoQueue[index].ChildGroupBindingInfo[childIndex] = &apiv2.PodGroupBindInfo{}
				}
			}
			podGroupIndex++
			newPhysicalPlacementQueue = append(newPhysicalPlacementQueue, physicalPlacementQueue[index].childGroupsPlacement...)
			newVirtualPlacementQueue = append(newVirtualPlacementQueue, virtualPlacementQueue[index].childGroupsPlacement...)
			newPodGroupBindInfoQueue = append(newPodGroupBindInfoQueue, podGroupBindInfoQueue[index].ChildGroupBindingInfo...)
		}
		physicalPlacementQueue = newPhysicalPlacementQueue
		virtualPlacementQueue = newVirtualPlacementQueue
		podGroupBindInfoQueue = newPodGroupBindInfoQueue
	}

	return podRootGroupBindInfo, selectedNode, selectedLeafCellIndices, chain
}

// collectBadOrNonSuggestedNodes collects all the nodes that are not within the suggested nodes
// in the physical placement of a pod group.
func collectBadOrNonSuggestedNodes(
	placement PodGroupPhysicalPlacement,
	suggestedNodes common.Set,
	ignoreSuggestedNodes bool) (
	badOrNonSuggestedNodes common.Set) {

	badOrNonSuggestedNodes = common.NewSet()
	for iter := PodGroupPlacement(placement).Iterator(); iter.HasNext(); {
		for _, leafCell := range *iter.Next() {
			if leafCell == nil {
				continue
			}
			nodes, _ := leafCell.(*PhysicalCell).GetPhysicalPlacement()
			if !leafCell.(*PhysicalCell).IsHealthy() ||
				(!ignoreSuggestedNodes && !suggestedNodes.Contains(nodes[0])) {
				badOrNonSuggestedNodes.Add(nodes[0])
			}
		}
	}
	return badOrNonSuggestedNodes
}

// collectPreemptionVictims collects preemption victims of a pod group.
// If any of the leaf cells allocated for the whole group is still used by a pod,
// we will wait for the preemption, as a group is gang-scheduled.
func collectPreemptionVictims(placement PodGroupPhysicalPlacement) (
	victimPods map[string]common.Set, overlappingPreemptorGroups common.Set) {

	victimPods = map[string]common.Set{} // node -> pods
	overlappingPreemptorGroups = common.NewSet()
	for iter := PodGroupPlacement(placement).Iterator(); iter.HasNext(); {
		for _, leafCell := range *iter.Next() {
			if leafCell == nil {
				continue
			}
			pLeafCell := leafCell.(*PhysicalCell)
			state := pLeafCell.GetState()
			if state == cellUsed || state == cellReserving {
				// for any victim pod, gang-preempt all the other pods from the same pod group
				for iter := pLeafCell.GetUsingGroup().allocatedPodGroup.Iterator(); iter.HasNext(); {
					pod := iter.Next()
					if pod != nil {
						if _, ok := victimPods[pod.Spec.NodeName]; !ok {
							victimPods[pod.Spec.NodeName] = common.NewSet()
						}
						victimPods[pod.Spec.NodeName].Add(pod)
					}
				}
			}
			if state == cellReserving || state == cellReserved {
				overlappingPreemptorGroups.Add(pLeafCell.GetReservingOrReservedGroup())
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

// retrieveMissingPodPlacement finds the placement of a pod from the annotation of other pods in the same pod group
// when the pod's placement has been invalid (i.e., not found in the spec).
func retrieveMissingPodPlacement(podGroupSchedStatus *PodGroupSchedulingStatus, podGroupIndex int32, podIndex int32) (apiv2.PodPlacementInfo, string) {
	for iter := podGroupSchedStatus.allocatedPodGroup.Iterator(); iter.HasNext(); {
		pod := iter.Next()
		if pod != nil {
			info := internal.ExtractPodBindInfo(pod)
			index := int32(0)
			for infoIter := info.PodRootGroupBindInfo.Iterator(podGroupIndex); infoIter.HasNext(); {
				podPlacementInfo := infoIter.Next()
				if index == podIndex {
					return *podPlacementInfo, info.CellChain
				}
				index++
			}
		}
	}
	panic(fmt.Sprintf(
		"No allocated pod found in an allocated group %v when retrieving placement for pod group %v pod %v", podGroupSchedStatus.name, podGroupIndex, podIndex))
}

// retrieveVirtualCell finds the corresponding virtual cell for a physical cell in the placements of a pod group.
func retrieveVirtualCell(
	physicalPlacement PodGroupPhysicalPlacement,
	virtualPlacement PodGroupVirtualPlacement,
	pLeafCell *PhysicalCell) (vLeafCell *VirtualCell) {

	pIter := PodGroupPlacement(physicalPlacement).Iterator()
	vIter := PodGroupPlacement(virtualPlacement).Iterator()
	for pIter.HasNext() {
		pLeafCells := *pIter.Next()
		vLeafCells := *vIter.Next()
		for leafCellIndex, leafCell := range pLeafCells {
			if leafCell != nil && CellEqual(leafCell, pLeafCell) {
				return vLeafCells[leafCellIndex].(*VirtualCell)
			}
		}
	}
	return nil
}

// getAllocatedPodIndex assigns a new index for a new pod in a pod group.
func getNewPodIndex(allocatedPodGroup AllocatedPodGroup, podGroupIndex int32) int32 {
	podIndex := int32(0)
	for iter := allocatedPodGroup.Iterator(podGroupIndex); iter.HasNext(); {
		if iter.Next() == nil {
			return podIndex
		}
		podIndex++
	}
	return -1
}

// getAllocatedPodIndex finds the index of an allocated pod in its group according to its placement.
func getAllocatedPodIndex(info *apiv2.PodBindInfo, podGroupIndex int32) int32 {
	podIndex := int32(0)
	for iter := info.PodRootGroupBindInfo.Iterator(podGroupIndex); iter.HasNext(); {
		podPlacementInfo := iter.Next()
		if podPlacementInfo.PhysicalNode == info.Node && common.Int32SliceContains(
			podPlacementInfo.PhysicalLeafCellIndices, info.LeafCellIsolation[0]) {
			return podIndex
		}
		podIndex++
	}
	return -1
}

// allPodsReleased checks if all the pods of an affinity group were released.
func allPodsReleased(allocatedPodGroup AllocatedPodGroup) bool {
	for iter := allocatedPodGroup.Iterator(); iter.HasNext(); {
		if iter.Next() != nil {
			return false
		}
	}
	return true
}

// findPhysicalLeafCell finds a physical leaf cell in the full list. If the leaf cell is not found in the chain specified
// in the PodBindInfo (due to reconfiguration), we will try to search in the other chains.
func findPhysicalLeafCell(
	fullCellList map[CellChain]ChainCellList,
	chain CellChain,
	node string,
	leafCellIndex int32) *PhysicalCell {

	if g := findPhysicalLeafCellInChain(fullCellList, chain, node, leafCellIndex); g == nil {
		for c := range fullCellList {
			if c != chain {
				if g = findPhysicalLeafCellInChain(fullCellList, c, node, leafCellIndex); g != nil {
					klog.Warningf("Leaf cell %v on node %v has been moved to chain %v", leafCellIndex, node, c)
					return g
				}
			}
		}
		return nil
	} else {
		return g
	}
}

// findPhysicalLeafCellInChain finds a physical leaf cell in the full list of a given chain. This search is based on
// *one* node and *one* leaf cell index, assuming there is no resource overlapping among cells at the same level.
func findPhysicalLeafCellInChain(
	fullCellList map[CellChain]ChainCellList,
	chain CellChain,
	node string,
	leafCellIndex int32) *PhysicalCell {

	for _, c := range fullCellList[chain][1] {
		success := false
		cc := c.(*PhysicalCell)
		nodes, leafCellIndices := cc.GetPhysicalPlacement()
		for _, n := range nodes {
			if n == node {
				success = true
				break
			}
		}
		if success {
			if leafCellIndex < 0 {
				return cc
			} else {
				for _, g := range leafCellIndices {
					if g == leafCellIndex {
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
// setCellState always starts from the lowest level, i.e., leaf-level cells.
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

// generateOTVirtualCell generates a fake virtual cell in a VC's API status
// for an opportunistic cell used by the VC.
func generateOTVirtualCell(pc *api.PhysicalCellStatus) *api.VirtualCellStatus {
	vc := &api.VirtualCellStatus{
		CellStatus: api.CellStatus{
			LeafCellType:    pc.LeafCellType,
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

// deleteOTVirtualCell deletes the fake virtual cell of an opportunistic cell from the VC's API status.
func deleteOTVirtualCell(s api.VirtualClusterStatus, addr api.CellAddress) api.VirtualClusterStatus {
	idx := -1
	for i, ovc := range s {
		if ovc.PhysicalCell != nil && ovc.PhysicalCell.CellAddress == addr {
			idx = i
			break
		}
	}
	if idx < 0 {
		klog.Errorf("trying to delete an opportunistic virtual cell that does not exist, "+
			"physical cell address: %v", addr)
		return s
	}
	n := len(s)
	s[idx] = s[n-1]
	s[n-1] = nil
	return s[:n-1]
}
