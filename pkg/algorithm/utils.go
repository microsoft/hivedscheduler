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
	"github.com/microsoft/hivedscheduler/pkg/common"
	"github.com/microsoft/hivedscheduler/pkg/internal"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog"
)

// generatePodScheduleResult writes the scheduling result into a PodScheduleResult.
func generatePodScheduleResult(
	groupPhysicalPlacement groupPhysicalPlacement,
	groupVirtualPlacement groupVirtualPlacement,
	preemptionVictims map[string]common.Set,
	waitReason string,
	cellLevelToType map[CellChain]map[CellLevel]api.CellType,
	currentSkuNum int32,
	currentPodIndex int32,
	group *AlgoAffinityGroup,
	groupName string,
	suggestedNodes common.Set,
	pod *core.Pod) internal.PodScheduleResult {

	klog.V(4).Infof("[%v]: Got K8s suggested nodes: %v", internal.Key(pod), suggestedNodes)
	if groupPhysicalPlacement == nil {
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
	affinityGroupBindInfo, selectedNode, selectedDeviceIndices, cellChain := generateAffinityGroupBindInfo(
		groupPhysicalPlacement, groupVirtualPlacement, cellLevelToType, currentSkuNum, currentPodIndex, group, groupName)
	klog.Infof("[%v]: pod is decided to be scheduled to node %v, devices %v",
		internal.Key(pod), selectedNode, common.ToJson(selectedDeviceIndices))
	return internal.PodScheduleResult{
		PodBindInfo: &api.PodBindInfo{
			Node:                  selectedNode,
			DeviceIsolation:       selectedDeviceIndices,
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
// into a a series of AffinityGroupMemberBindInfos, and also returns the allocated node and device addresses
// of the current pod.
func generateAffinityGroupBindInfo(
	groupPhysicalPlacement groupPhysicalPlacement,
	groupVirtualPlacement groupVirtualPlacement,
	cellLevelToType map[CellChain]map[CellLevel]api.CellType,
	currentSkuNum int32,
	currentPodIndex int32,
	group *AlgoAffinityGroup,
	groupName string) (
	affinityGroupBindInfo []api.AffinityGroupMemberBindInfo,
	selectedNode string,
	selectedDeviceIndices []int32,
	chain string) {

	affinityGroupBindInfo = make([]api.AffinityGroupMemberBindInfo, len(groupPhysicalPlacement))
	groupMemberIndex := 0
	for podSkuNum, podPhysicalPlacements := range groupPhysicalPlacement {
		mbi := api.AffinityGroupMemberBindInfo{
			PodPlacements: make([]api.PodPlacementInfo, len(podPhysicalPlacements)),
		}
		for podIndex := int32(0); podIndex < int32(len(podPhysicalPlacements)); podIndex++ {
			mbi.PodPlacements[podIndex].PhysicalDeviceIndices = make([]int32, podSkuNum)
			mbi.PodPlacements[podIndex].PreassignedCellTypes = make([]api.CellType, podSkuNum)
			for deviceIndex := int32(0); deviceIndex < podSkuNum; deviceIndex++ {
				pDevice := podPhysicalPlacements[podIndex][deviceIndex]
				if pDevice == nil {
					if group == nil || group.state == groupPreempting {
						panic(fmt.Sprintf("The first pod in group %v was allocated invalid resource", groupName))
					}
					// if the physical placement of this pod is not found (e.g., removed due to reconfiguration),
					// we will insist the decision by retrieving it from other pods
					mbi.PodPlacements[podIndex], chain = retrieveMissingPodPlacement(group, podSkuNum, podIndex)
					klog.Warningf(
						"pod placement has been invalid and is retrieved from annotation of other pods: node %v, device %v",
						mbi.PodPlacements[podIndex].PhysicalNode, mbi.PodPlacements[podIndex].PhysicalDeviceIndices[deviceIndex])
				} else {
					nodes, deviceIndices := pDevice.(*PhysicalCell).GetPhysicalPlacement()
					// here each cell (i.e., pDevice) is only one device, hence we takes the first element
					// in its "nodes" and "deviceIndices" as the node and device address
					if mbi.PodPlacements[podIndex].PhysicalNode == "" {
						mbi.PodPlacements[podIndex].PhysicalNode = nodes[0]
					}
					mbi.PodPlacements[podIndex].PhysicalDeviceIndices[deviceIndex] = deviceIndices[0]
					if groupVirtualPlacement != nil {
						vDevice := groupVirtualPlacement[podSkuNum][podIndex][deviceIndex].(*VirtualCell)
						mbi.PodPlacements[podIndex].PreassignedCellTypes[deviceIndex] =
							cellLevelToType[vDevice.GetChain()][vDevice.GetPreassignedCell().GetLevel()]
					} else {
						mbi.PodPlacements[podIndex].PreassignedCellTypes[deviceIndex] = ""
					}
				}
			}
		}
		if podSkuNum == currentSkuNum {
			selectedNode = mbi.PodPlacements[currentPodIndex].PhysicalNode
			selectedDeviceIndices = mbi.PodPlacements[currentPodIndex].PhysicalDeviceIndices
			if pDevice := groupPhysicalPlacement[currentSkuNum][currentPodIndex][0]; pDevice != nil {
				chain = string(pDevice.GetChain())
			}
		}
		affinityGroupBindInfo[groupMemberIndex] = mbi
		groupMemberIndex++
	}
	return affinityGroupBindInfo, selectedNode, selectedDeviceIndices, chain
}

// collectBadOrNonSuggestedNodes collects all the nodes that are not within the suggested nodes
// in the physical placement of an affinity group.
func collectBadOrNonSuggestedNodes(
	placement groupPhysicalPlacement,
	suggestedNodes common.Set,
	ignoreSuggestedNodes bool) (
	badOrNonSuggestedNodes common.Set) {

	badOrNonSuggestedNodes = common.NewSet()
	for skuNum := range placement {
		for podIndex := range placement[skuNum] {
			for _, device := range placement[skuNum][podIndex] {
				if device == nil {
					continue
				}
				nodes, _ := device.(*PhysicalCell).GetPhysicalPlacement()
				if !device.(*PhysicalCell).IsHealthy() ||
					(!ignoreSuggestedNodes && !suggestedNodes.Contains(nodes[0])) {
					badOrNonSuggestedNodes.Add(nodes[0])
				}
			}
		}
	}
	return badOrNonSuggestedNodes
}

// collectPreemptionVictims collects preemption victims of an affinity group.
// If any of the devices allocated for the whole group is still used by a pod,
// we will wait for the preemption, as a group is gang-scheduled.
func collectPreemptionVictims(placement groupPhysicalPlacement) (
	victimPods map[string]common.Set, overlappingPreemptorGroups common.Set) {

	victimPods = map[string]common.Set{} // node -> pods
	overlappingPreemptorGroups = common.NewSet()
	for skuNum := range placement {
		for podIndex := range placement[skuNum] {
			for _, device := range placement[skuNum][podIndex] {
				if device == nil {
					continue
				}
				pDevice := device.(*PhysicalCell)
				state := pDevice.GetState()
				if state == cellUsed || state == cellReserving {
					// for any victim pod, gang-preempt all the other pods from the same affinity group
					for _, pods := range pDevice.GetUsingGroup().allocatedPods {
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
					overlappingPreemptorGroups.Add(pDevice.GetReservingOrReservedGroup())
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
func retrieveMissingPodPlacement(g *AlgoAffinityGroup, skuNum int32, podIndex int32) (api.PodPlacementInfo, string) {
	for _, pods := range g.allocatedPods {
		for _, p := range pods {
			if p != nil {
				info := internal.ExtractPodBindInfo(p)
				for _, mbi := range info.AffinityGroupBindInfo {
					if skuNum == int32(len(mbi.PodPlacements[0].PhysicalDeviceIndices)) {
						return mbi.PodPlacements[podIndex], info.CellChain
					}
				}
			}
		}
	}
	panic(fmt.Sprintf(
		"No allocated pod found in an allocated group %v when retrieving placement for pod %v with SKU number %v", g.name, podIndex, skuNum))
}

// retrieveVirtualCell finds the corresponding virtual cell for a physical cell in the placements of an affinity group.
func retrieveVirtualCell(
	physicalPlacement groupPhysicalPlacement,
	virtualPlacement groupVirtualPlacement,
	pDevice *PhysicalCell) (vDevice *VirtualCell) {

	for skuNum := range physicalPlacement {
		for podIndex := range physicalPlacement[skuNum] {
			for deviceIndex, device := range physicalPlacement[skuNum][podIndex] {
				if device != nil && CellEqual(device, pDevice) {
					return virtualPlacement[skuNum][podIndex][deviceIndex].(*VirtualCell)
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
func getAllocatedPodIndex(info *api.PodBindInfo, skuNum int32) int32 {
	for _, gms := range info.AffinityGroupBindInfo {
		if skuNumber := int32(len(gms.PodPlacements[0].PhysicalDeviceIndices)); skuNumber == skuNum {
			for podIndex, placement := range gms.PodPlacements {
				if placement.PhysicalNode == info.Node && common.Int32SliceContains(
					placement.PhysicalDeviceIndices, info.DeviceIsolation[0]) {
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

// findPhysicalDevice finds a physical device cell in the full list. If the device is not found in the chain specified
// in the PodBindInfo (due to reconfiguration), we will try to search in the other chains.
func findPhysicalDevice(
	fullCellList map[CellChain]ChainCellList,
	chain CellChain,
	node string,
	deviceIndex int32) *PhysicalCell {

	if g := findPhysicalDeviceInChain(fullCellList, chain, node, deviceIndex); g == nil {
		for c := range fullCellList {
			if c != chain {
				if g = findPhysicalDeviceInChain(fullCellList, c, node, deviceIndex); g != nil {
					klog.Warningf("Device %v on node %v has been moved to chain %v", deviceIndex, node, c)
					return g
				}
			}
		}
		return nil
	} else {
		return g
	}
}

// findPhysicalDeviceInChain finds a physical device cell in the full list of a given chain. This search is based on
// *one* node and *one* device index, assuming there is no resource overlapping among cells at the same level.
func findPhysicalDeviceInChain(
	fullCellList map[CellChain]ChainCellList,
	chain CellChain,
	node string,
	deviceIndex int32) *PhysicalCell {

	for _, c := range fullCellList[chain][1] {
		success := false
		cc := c.(*PhysicalCell)
		nodes, deviceIndices := cc.GetPhysicalPlacement()
		for _, n := range nodes {
			if n == node {
				success = true
				break
			}
		}
		if success {
			if deviceIndex < 0 {
				return cc
			} else {
				for _, g := range deviceIndices {
					if g == deviceIndex {
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
// setCellState always starts from the lowest level, i.e., device-level cells.
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
			SkuType:         pc.SkuType,
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
