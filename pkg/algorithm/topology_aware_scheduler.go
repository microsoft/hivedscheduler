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
	"sort"

	"github.com/microsoft/hivedscheduler/pkg/api"
	"github.com/microsoft/hivedscheduler/pkg/common"
)

// topologyAwareScheduler can schedule a set of pods on a cluster view.
// It first tries to place pods to nodes with fewer free devices (i.e., packing), while trying to avoid preemptions.
// Then inside each node, it tries to allocate devices with better affinity.
type topologyAwareScheduler struct {
	// a list of nodes (node-level cells or top-level cells that are lower than node level)
	cv clusterView
	// SKU number at each level in the cell hierarchy. we use this to
	// calculate the optimal affinity for a given SKU number.
	levelSkuNum map[CellLevel]int32
	// pack pods cross different priorities, or inside each priority. the former is for intra-VC scheduling,
	// because high-priority can avoid preemption in the whole cluster view,
	// and hence we can pack pods with different priorities.
	// the latter is for opportunistic pod scheduling (stay away from guaranteed pods),
	// because guaranteed pods can avoid preempting opportunistic pods only among buddy cells (this is decided
	// by the buddy cell allocation algorithm).
	crossPriorityPack bool
}

// NewTopologyAwareScheduler initializes the scheduler by extracting node-level cells
// (lower-level if no node-level) from a free cell list.
func NewTopologyAwareScheduler(
	ccl ChainCellList,
	levelSkuNum map[CellLevel]int32,
	crossPriorityPack bool) *topologyAwareScheduler {

	return &topologyAwareScheduler{
		cv:                newClusterView(ccl),
		levelSkuNum:       levelSkuNum,
		crossPriorityPack: crossPriorityPack,
	}
}

func (t *topologyAwareScheduler) Schedule(
	podSkuNumbers map[int32]int32,
	p CellPriority,
	suggestedNodes common.Set,
	ignoreSuggestedNodes bool) (
	podPlacements map[int32][]CellList,
	failedReason string) {

	// SKU numbers of the pods to schedule
	var sortedPodSkuNumbers []int32
	for skuNum, podNum := range podSkuNumbers {
		for i := int32(0); i < podNum; i++ {
			sortedPodSkuNumbers = append(sortedPodSkuNumbers, skuNum)
		}
	}
	common.SortInt32(sortedPodSkuNumbers)

	// disable preemption first (reduce preemption)
	priority := opportunisticPriority
	t.updateClusterView(priority, suggestedNodes, ignoreSuggestedNodes)
	// try to fit the pods to a set of nodes
	selectedNodeIndices, failedReason := findNodesForPods(t.cv, sortedPodSkuNumbers)
	// enable preemption if scheduling failed
	if selectedNodeIndices == nil && p > opportunisticPriority {
		priority = p
		t.updateClusterView(priority, suggestedNodes, ignoreSuggestedNodes)
		selectedNodeIndices, failedReason = findNodesForPods(t.cv, sortedPodSkuNumbers)
	}
	if selectedNodeIndices == nil {
		return nil, failedReason
	}
	// find devices inside the selected node for each pod
	selectedNodes := make(CellList, len(sortedPodSkuNumbers))
	for i := 0; i < len(selectedNodeIndices); i++ {
		selectedNodes[i] = t.cv[selectedNodeIndices[i]].c
	}
	selectedDevices := CellList{}
	nodeAvailableDevices := map[Cell]CellList{}
	podPlacements = map[int32][]CellList{}
	for podIndex := 0; podIndex < len(sortedPodSkuNumbers); podIndex++ {
		skuNumber := sortedPodSkuNumbers[podIndex]
		n := selectedNodes[podIndex]
		// TODO: Optimize findNodesForPods and findDevicesInNode together to get a better placement,
		//  such as also aware intra node topology when findNodesForPods.
		selectedDevices, nodeAvailableDevices[n] = findDevicesInNode(n, skuNumber, priority, nodeAvailableDevices[n], t.levelSkuNum)
		if podPlacements[skuNumber] == nil {
			podPlacements[skuNumber] = []CellList{}
		}
		podPlacements[skuNumber] = append(podPlacements[skuNumber], selectedDevices)
	}
	return podPlacements, ""
}

type node struct {
	c                        Cell            // a node-level cell or a top-level cell that is lower than node level
	freeSkuNumAtPriority     int32           // free SKU number at the priority of the pod to be scheduled (lower priority considered as free)
	usedSkuNumSamePriority   int32           // SKU number used by the same priority as that of the pod to be scheduled
	usedSkuNumHigherPriority int32           // SKU number used by higher priorities than that of the pod to be scheduled
	healthy                  bool            // if the node is healthy
	suggested                bool            // if the node is within suggested nodes
	nodeAddress              api.CellAddress // used for logging the node address when bad or not suggested
}

// When cross-priority packing is not enabled, we count the SKU numbers used by the current
// priority (n.usedSkuNumSamePriority), and the higher priorities (n.usedSkuNumHigherPriority), respectively.
// When sorting the nodes, nodes with higher usedSkuNumSamePriority and lower usedSkuNumHigherPriority
// will be preferred (i.e., pack pods inside the same priority, and stay from higher priorities).
// Note that in this case, the nodes may NOT be ordered in term of total used SKU number,
// which may result in feasible pod placements being not found.
//
// Otherwise, n.usedSkuNumSamePriority is set to the total used SKU number,
// so that nodes with more used devices will be preferred (i.e., pack pods globally across priorities).
// In this case a feasible pod placement is guaranteed to be found (as long as all nodes are in suggested nodes).
func (n *node) updateUsedSkuNumForPriority(p CellPriority, crossPriorityPack bool) {
	n.usedSkuNumSamePriority = n.c.GetUsedSkuNumAtPriorities()[p]
	n.usedSkuNumHigherPriority = 0
	n.freeSkuNumAtPriority = n.c.GetTotalSkuNum()
	for priority, num := range n.c.GetUsedSkuNumAtPriorities() {
		if crossPriorityPack {
			if priority != p {
				n.usedSkuNumSamePriority += num
			}
		} else if priority > p {
			n.usedSkuNumHigherPriority += num
		}
		if priority >= p {
			n.freeSkuNumAtPriority -= num
		}
	}
}

type clusterView []*node

func newClusterView(ccl ChainCellList) clusterView {
	var l CellLevel
	// TODO: currently if a top-level cell is lower than node level, it will be considered as a single node.
	// For example, 2 single device-level cells are considered as 2 nodes each with 1 device.
	// We cannot merge them because the 2 cells might be mapped to different physical nodes.
	// We plan to support using multiple cells in a best-effort manner (for example, schedule a 2-device pod
	// on 2 1-device cells, if we can find 2 1-device cells that can be mapped to the same physical node).
	for l = CellLevel(1); l <= CellLevel(len(ccl)); l++ {
		if ccl[l][0].AtOrHigherThanNode() {
			break
		}
	}
	cv := clusterView{}
	for ; l >= lowestLevel; l-- {
		for _, c := range ccl[l] {
			if !cv.containsCell(ancestorNoHigherThanNode(c)) {
				cv = append(cv, &node{c: c})
			}
		}
	}
	return cv
}

// ancestorNoHigherThanNode finds an ancestor at a level no higher than node level for a cell.
// If the input cell is at node (or higher) level, will return the cell itself.
func ancestorNoHigherThanNode(c Cell) Cell {
	if c.AtOrHigherThanNode() || c.GetParent() == nil {
		return c
	} else {
		return ancestorNoHigherThanNode(c.GetParent())
	}
}

func (cv clusterView) containsCell(c Cell) bool {
	for _, n := range cv {
		if CellEqual(c, n.c) {
			return true
		}
	}
	return false
}

// Methods for sorting nodes in a clusterView.
func (cv clusterView) Len() int {
	return len(cv)
}

// We sort the nodes in decreasing significance of:
// (1) if the node is healthy (avoid unhealthy),
// (2) if the node is suggested (avoid non-suggested),
// (3) usedSkuNumSamePriority (more is preferred),
// (4) usedSkuNumHigherPriority (less is preferred).
func (cv clusterView) Less(i int, j int) bool {
	if cv[i].healthy != cv[j].healthy {
		return cv[i].healthy
	} else if cv[i].suggested != cv[j].suggested {
		return cv[i].suggested
	} else if cv[i].usedSkuNumSamePriority > cv[j].usedSkuNumSamePriority {
		return true
	} else if cv[i].usedSkuNumSamePriority < cv[j].usedSkuNumSamePriority {
		return false
	} else if cv[i].usedSkuNumHigherPriority < cv[j].usedSkuNumHigherPriority {
		return true
	} else {
		return false
	}
}

func (cv clusterView) Swap(i int, j int) {
	cv[i], cv[j] = cv[j], cv[i]
}

// updateClusterView updates the SKU numbers of the nodes for the sorting.
func (t *topologyAwareScheduler) updateClusterView(
	p CellPriority,
	suggestedNodes common.Set,
	ignoreSuggestedNodes bool) {

	for _, n := range t.cv {
		n.updateUsedSkuNumForPriority(p, t.crossPriorityPack)
		n.healthy, n.suggested, n.nodeAddress = nodeHealthyAndInSuggested(n, suggestedNodes, ignoreSuggestedNodes)
	}
}

func nodeHealthyAndInSuggested(
	n *node,
	suggestedNodes common.Set,
	ignoreSuggestedNodes bool) (
	healthy bool,
	suggested bool,
	addr api.CellAddress) {

	switch v := n.c.(type) {
	case *PhysicalCell:
		nodeNames, _ := v.GetPhysicalPlacement()
		return v.IsHealthy(),
			ignoreSuggestedNodes || suggestedNodes.Contains(nodeNames[0]),
			n.c.GetAddress()
	case *VirtualCell:
		if pn := v.GetPhysicalCell(); pn != nil {
			nodeNames, _ := pn.GetPhysicalPlacement()
			return pn.IsHealthy(),
				ignoreSuggestedNodes || suggestedNodes.Contains(nodeNames[0]),
				pn.GetAddress()
		}
	}
	return true, true, ""
}

// findNodesForPods finds a set of nodes that can accommodate the device requirements of the pods.
func findNodesForPods(cv clusterView, skuNums []int32) (pickedNodeIndices []int32, failedReason string) {
	// sort the nodes according to sku numbers in each node.
	// this is achieved through the Less method defined in type clusterView.
	// TODO: Ensure Opportunistic Pods also can always can find the solution, regardless of
	//  the iteration order.
	//  For example:
	//   1. clusterView = 2-device Node, 1-device Node
	//   2. skuNums = 1-device Pod, 2-device Pod
	//   First 1-device Pod may allocate to 2-device Node, but the latter pod cannot be fitted anymore.
	sort.Stable(cv)
	pickedNodeIndices = make([]int32, len(skuNums)) // indices of the currently picked nodes
	podIndex := 0
	pickedSkuNum := int32(0)
	var n *node
	for nodeIndex := 0; nodeIndex < len(cv); {
		n = cv[nodeIndex]
		if n.freeSkuNumAtPriority-pickedSkuNum >= skuNums[podIndex] {
			// fail when encountering a node that is either bad or not within suggested nodes
			if !n.healthy {
				return nil, fmt.Sprintf(
					"have to use at least one bad node %v", n.nodeAddress)
			}
			if !n.suggested {
				return nil, fmt.Sprintf(
					"have to use at least one non-suggested node %v", n.nodeAddress)
			}
			pickedNodeIndices[podIndex] = int32(nodeIndex)
			pickedSkuNum += skuNums[podIndex]
			podIndex++
			if podIndex == len(skuNums) {
				return pickedNodeIndices, ""
			}
		} else {
			pickedSkuNum = 0
			nodeIndex++
		}
	}
	return nil, "insufficient capacity"
}

// findDevicesInNode finds a set of devices with the best affinity in a node for a pod.
func findDevicesInNode(
	n Cell,
	skuNum int32,
	p CellPriority,
	availableDevices CellList,
	levelSkuNum map[CellLevel]int32) (CellList, CellList) {

	// indices of the currently picked devices
	currentDeviceIndices := make([]int32, skuNum)
	// affinity of the currently picked devices, defined as the lowest common ancestor
	// of the devices in the cell hierarchy (lower level means better affinity)
	currentAffinity := make(CellList, skuNum)
	// devices with the best affinity ever seen
	bestAffinityDevices := make(CellList, skuNum)
	// indices of the devices with the best affinity ever seen
	bestAffinityDeviceIndices := make([]int32, skuNum)
	// the best affinity ever seen (i.e., lowest level of lowest common ancestor of a set of devices)
	bestAffinity := highestLevel
	// the optimal affinity for the SKU number, i.e., the lowest possible of the lowest common ancestor of devices
	optimalAffinity := getOptimalAffinity(skuNum, levelSkuNum)

	if availableDevices == nil {
		availableDevices = CellList{}
		preemptibleDevices := CellList{}
		availableDevices, preemptibleDevices = getDevicesFromNode(n, p, availableDevices, preemptibleDevices)
		// free devices will be used first (before preemptible devices)
		availableDevices = append(availableDevices, preemptibleDevices...)
	}
	availableDeviceIndex := int32(0)
	searchDeviceIndex := int32(0)
	var device Cell
	for {
		for availableDeviceIndex < int32(len(availableDevices)) {
			device = availableDevices[availableDeviceIndex]
			currentDeviceIndices[searchDeviceIndex] = availableDeviceIndex
			if searchDeviceIndex == 0 {
				currentAffinity[searchDeviceIndex] = device
			} else {
				currentAffinity[searchDeviceIndex] = findLCA(device, currentAffinity[searchDeviceIndex-1])
				// pruning: if the current LCA has been higher than the lowest ever,
				// the node will be skipped
				if (currentAffinity[searchDeviceIndex] == nil && bestAffinity < highestLevel) ||
					(currentAffinity[searchDeviceIndex] != nil && currentAffinity[searchDeviceIndex].GetLevel() > bestAffinity) {
					availableDeviceIndex++
					continue
				}
			}
			if searchDeviceIndex == skuNum-1 {
				foundOptimalAffinity := false
				bestAffinity, foundOptimalAffinity = checkCurrentDevices(
					currentAffinity[len(currentAffinity)-1].GetLevel(),
					availableDevices,
					currentDeviceIndices,
					bestAffinity,
					bestAffinityDevices,
					bestAffinityDeviceIndices,
					optimalAffinity)
				if foundOptimalAffinity {
					// early stop: return if the solution is optimal (i.e., all buddies)
					availableDevices = removePickedDevices(availableDevices, bestAffinityDeviceIndices)
					return bestAffinityDevices, availableDevices
				}
			} else {
				searchDeviceIndex++
			}
			availableDeviceIndex++
		}
		searchDeviceIndex--
		if searchDeviceIndex < 0 {
			if bestAffinity == highestLevel {
				// Unreachable
				panic(fmt.Sprintf("Assert Failure: failed to allocate %v devices in picked node %v", skuNum, n.GetAddress()))
			}
			availableDevices = removePickedDevices(availableDevices, bestAffinityDeviceIndices)
			return bestAffinityDevices, availableDevices
		}
		availableDeviceIndex = currentDeviceIndices[searchDeviceIndex] + 1
	}
}

// getOptimalAffinity calculates the optimal affinity for a given SKU number.
func getOptimalAffinity(skuNum int32, levelSkuNum map[CellLevel]int32) CellLevel {
	for l := CellLevel(1); l <= CellLevel(len(levelSkuNum)); l++ {
		if levelSkuNum[l] >= skuNum {
			return l
		}
	}

	// Unreachable
	panic(fmt.Sprintf("Assert Failure: pod allocated a node but exceeds the capacity of the current chain"))
}

// checkCurrentDevices checks if the currently picked devices have the lowest LCA. It also checks if the solution
// is optimal (if the devices are all buddies).
func checkCurrentDevices(
	affinity CellLevel,
	devices CellList,
	currentIndices []int32,
	bestAffinity CellLevel,
	bestAffinityDevices CellList,
	bestAffinityDeviceIndices []int32,
	optimalAffinity CellLevel) (CellLevel, bool) {

	if affinity < bestAffinity {
		copy(bestAffinityDeviceIndices, currentIndices)
		for i := 0; i < len(currentIndices); i++ {
			bestAffinityDevices[i] = devices[currentIndices[i]]
		}
		if affinity == optimalAffinity {
			return affinity, true
		} else {
			return affinity, false
		}
	}
	return bestAffinity, false
}

// removePickedDevices remove picked devices from the available device list.
func removePickedDevices(devices CellList, indices []int32) CellList {
	for i, index := range indices {
		offset := int32(i)
		if i < len(indices)-1 {
			nextIndex := indices[i+1]
			copy(devices[index-offset:nextIndex-offset-1], devices[index+1:nextIndex])
		} else {
			copy(devices[index-offset:], devices[index+1:])
		}
	}
	for i := len(devices) - len(indices); i < len(devices); i++ {
		devices[i] = nil
	}
	return devices[:len(devices)-len(indices)]
}

// findLCA finds the lowest common ancestor of two cells (nil if they have no LCA).
func findLCA(lower Cell, higher Cell) Cell {
	for lower.GetLevel() < higher.GetLevel() {
		if lower.GetParent() == nil {
			return nil
		}
		lower = lower.GetParent()
	}
	if CellEqual(lower, higher) {
		return lower
	}
	for !CellEqual(lower.GetParent(), higher.GetParent()) {
		if lower.GetParent() == nil || higher.GetParent() == nil {
			return nil
		}
		lower = lower.GetParent()
		higher = higher.GetParent()
	}
	return lower.GetParent()
}

// getDevicesFromNode collects free devices and preemptible devices according to the priority.
func getDevicesFromNode(c Cell, p CellPriority, freeDevices CellList, preemptibleDevices CellList) (CellList, CellList) {
	if c.GetLevel() > 1 {
		for _, cc := range c.GetChildren() {
			freeDevices, preemptibleDevices = getDevicesFromNode(cc, p, freeDevices, preemptibleDevices)
		}
	} else if c.GetPriority() == freePriority {
		freeDevices = append(freeDevices, c)
	} else if c.GetPriority() < p {
		preemptibleDevices = append(preemptibleDevices, c)
	}
	return freeDevices, preemptibleDevices
}
