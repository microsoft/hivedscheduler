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
// It first tries to place pods to nodes with fewer free GPUs (i.e., packing), while trying to avoid preemptions.
// Then inside each node, it tries to allocate GPUs with better affinity.
type topologyAwareScheduler struct {
	// a list of nodes (node-level cells or top-level cells that are lower than node level)
	cv clusterView
	// GPU number at each level in the cell hierarchy. we use this to
	// calculate the optimal affinity for a given GPU number.
	levelGpuNum map[CellLevel]int32
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
	levelGpuNum map[CellLevel]int32,
	crossPriorityPack bool) *topologyAwareScheduler {

	return &topologyAwareScheduler{
		cv:                newClusterView(ccl),
		levelGpuNum:       levelGpuNum,
		crossPriorityPack: crossPriorityPack,
	}
}

func (t *topologyAwareScheduler) Schedule(
	podGpuNumbers map[int32]int32,
	p CellPriority,
	suggestedNodes common.Set,
	ignoreSuggestedNodes bool) (
	podPlacements map[int32][]CellList,
	failedReason string) {

	// GPU numbers of the pods to schedule
	var sortedPodGpuNumbers []int32
	for gpuNum, podNum := range podGpuNumbers {
		for i := int32(0); i < podNum; i++ {
			sortedPodGpuNumbers = append(sortedPodGpuNumbers, gpuNum)
		}
	}
	common.SortInt32(sortedPodGpuNumbers)

	// disable preemption first (reduce preemption)
	priority := opportunisticPriority
	t.updateClusterView(priority, suggestedNodes, ignoreSuggestedNodes)
	// try to fit the pods to a set of nodes
	selectedNodeIndices, failedReason := findNodesForPods(t.cv, sortedPodGpuNumbers)
	// enable preemption if scheduling failed
	if selectedNodeIndices == nil && p > opportunisticPriority {
		priority = p
		t.updateClusterView(priority, suggestedNodes, ignoreSuggestedNodes)
		selectedNodeIndices, failedReason = findNodesForPods(t.cv, sortedPodGpuNumbers)
	}
	if selectedNodeIndices == nil {
		return nil, failedReason
	}
	// find GPUs inside the selected node for each pod
	selectedNodes := make(CellList, len(sortedPodGpuNumbers))
	for i := 0; i < len(selectedNodeIndices); i++ {
		selectedNodes[i] = t.cv[selectedNodeIndices[i]].c
	}
	selectedGpus := CellList{}
	nodeAvailableGpus := map[Cell]CellList{}
	podPlacements = map[int32][]CellList{}
	for podIndex := 0; podIndex < len(sortedPodGpuNumbers); podIndex++ {
		gpuNumber := sortedPodGpuNumbers[podIndex]
		n := selectedNodes[podIndex]
		// TODO: Optimize findNodesForPods and findGpusInNode together to get a better placement,
		//  such as also aware intra node topology when findNodesForPods.
		selectedGpus, nodeAvailableGpus[n] = findGpusInNode(n, gpuNumber, priority, nodeAvailableGpus[n], t.levelGpuNum)
		if podPlacements[gpuNumber] == nil {
			podPlacements[gpuNumber] = []CellList{}
		}
		podPlacements[gpuNumber] = append(podPlacements[gpuNumber], selectedGpus)
	}
	return podPlacements, ""
}

type node struct {
	c                        Cell            // a node-level cell or a top-level cell that is lower than node level
	freeGpuNumAtPriority     int32           // free GPU number at the priority of the pod to be scheduled (lower priority considered as free)
	usedGpuNumSamePriority   int32           // GPU number used by the same priority as that of the pod to be scheduled
	usedGpuNumHigherPriority int32           // GPU number used by higher priorities than that of the pod to be scheduled
	healthy                  bool            // if the node is healthy
	suggested                bool            // if the node is within suggested nodes
	nodeAddress              api.CellAddress // used for logging the node address when bad or not suggested
}

// When cross-priority packing is not enabled, we count the GPU numbers used by the current
// priority (n.usedGpuNumSamePriority), and the higher priorities (n.usedGpuNumHigherPriority), respectively.
// When sorting the nodes, nodes with higher usedGpuNumSamePriority and lower usedGpuNumHigherPriority
// will be preferred (i.e., pack pods inside the same priority, and stay from higher priorities).
// Note that in this case, the nodes may NOT be ordered in term of total used GPU number,
// which may result in feasible pod placements being not found.
//
// Otherwise, n.usedGpuNumSamePriority is set to the total used GPU number,
// so that nodes with more used GPUs will be preferred (i.e., pack pods globally across priorities).
// In this case a feasible pod placement is guaranteed to be found (as long as all nodes are in suggested nodes).
func (n *node) updateUsedGpuNumForPriority(p CellPriority, crossPriorityPack bool) {
	n.usedGpuNumSamePriority = n.c.GetUsedGpuNumAtPriorities()[p]
	n.usedGpuNumHigherPriority = 0
	n.freeGpuNumAtPriority = n.c.GetTotalGpuNum()
	for priority, num := range n.c.GetUsedGpuNumAtPriorities() {
		if crossPriorityPack {
			if priority != p {
				n.usedGpuNumSamePriority += num
			}
		} else if priority > p {
			n.usedGpuNumHigherPriority += num
		}
		if priority >= p {
			n.freeGpuNumAtPriority -= num
		}
	}
}

type clusterView []*node

func newClusterView(ccl ChainCellList) clusterView {
	var l CellLevel
	// TODO: currently if a top-level cell is lower than node level, it will be considered as a single node.
	// For example, 2 single GPU-level cells are considered as 2 nodes each with 1 GPU.
	// We cannot merge them because the 2 cells might be mapped to different physical nodes.
	// We plan to support using multiple cells in a best-effort manner (for example, schedule a 2-GPU pod
	// on 2 1-GPU cells, if we can find 2 1-GPU cells that can be mapped to the same physical node).
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
// (3) usedGpuNumSamePriority (more is preferred),
// (4) usedGpuNumHigherPriority (less is preferred).
func (cv clusterView) Less(i int, j int) bool {
	if cv[i].healthy != cv[j].healthy {
		return cv[i].healthy
	} else if cv[i].suggested != cv[j].suggested {
		return cv[i].suggested
	} else if cv[i].usedGpuNumSamePriority > cv[j].usedGpuNumSamePriority {
		return true
	} else if cv[i].usedGpuNumSamePriority < cv[j].usedGpuNumSamePriority {
		return false
	} else if cv[i].usedGpuNumHigherPriority < cv[j].usedGpuNumHigherPriority {
		return true
	} else {
		return false
	}
}

func (cv clusterView) Swap(i int, j int) {
	cv[i], cv[j] = cv[j], cv[i]
}

// updateClusterView updates the GPU numbers of the nodes for the sorting.
func (t *topologyAwareScheduler) updateClusterView(
	p CellPriority,
	suggestedNodes common.Set,
	ignoreSuggestedNodes bool) {

	for _, n := range t.cv {
		n.updateUsedGpuNumForPriority(p, t.crossPriorityPack)
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

// findNodesForPods finds a set of nodes that can accommodate the GPU requirements of the pods.
func findNodesForPods(cv clusterView, gpuNums []int32) (pickedNodeIndices []int32, failedReason string) {
	// sort the nodes according to gpu numbers in each node.
	// this is achieved through the Less method defined in type clusterView.
	// TODO: Ensure Opportunistic Pods also can always can find the solution, regardless of
	//  the iteration order.
	//  For example:
	//   1. clusterView = 2GPU Node, 1GPU Node
	//   2. gpuNums = 1GPU Pod, 2GPU Pod
	//   First 1GPU Pod may allocate to 2GPU Node, but the latter pod cannot be fitted anymore.
	sort.Stable(cv)
	pickedNodeIndices = make([]int32, len(gpuNums)) // indices of the currently picked nodes
	podIndex := 0
	pickedGpuNum := int32(0)
	var n *node
	for nodeIndex := 0; nodeIndex < len(cv); {
		n = cv[nodeIndex]
		if n.freeGpuNumAtPriority-pickedGpuNum >= gpuNums[podIndex] {
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
			pickedGpuNum += gpuNums[podIndex]
			podIndex++
			if podIndex == len(gpuNums) {
				return pickedNodeIndices, ""
			}
		} else {
			pickedGpuNum = 0
			nodeIndex++
		}
	}
	return nil, "insufficient capacity"
}

// findGpusInNode finds a set of GPUs with the best affinity in a node for a pod.
func findGpusInNode(
	n Cell,
	gpuNum int32,
	p CellPriority,
	availableGpus CellList,
	levelGpuNum map[CellLevel]int32) (CellList, CellList) {

	// indices of the currently picked GPUs
	currentGpuIndices := make([]int32, gpuNum)
	// affinity of the currently picked GPUs, defined as the lowest common ancestor
	// of the GPUs in the cell hierarchy (lower level means better affinity)
	currentAffinity := make(CellList, gpuNum)
	// GPUs with the best affinity ever seen
	bestAffinityGpus := make(CellList, gpuNum)
	// indices of the GPUs with the best affinity ever seen
	bestAffinityGpuIndices := make([]int32, gpuNum)
	// the best affinity ever seen (i.e., lowest level of lowest common ancestor of a set of GPUs)
	bestAffinity := highestLevel
	// the optimal affinity for the GPU number, i.e., the lowest possible of the lowest common ancestor of GPUs
	optimalAffinity := getOptimalAffinity(gpuNum, levelGpuNum)

	if availableGpus == nil {
		availableGpus = CellList{}
		preemptibleGpus := CellList{}
		availableGpus, preemptibleGpus = getGpusFromNode(n, p, availableGpus, preemptibleGpus)
		// free GPUs will be used first (before preemptible GPUs)
		availableGpus = append(availableGpus, preemptibleGpus...)
	}
	availableGpuIndex := int32(0)
	searchGpuIndex := int32(0)
	var gpu Cell
	for {
		for availableGpuIndex < int32(len(availableGpus)) {
			gpu = availableGpus[availableGpuIndex]
			currentGpuIndices[searchGpuIndex] = availableGpuIndex
			if searchGpuIndex == 0 {
				currentAffinity[searchGpuIndex] = gpu
			} else {
				currentAffinity[searchGpuIndex] = findLCA(gpu, currentAffinity[searchGpuIndex-1])
				// pruning: if the current LCA has been higher than the lowest ever,
				// the node will be skipped
				if (currentAffinity[searchGpuIndex] == nil && bestAffinity < highestLevel) ||
					(currentAffinity[searchGpuIndex] != nil && currentAffinity[searchGpuIndex].GetLevel() > bestAffinity) {
					availableGpuIndex++
					continue
				}
			}
			if searchGpuIndex == gpuNum-1 {
				foundOptimalAffinity := false
				bestAffinity, foundOptimalAffinity = checkCurrentGpus(
					currentAffinity[len(currentAffinity)-1].GetLevel(),
					availableGpus,
					currentGpuIndices,
					bestAffinity,
					bestAffinityGpus,
					bestAffinityGpuIndices,
					optimalAffinity)
				if foundOptimalAffinity {
					// early stop: return if the solution is optimal (i.e., all buddies)
					availableGpus = removePickedGpus(availableGpus, bestAffinityGpuIndices)
					return bestAffinityGpus, availableGpus
				}
			} else {
				searchGpuIndex++
			}
			availableGpuIndex++
		}
		searchGpuIndex--
		if searchGpuIndex < 0 {
			if bestAffinity == highestLevel {
				// Unreachable
				panic(fmt.Sprintf("Assert Failure: failed to allocate %v GPUs in picked node %v", gpuNum, n.GetAddress()))
			}
			availableGpus = removePickedGpus(availableGpus, bestAffinityGpuIndices)
			return bestAffinityGpus, availableGpus
		}
		availableGpuIndex = currentGpuIndices[searchGpuIndex] + 1
	}
}

// getOptimalAffinity calculates the optimal affinity for a given GPU number.
func getOptimalAffinity(gpuNum int32, levelGpuNum map[CellLevel]int32) CellLevel {
	for l := CellLevel(1); l <= CellLevel(len(levelGpuNum)); l++ {
		if levelGpuNum[l] >= gpuNum {
			return l
		}
	}

	// Unreachable
	panic(fmt.Sprintf("Assert Failure: pod allocated a node but exceeds the capacity of the current chain"))
}

// checkCurrentGpus checks if the currently picked GPUs have the lowest LCA. It also checks if the solution
// is optimal (if the GPUs are all buddies).
func checkCurrentGpus(
	affinity CellLevel,
	gpus CellList,
	currentIndices []int32,
	bestAffinity CellLevel,
	bestAffinityGpus CellList,
	bestAffinityGpuIndices []int32,
	optimalAffinity CellLevel) (CellLevel, bool) {

	if affinity < bestAffinity {
		copy(bestAffinityGpuIndices, currentIndices)
		for i := 0; i < len(currentIndices); i++ {
			bestAffinityGpus[i] = gpus[currentIndices[i]]
		}
		if affinity == optimalAffinity {
			return affinity, true
		} else {
			return affinity, false
		}
	}
	return bestAffinity, false
}

// removePickedGpus remove picked GPUs from the available GPU list.
func removePickedGpus(gpus CellList, indices []int32) CellList {
	for i, index := range indices {
		offset := int32(i)
		if i < len(indices)-1 {
			nextIndex := indices[i+1]
			copy(gpus[index-offset:nextIndex-offset-1], gpus[index+1:nextIndex])
		} else {
			copy(gpus[index-offset:], gpus[index+1:])
		}
	}
	for i := len(gpus) - len(indices); i < len(gpus); i++ {
		gpus[i] = nil
	}
	return gpus[:len(gpus)-len(indices)]
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

// getGpusFromNode collects free GPUs and preemptible GPUs according to the priority.
func getGpusFromNode(c Cell, p CellPriority, freeGpus CellList, preemptibleGpus CellList) (CellList, CellList) {
	if c.GetLevel() > 1 {
		for _, cc := range c.GetChildren() {
			freeGpus, preemptibleGpus = getGpusFromNode(cc, p, freeGpus, preemptibleGpus)
		}
	} else if c.GetPriority() == freePriority {
		freeGpus = append(freeGpus, c)
	} else if c.GetPriority() < p {
		preemptibleGpus = append(preemptibleGpus, c)
	}
	return freeGpus, preemptibleGpus
}
