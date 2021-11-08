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
	apiv2 "github.com/microsoft/hivedscheduler/pkg/api/v2"
)

// skuCell type for selected level cell in virtual cluster view
type skuCell struct {
	cell                          Cell            // within cell, whose level maybe higher or lower than node level
	freeLeafCellNumAtPriority     int32           // free leaf cell number at the priority of the pod to be scheduled (lower priority considered as free)
	usedLeafCellNumAtPriority     int32           // used leaf cell number at the priority of the pod to be scheduler
	usedLeafCellNumHigherPriority int32           // used leaf cell number by higher priorities than the pod to be scheduler
	healthy                       bool            // if the cell is healthy
	address                       api.CellAddress // used for logging the cell address when bad or not suggested
}

// virtual cluster view
type skuClusterView []*skuCell

// topologyGuaranteeScheduler can schedule a group of pods with guaranteed affinity requirement (withinOneCell, e.g., within one rack),
// and each pod can specify arbitrary cell types (e.g., non-leaf cell).
// It first tries to place pod group without preemption, then enable preemption if schedule failed.
// For each try, it will find within cells for each pod group, then find cells for each pods with better affinity.
type topologyGuaranteeScheduler struct {
	// cell list for each level in a chain.
	chainCellList ChainCellList
	// leaf cell number at each level in the cell hierarchy. we use this to
	// calculate the optimal affinity for a given leaf cell number.
	levelLeafCellNum map[CellLevel]int32
	// cell type to cell level in a chain.
	cellLevels map[api.CellType]CellLevel
	// pack pods cross different priorities, or inside each priority. the former is for intra-VC scheduling,
	// because high-priority can avoid preemption in the whole cluster view,
	// and hence we can pack pods with different priorities.
	// the latter is for opportunistic pod scheduling (stay away from guaranteed pods),
	// because guaranteed pods can avoid preempting opportunistic pods only among buddy cells (this is decided
	// by the buddy cell allocation algorithm).
	crossPriorityPack bool
}

// NewTopologyGuaranteeScheduler initializes the scheduler
func NewTopologyGuaranteeScheduler(
	chainCellList ChainCellList,
	levelLeafCellNum map[CellLevel]int32,
	cellLevels map[api.CellType]CellLevel,
	crossPriorityPack bool) *topologyGuaranteeScheduler {

	return &topologyGuaranteeScheduler{
		chainCellList:     chainCellList,
		levelLeafCellNum:  levelLeafCellNum,
		cellLevels:        cellLevels,
		crossPriorityPack: crossPriorityPack,
	}
}

func (s *topologyGuaranteeScheduler) Schedule(
	podRootGroup *apiv2.PodGroupSpec,
	priority CellPriority) (
	placement PodGroupPlacement,
	failedReason string) {

	// sort pods in descending order by counting leaf cell number
	s.sortPodGroup(podRootGroup)

	// disable preemption first to reduce preemption, try to schedule
	placement, failedReason = s.findCellsForPodGroup(podRootGroup, opportunisticPriority, nil, &placement)

	// enable preemption if scheduling failed
	if failedReason != "" && priority > opportunisticPriority {
		placement, failedReason = s.findCellsForPodGroup(podRootGroup, priority, nil, &placement)
	}

	// convert cells to leaf cells in placement
	if failedReason == "" {
		for iter := placement.Iterator(); iter.HasNext(); {
			cells, leafCells := iter.Next(), CellList{}
			for _, c := range *cells {
				currLevelCells := CellList{c}
				for currLevelCells[0].GetLevel() > CellLevel(1) {
					childLevelCells := CellList{}
					for _, cc := range currLevelCells {
						childLevelCells = append(childLevelCells, cc.GetChildren()...)
					}
					currLevelCells = childLevelCells
				}
				leafCells = append(leafCells, currLevelCells...)
			}
			*cells = leafCells
		}
	}

	return placement, failedReason
}

func (s *topologyGuaranteeScheduler) sortPodGroup(podGroup *apiv2.PodGroupSpec) {
	sort.SliceStable(podGroup.Pods, func(i, j int) bool {
		return s.countLeafCellNums(podGroup.Pods[i]) > s.countLeafCellNums(podGroup.Pods[j])
	})
	sortedPods := []apiv2.PodGroupMemberSpec{}
	for _, p := range podGroup.Pods {
		for i := int32(0); i < p.PodMinNumber; i++ {
			sortedPods = append(sortedPods, p)
		}
	}
	podGroup.Pods = sortedPods

	sort.SliceStable(podGroup.ChildGroups, func(i, j int) bool {
		return s.countLeafCellNums(podGroup.ChildGroups[i]) > s.countLeafCellNums(podGroup.ChildGroups[j])
	})
	for _, g := range podGroup.ChildGroups {
		s.sortPodGroup(g)
	}
}

func (s *topologyGuaranteeScheduler) countLeafCellNums(x interface{}) int32 {
	count := int32(0)
	switch p := x.(type) {
	case apiv2.PodGroupMemberSpec:
		count = s.levelLeafCellNum[s.cellLevels[p.CellsPerPod.CellType]] * p.CellsPerPod.CellNumber
	case []apiv2.PodGroupMemberSpec:
		for _, pp := range p {
			count += s.countLeafCellNums(pp)
		}
	case *apiv2.PodGroupSpec:
		count += s.countLeafCellNums(p.Pods) + s.countLeafCellNums(p.ChildGroups)
	case []*apiv2.PodGroupSpec:
		for _, pp := range p {
			count += s.countLeafCellNums(pp)
		}
	}
	return count
}

func (s *topologyGuaranteeScheduler) findCellsForPodGroup(
	podGroup *apiv2.PodGroupSpec,
	priority CellPriority,
	within *skuCell,
	allocated *PodGroupPlacement) (
	placement PodGroupPlacement,
	failedReason string) {

	placement, failedReason = PodGroupPlacement{}, "no matched cells in vc"
	if _, ok := s.cellLevels[podGroup.WithinOneCell]; !ok && podGroup.WithinOneCell != "" {
		return placement, fmt.Sprintf(
			"%v, unknown withinOneCell %v", failedReason, podGroup.WithinOneCell)
	}

	cv := skuClusterView{nil}
	if level, ok := s.cellLevels[podGroup.WithinOneCell]; ok {
		cv = s.createSkuClusterView(within, level, priority)
	} else if within != nil {
		cv = s.createSkuClusterView(within, within.cell.GetLevel(), priority)
	}

	for _, withinCell := range cv {
		if len(podGroup.Pods) > 0 && withinCell != nil && !withinCell.healthy {
			return PodGroupPlacement{}, fmt.Sprintf(
				"have to use at least one bad cell %v", withinCell.address)
		}
		placement.podsPlacement, failedReason = s.findCellsForPods(podGroup.Pods, priority, withinCell, allocated)
		if failedReason == "" {
			for _, childGroup := range podGroup.ChildGroups {
				childPodsPlacement, childFailedReason := s.findCellsForPodGroup(childGroup, priority, withinCell, &placement)
				if childFailedReason != "" {
					placement.childGroupsPlacement, failedReason = nil, childFailedReason
					break
				}
				if placement.childGroupsPlacement == nil {
					placement.childGroupsPlacement = []*PodGroupPlacement{}
				}
				placement.childGroupsPlacement = append(placement.childGroupsPlacement, &childPodsPlacement)
			}
			if failedReason == "" {
				break
			}
		}
	}
	return placement, failedReason
}

func (s *topologyGuaranteeScheduler) findCellsForPods(
	pods []apiv2.PodGroupMemberSpec,
	priority CellPriority,
	within *skuCell,
	allocated *PodGroupPlacement) (
	placement []CellList,
	failedReason string) {

	placement, failedReason = []CellList{}, ""
	if pods == nil || len(pods) == 0 {
		return placement, failedReason
	}

	allocatedCells := CellList{}
	for iter := allocated.Iterator(); iter.HasNext(); {
		for _, c := range *iter.Next() {
			allocatedCells = append(allocatedCells, c)
		}
	}

	cv := skuClusterView{within}
	nodeLevel := s.getNodeLevel()
	if within == nil || within.cell.GetLevel() > nodeLevel {
		cv = s.createSkuClusterView(within, nodeLevel, priority)
	}

	withinCellIndex, podIndex := 0, 0
	for podIndex < len(pods) {
		if withinCellIndex >= len(cv) {
			return nil, "insufficient capacity"
		}
		withinCell := cv[withinCellIndex]
		if !withinCell.healthy {
			return nil, fmt.Sprintf(
				"have to use at least one bad cell %v", withinCell.address)
		}
		podPlacement := s.findCellsForSinglePod(pods[podIndex], priority, withinCell, allocatedCells)
		if podPlacement == nil {
			withinCellIndex++
		} else {
			placement = append(placement, podPlacement)
			allocatedCells = append(allocatedCells, podPlacement...)
			podIndex++
		}
	}

	return placement, failedReason
}

// findCellsForSinglePod finds a set of cells with the best affinity in a node for a pod in best effort.
func (s *topologyGuaranteeScheduler) findCellsForSinglePod(
	pod apiv2.PodGroupMemberSpec,
	priority CellPriority,
	withinCell *skuCell,
	allocatedCells CellList) CellList {

	currLevel := s.cellLevels[pod.CellsPerPod.CellType]
	availableCells, preemptibleCells := CellList{}, CellList{}
	availableCells, preemptibleCells = getFreeCellsAtLevel(
		withinCell.cell, currLevel, priority, allocatedCells, availableCells, preemptibleCells)
	// free leaf cells will be used first (before preemptible leaf cells)
	availableCells = append(availableCells, preemptibleCells...)
	if pod.CellsPerPod.CellNumber > int32(len(availableCells)) {
		return nil
	}

	var freeCell Cell
	freeCellIndex, searchCellIndex := int32(0), int32(0)
	// indices of the currently picked cells
	currentCellIndices := make([]int32, pod.CellsPerPod.CellNumber)
	// affinity of the currently picked cells, defined as the lowest common ancestor
	// of the leaf cells in the cell hierarchy (lower level means better affinity)
	currentAffinity := make(CellList, pod.CellsPerPod.CellNumber)
	// cells with the best affinity ever seen
	bestAffinityCells := make(CellList, pod.CellsPerPod.CellNumber)
	// indices of the cells with the best affinity ever seen
	bestAffinityCellIndices := make([]int32, pod.CellsPerPod.CellNumber)
	// the best affinity ever seen (i.e., lowest level of lowest common ancestor of a set of cells)
	bestAffinity := highestLevel
	// the optimal affinity for the cell number, i.e., the lowest possible of the lowest common ancestor of cells
	optimalAffinity := CellLevel(1)
	for l := CellLevel(currLevel); l <= CellLevel(len(s.levelLeafCellNum)); l++ {
		if s.levelLeafCellNum[l] >= s.levelLeafCellNum[currLevel]*pod.CellsPerPod.CellNumber {
			optimalAffinity = l
			break
		}
	}

	for {
		for freeCellIndex < int32(len(availableCells)) {
			freeCell = availableCells[freeCellIndex]
			currentCellIndices[searchCellIndex] = freeCellIndex
			if searchCellIndex == 0 {
				currentAffinity[searchCellIndex] = freeCell
			} else {
				currentAffinity[searchCellIndex] = findLCA(freeCell, currentAffinity[searchCellIndex-1])
				// pruning: if the current LCA has been higher than the lowest ever,
				// the node will be skipped
				if (currentAffinity[searchCellIndex] == nil && bestAffinity < highestLevel) ||
					(currentAffinity[searchCellIndex] != nil && currentAffinity[searchCellIndex].GetLevel() > bestAffinity) {
					freeCellIndex++
					continue
				}
			}
			if searchCellIndex == pod.CellsPerPod.CellNumber-1 {
				foundOptimalAffinity := false
				bestAffinity, foundOptimalAffinity = checkOptimalAffinityForCells(
					currentAffinity[len(currentAffinity)-1].GetLevel(),
					availableCells,
					currentCellIndices,
					bestAffinity,
					bestAffinityCells,
					bestAffinityCellIndices,
					optimalAffinity)
				if foundOptimalAffinity {
					// early stop: return if the solution is optimal (i.e., all buddies)
					return bestAffinityCells
				}
			} else {
				searchCellIndex++
			}
			freeCellIndex++
		}
		searchCellIndex--
		if searchCellIndex < 0 {
			if bestAffinity == highestLevel {
				// Unreachable
				panic(fmt.Sprintf("Assert Failure: failed to allocate %v cells in cell %v", pod.CellsPerPod.CellNumber, withinCell.address))
			}
			return bestAffinityCells
		}
		freeCellIndex = currentCellIndices[searchCellIndex] + 1
	}
}

func (s *topologyGuaranteeScheduler) getNodeLevel() CellLevel {
	for l := CellLevel(1); l <= CellLevel(len(s.chainCellList)); l++ {
		if s.chainCellList[l][0].AtOrHigherThanNode() {
			return l
		}
	}
	return -1
}

// getFreeCellsAtLevel collects free cells and preemptible cells at given level according to the priority.
// Sort cells when splitting so that cells need higher level split can be used later.
func getFreeCellsAtLevel(
	cell Cell,
	level CellLevel,
	priority CellPriority,
	allocatedCells CellList,
	availableCells CellList,
	preemptibleCells CellList) (
	CellList, CellList) {

	if cell.GetLevel() > level {
		cellChildren := cell.GetChildren()
		usedCellNums := make([]int32, len(cellChildren))
		for i, c := range cellChildren {
			usedCellNums[i] = 0
			for p, num := range c.GetUsedLeafCellNumAtPriorities() {
				if p >= priority {
					usedCellNums[i] += num
				}
			}
		}
		sort.SliceStable(cellChildren, func(i, j int) bool {
			return usedCellNums[i] > usedCellNums[j]
		})
		for _, c := range cellChildren {
			availableCells, preemptibleCells = getFreeCellsAtLevel(
				c, level, priority, allocatedCells, availableCells, preemptibleCells)
		}
	} else if cell.GetLevel() == level {
		isAllocated := false
		for _, c := range allocatedCells {
			if isAncestor(cell, c) || isAncestor(c, cell) {
				isAllocated = true
				break
			}
		}
		if !isAllocated {
			if cell.GetPriority() == freePriority {
				availableCells = append(availableCells, cell)
			} else if cell.GetPriority() < priority {
				preemptibleCells = append(preemptibleCells, cell)
			}
		}
	}
	return availableCells, preemptibleCells
}

// checkOptimalAffinityForCells checks if the currently picked cells have the lowest LCA.
// It also checks if the solution is optimal (if the leaf cells are all buddies).
func checkOptimalAffinityForCells(
	affinity CellLevel,
	availableCells CellList,
	currentCellIndices []int32,
	bestAffinity CellLevel,
	bestAffinityCells CellList,
	bestAffinityCellIndices []int32,
	optimalAffinity CellLevel) (CellLevel, bool) {

	if affinity < bestAffinity {
		copy(bestAffinityCellIndices, currentCellIndices)
		for i := 0; i < len(currentCellIndices); i++ {
			bestAffinityCells[i] = availableCells[currentCellIndices[i]]
		}
		if affinity == optimalAffinity {
			return affinity, true
		} else {
			return affinity, false
		}
	}
	return bestAffinity, false
}

// createSkuClusterView returns list of sku cells within
// the given cell, level and priority in virtual cluster view.
func (s *topologyGuaranteeScheduler) createSkuClusterView(
	withinCell *skuCell,
	withinLevel CellLevel,
	priority CellPriority) skuClusterView {

	cv := skuClusterView{}
	for l := withinLevel; l >= CellLevel(1); l-- {
		for _, c := range s.chainCellList[l] {
			if (withinCell != nil && !isAncestor(withinCell.cell, c)) ||
				cv.contains(ancestorNoLowerThanLevel(withinLevel, c)) {
				continue
			}
			cell := &skuCell{
				cell:                          c,
				freeLeafCellNumAtPriority:     c.GetTotalLeafCellNum(),
				usedLeafCellNumAtPriority:     0,
				usedLeafCellNumHigherPriority: 0,
				healthy:                       true,
				address:                       "",
			}
			for p, num := range c.GetUsedLeafCellNumAtPriorities() {
				if p >= priority {
					cell.freeLeafCellNumAtPriority -= num
				}
				if s.crossPriorityPack {
					cell.usedLeafCellNumAtPriority += num
				} else {
					if p == priority {
						cell.usedLeafCellNumAtPriority += num
					}
					if p > priority {
						cell.usedLeafCellNumHigherPriority += num
					}
				}
			}
			switch v := c.(type) {
			case *PhysicalCell:
				cell.healthy = v.IsHealthy()
				cell.address = c.GetAddress()
			case *VirtualCell:
				if pn := v.GetPhysicalCell(); pn != nil {
					cell.healthy = pn.IsHealthy()
					cell.address = pn.GetAddress()
				}
			}
			cv = append(cv, cell)
		}
	}
	sort.Stable(cv)
	return cv
}

// Len method for sorting sku cells in cluster view.
func (cv skuClusterView) Len() int {
	return len(cv)
}

// Less method for sorting sku cells in cluster view
// sort in the following order:
// 1. cell health (prefer healthy)
// 2. cell level (prefer lower)
// 3. usedLeafCellNumAtPriority (prefer higher)
// 4. usedLeafCellNumHigherPriority (prefer lower)
// 5. cell physical/virtual address (prefer lower)
//
// When crossPriorityPack is not enabled, we count the cell numbers used by the current
// priority (usedLeafCellNumAtPriority), and the higher priorities (usedLeafCellNumHigherPriority), respectively.
// When sorting the sku cells, cells with higher usedLeafCellNumAtPriority and lower usedLeafCellNumHigherPriority
// will be preferred (i.e., pack pods inside the same priority, and stay from higher priorities).
// Note that in this case, the sku cells may NOT be ordered in term of total used leaf cell number,
// which may result in feasible pod placements being not found.
//
// Otherwise, usedLeafCellNumAtPriority is set to the total used leaf cell number,
// so that nodes with more used leaf cells will be preferred (i.e., pack pods globally across priorities).
// In this case a feasible pod placement is guaranteed to be found (as long as all nodes are in suggested nodes).
func (cv skuClusterView) Less(i, j int) bool {
	if cv[i].healthy != cv[j].healthy {
		return cv[i].healthy
	}
	if cv[i].cell.GetLevel() != cv[j].cell.GetLevel() {
		return cv[i].cell.GetLevel() < cv[j].cell.GetLevel()
	}
	if cv[i].usedLeafCellNumAtPriority != cv[j].usedLeafCellNumAtPriority {
		return cv[i].usedLeafCellNumAtPriority > cv[j].usedLeafCellNumAtPriority
	}
	if cv[i].usedLeafCellNumHigherPriority != cv[j].usedLeafCellNumHigherPriority {
		return cv[i].usedLeafCellNumHigherPriority < cv[j].usedLeafCellNumHigherPriority
	}
	if cv[i].address != cv[j].address {
		return cv[i].address < cv[j].address
	}
	if cv[i].cell.GetAddress() != cv[j].cell.GetAddress() {
		return cv[i].cell.GetAddress() < cv[j].cell.GetAddress()
	}
	return true
}

// Swap method for sorting sku cells in cluster view.
func (cv skuClusterView) Swap(i int, j int) {
	cv[i], cv[j] = cv[j], cv[i]
}

func (cv skuClusterView) contains(cell Cell) bool {
	for _, withinCell := range cv {
		if CellEqual(cell, withinCell.cell) {
			return true
		}
	}
	return false
}

// ancestorNoLowerThanLevel returns the ancestor of the given cell
// and its level is no lower than given cell's level.
func ancestorNoLowerThanLevel(withinLevel CellLevel, cell Cell) Cell {
	if cell.GetLevel() >= withinLevel || cell.GetParent() == nil {
		return cell
	} else {
		return ancestorNoLowerThanLevel(withinLevel, cell.GetParent())
	}
}

// isAncestor determines whether the given ancestor
// is the ancestor of the given cell.
func isAncestor(ancestor Cell, cell Cell) bool {
	if CellEqual(ancestor, cell) {
		return true
	}
	if cell.GetLevel() >= ancestor.GetLevel() || cell.GetParent() == nil {
		return false
	}
	return isAncestor(ancestor, cell.GetParent())
}
