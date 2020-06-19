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
	"k8s.io/klog"
	"sort"
)

// buddyAlloc is used for allocating a free physical cell to a preassigned virtual cell.
// It splits a higher-level cell when there is no free cell at the current level.
// Note that this is a backtracking version of the buddy alloc algorithm (slightly more complex
// than the simple recursive version present in the paper). Ideally we do not need to backtrack
// because buddy alloc has safety guarantee (the simple algorithm already works).
// But it's possible that the cell allocated by the non-backtrack buddy alloc is temporarily unavailable
// (e.g., it's bad or not within K8s suggested nodes), then we need to do backtracking search
// on the free cell list so as to find an available cell.
func buddyAlloc(
	cell *cellBindingPathVertex,
	freeList ChainCellList,
	currentLevel CellLevel,
	suggestedNodes common.Set,
	avoidNonSuggestedNodes bool,
	bindings map[api.CellAddress]*PhysicalCell) bool {

	if currentLevel == cell.cell.GetLevel() {
		ok, pickedCells := mapVirtualCellsToPhysical(
			[]*cellBindingPathVertex{cell},
			freeList[currentLevel],
			suggestedNodes,
			avoidNonSuggestedNodes,
			bindings,
			true)
		if ok {
			for _, c := range pickedCells {
				freeList.remove(c, currentLevel)
			}
			return true
		}
		return false
	}
	freeCells := getUsablePhysicalCells(freeList[currentLevel], 1, suggestedNodes, avoidNonSuggestedNodes)
	if freeCells == nil {
		return false
	}
	for _, c := range freeCells {
		freeList[currentLevel-1] = append(freeList[currentLevel-1], c.GetChildren()...)
		if buddyAlloc(cell, freeList, currentLevel-1, suggestedNodes, avoidNonSuggestedNodes, bindings) {
			freeList.remove(c, currentLevel)
			return true
		} else {
			freeList[currentLevel-1] = nil
		}
	}
	return false
}

// getLowestFreeCellLevel returns the lowest level in the free cell list with at least one free cell.
func getLowestFreeCellLevel(freeList ChainCellList, l CellLevel) CellLevel {
	for ; l <= CellLevel(len(freeList)); l++ {
		if len(freeList[l]) != 0 {
			return l
		}
	}
	panic(fmt.Sprintf("VC Safety Broken: free cell not found "+
		"even split to the highest level %v", l-1))
}

// mapVirtualPlacementToPhysical maps cells in a VC placement to the physical cluster.
// For the preassigned cells, it will call buddy alloc to map them;
// For the nonPreassigned cells, it will map them following the topology inside the corresponding preassigned cells.
func mapVirtualPlacementToPhysical(
	preassignedCells []*cellBindingPathVertex,
	nonPreassignedCells [][]*cellBindingPathVertex,
	freeList ChainCellList,
	suggestedNodes common.Set,
	avoidNonSuggestedNodes bool,
	bindings map[api.CellAddress]*PhysicalCell) bool {

	for _, c := range preassignedCells {
		if !buddyAlloc(c, freeList, getLowestFreeCellLevel(
			freeList, c.cell.GetLevel()), suggestedNodes, avoidNonSuggestedNodes, bindings) {
			return false
		}
	}
	for _, cells := range nonPreassignedCells {
		ok, _ := mapVirtualCellsToPhysical(
			cells, cells[0].cell.GetParent().(*VirtualCell).GetPhysicalCell().GetChildren(),
			suggestedNodes, avoidNonSuggestedNodes, bindings, false)
		if !ok {
			return false
		}
	}
	return true
}

// getUsablePhysicalCells returns the usable cells in a physical cell list for cell binding.
func getUsablePhysicalCells(
	candidates CellList,
	numNeeded int32,
	suggestedNodes common.Set,
	avoidNonSuggestedNodes bool) (usableCandidates CellList) {

	for i := range candidates {
		c := candidates[i].(*PhysicalCell)
		// skip the cell if it is already bound
		if c.GetVirtualCell() != nil {
			continue
		}
		// skip the cell if it is a bad node
		nodes, _ := c.GetPhysicalPlacement()
		if len(nodes) == 1 && !c.IsHealthy() {
			continue
		}
		if avoidNonSuggestedNodes {
			// skip the cell if all of its nodes are not within suggested nodes (if only some of them are,
			// we can possibly find usable cells when searching in the next level)
			allNonSuggested := true
			for _, n := range nodes {
				if suggestedNodes.Contains(n) {
					allNonSuggested = false
					break
				}
			}
			if allNonSuggested {
				continue
			}
		}
		usableCandidates = append(usableCandidates, c)
	}
	// the usable candidates should be no fewer than needed
	if int32(len(usableCandidates)) < numNeeded {
		return nil
	}
	// prioritize the cells with fewer opportunistic pods (to reduce preemption)
	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].GetUsedGpuNumAtPriorities()[opportunisticPriority] <
			candidates[j].GetUsedGpuNumAtPriorities()[opportunisticPriority]
	})
	return usableCandidates
}

// mapVirtualCellsToPhysical maps a set of virtual cells to a set of candidate physical cells,
// and recursively for their children. When mapping the children, the candidates for the children
// will be the children of the currently picked physical cell (this way we will maintain the equivalence
// of topology inside a preassigned cell and that of its physical cell).
// Similar to buddyAlloc, this is a backtracking search:
// if the current candidate cells cannot satisfy the virtual cells (e.g., they are bad or not within
// K8s suggested nodes), we will backtrack to the last level and try other candidates.
func mapVirtualCellsToPhysical(
	cells []*cellBindingPathVertex,
	candidates CellList,
	suggestedNodes common.Set,
	avoidNonSuggestedNodes bool,
	bindings map[api.CellAddress]*PhysicalCell,
	returnPicked bool) (ok bool, pickedCells CellList) {

	candidates = getUsablePhysicalCells(candidates, int32(len(cells)), suggestedNodes, avoidNonSuggestedNodes)
	if candidates == nil {
		return false, nil
	}
	cellIndex := int32(0)
	candidateIndex := int32(0)
	pickedCandidateIndices := make([]int32, len(cells))
	pickedIndexSet := common.NewSet()
	for cellIndex >= 0 {
		for candidateIndex = pickedCandidateIndices[cellIndex]; candidateIndex < int32(len(candidates)); candidateIndex++ {
			if pickedIndexSet.Contains(candidateIndex) {
				continue
			}
			candidate := candidates[candidateIndex].(*PhysicalCell)
			picked := false
			if candidate.GetLevel() == lowestLevel {
				picked = true
				// record bindings for the lowest-level cells
				bindings[cells[cellIndex].cell.GetAddress()] = candidate
			} else {
				// search for the next level
				picked, _ = mapVirtualCellsToPhysical(
					cells[cellIndex].childrenToBind,
					candidate.GetChildren(),
					suggestedNodes,
					avoidNonSuggestedNodes,
					bindings,
					false)
			}
			if picked {
				pickedCandidateIndices[cellIndex] = candidateIndex
				pickedIndexSet.Add(candidateIndex)
				if cellIndex == int32(len(cells))-1 {
					if !returnPicked {
						return true, nil
					}
					for _, index := range pickedCandidateIndices {
						pickedCells = append(pickedCells, candidates[index])
					}
					return true, pickedCells
				}
				break
			}
		}
		if candidateIndex == int32(len(candidates)) {
			cellIndex--
			if cellIndex >= 0 {
				pickedIndexSet.Delete(pickedCandidateIndices[cellIndex])
				pickedCandidateIndices[cellIndex]++
			}
		} else {
			cellIndex++
		}
	}
	return false, nil
}

// mapPhysicalCellToVirtual is an inverse operation of mapVirtualCellToPhysical,
// used for finding the virtual cell when adding an allocated pod.
// It maps a physical cell (possibly allocated to a non-preassigned virtual cell) to the corresponding virtual cell.
func mapPhysicalCellToVirtual(
	c *PhysicalCell,
	vccl ChainCellList,
	preassignedLevel CellLevel,
	p CellPriority) (*VirtualCell, string) {

	if c.GetVirtualCell() != nil {
		return c.GetVirtualCell(), ""
	} else if c.GetLevel() == preassignedLevel {
		if preassignedVirtual := getLowestPriorityVirtualCell(vccl[preassignedLevel], p); preassignedVirtual == nil {
			return nil, fmt.Sprintf("insufficient free cell in the VC at the preassigned level (%v)", preassignedLevel)
		} else {
			return preassignedVirtual, ""
		}
	} else if c.GetParent() == nil {
		return nil, fmt.Sprintf(
			"physical and virtual cell hierarchies not match (cannot reach the preassigned level %v in physical)",
			preassignedLevel)
	} else {
		parentVirtual, message := mapPhysicalCellToVirtual(c.GetParent().(*PhysicalCell), vccl, preassignedLevel, p)
		if parentVirtual == nil {
			return nil, message
		} else {
			return getLowestPriorityVirtualCell(parentVirtual.GetChildren(), p), ""
		}
	}
}

// getLowestPriorityVirtualCell returns a virtual cell with the lowest priority
// among those whose priorities are lower than the given priority (p).
// We don't just return a free cell because in cases like reconfiguration, there might be no
// free cell left. Then we allow to return a non-free, but lowest-priority cell and preempt it.
func getLowestPriorityVirtualCell(cl CellList, p CellPriority) (lowestPriorityCell *VirtualCell) {
	lowestPriority := maxGuaranteedPriority
	for _, c := range cl {
		vc := c.(*VirtualCell)
		priority := vc.GetPriority()
		if priority == freePriority {
			if vc.GetPhysicalCell() == nil {
				return vc
			} else {
				// Although freePriority must be the lowest priority, we should not return a free cell
				// with binding, because such binding cannot be preempted (e.g., the binding is
				// created for doomed bad cell)
				continue
			}
		} else if priority < p && priority < lowestPriority {
			lowestPriority = priority
			lowestPriorityCell = vc
		}
	}
	return lowestPriorityCell
}

// getUnboundVirtualCell returns a virtual cell that is not bound to a physical cell.
func getUnboundVirtualCell(cl CellList) *VirtualCell {
	for _, c := range cl {
		if vc := c.(*VirtualCell); vc.GetPhysicalCell() == nil {
			return vc
		}
	}
	return nil
}

// bindCell binds a virtual cell to a physical cell and its parent recursively.
// bindCell always starts from the lowest level, i.e., GPU-level cells.
func bindCell(pc *PhysicalCell, vc *VirtualCell) {
	for vc.GetPhysicalCell() == nil {
		pc.SetVirtualCell(vc)
		vc.SetPhysicalCell(pc)
		klog.Infof("Virtual cell %v is bound to physical cell %v", vc.GetAddress(), pc.GetAddress())
		if vc.GetParent() == nil {
			break
		}
		vc = vc.GetParent().(*VirtualCell)
		pc = pc.GetParent().(*PhysicalCell)
	}
}

// unbindCell unbinds a virtual cell with a physical cell and its parent recursively.
// unbindCell always starts from the lowest level, i.e., GPU-level cells.
func unbindCell(c *PhysicalCell) {
	boundVirtual := c.GetVirtualCell()
	for !boundVirtual.GetPhysicalCell().IsPinned() {
		boundPhysical := boundVirtual.GetPhysicalCell()
		klog.Infof("Virtual cell %v is unbound from physical cell %v",
			boundVirtual.GetAddress(), boundPhysical.GetAddress())
		boundVirtual.SetPhysicalCell(nil)
		boundPhysical.SetVirtualCell(nil)
		if boundVirtual.GetParent() == nil {
			return
		} else {
			for _, cc := range boundVirtual.GetParent().GetChildren() {
				if child := cc.(*VirtualCell); child.GetPhysicalCell() != nil {
					return
				}
			}
			boundVirtual = boundVirtual.GetParent().(*VirtualCell)
		}
	}
}

// setCellPriority sets priority for a cell and its parent recursively, guaranteeing that
// the priority of a cell is the max of those of its children.
// setCellPriority always starts from the lowest level, i.e., GPU-level cells.
func setCellPriority(c Cell, p CellPriority) {
	originalPriority := c.GetPriority()
	c.SetPriority(p)
	if parent := c.GetParent(); parent != nil {
		if p > parent.GetPriority() {
			setCellPriority(parent, p)
		} else if originalPriority == parent.GetPriority() && p < originalPriority {
			maxBuddyPriority := freePriority
			for _, buddy := range parent.GetChildren() {
				if buddy.GetPriority() > maxBuddyPriority {
					maxBuddyPriority = buddy.GetPriority()
				}
			}
			setCellPriority(parent, maxBuddyPriority)
		}
	}
}

// updateUsedGpuNumAtPriority updates the number of used GPUs at a priority for a cell
// and its parent recursively.
func updateUsedGpuNumAtPriority(c Cell, p CellPriority, increase bool) {
	for c != nil {
		delta := int32(-1)
		if increase {
			delta = 1
		}
		c.IncreaseUsedGpuNumAtPriority(p, delta)
		c = c.GetParent()
	}
}
