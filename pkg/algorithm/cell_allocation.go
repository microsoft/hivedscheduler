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
	"math"
	"math/rand"
)

// buddyAlloc is used for allocating a free physical cell to a preassigned virtual cell.
// It splits a higher-level cell when there is no free cell at the current level.
// As the input cell list is a copy of the real free list and hence is one-off,
// we won't remove a returned cell from it.
func buddyAlloc(freeList ChainCellList, level CellLevel, suggestedNodes common.Set) *PhysicalCell {
	if len(freeList[level]) == 0 && level < CellLevel(len(freeList)) {
		higherCell := buddyAlloc(freeList, level+1, suggestedNodes)
		if higherCell != nil {
			freeList[level] = append(freeList[level], higherCell.GetChildren()...)
		}
	}
	if len(freeList[level]) == 0 {
		return nil
	}
	return getFewestOpporPhysicalCell(freeList[level], suggestedNodes)
}

// mapVirtualCellToPhysical maps a virtual cell to a physical cell. This mapping is done in two steps:
// We first map its preassigned cell to a physical cell, by calling the buddy cell allocation algorithm.
// We then map the virtual cell inside the preassigned one to a physical cell, while maintaining
// equivalence of the topology of the preassigned cell and that of its physical cell.
func mapVirtualCellToPhysical(c *VirtualCell, freeList ChainCellList, suggestedNodes common.Set) *PhysicalCell {
	pac := c.GetPreAssignedCell()
	// check if the preassigned cell has been (temporarily) bound to a physical cell
	preassignedPhysical := pac.GetPhysicalCell()
	if preassignedPhysical == nil {
		preassignedPhysical = pac.GetPreBoundPhysicalCell()
	}
	if preassignedPhysical == nil {
		// Allocate a new physical cell to the preassigned cell. Input a copy of the free cell list
		// because during the scheduling we should not make in-place change to the data structures
		// (they can be modified only when adding or deleting pods)
		c := buddyAlloc(freeList.shallowCopy(), pac.GetLevel(), suggestedNodes)
		if c == nil {
			panic(fmt.Sprintf(
				"VC Safety Broken: Cannot find physical cell for a VC cell: %v", pac.GetAddress()))
		} else {
			preassignedPhysical = c
			// create binding (which is temporary and will be cleared after the scheduling,
			// same reason as above)
			pac.SetPreBoundPhysicalCell(preassignedPhysical)
			preassignedPhysical.SetPreBoundVirtualCell(pac)
		}
	}
	return mapNonPreassignedVirtualToPhysical(c, suggestedNodes)
}

// mapNonPreassignedVirtualToPhysical is used for cell binding inside a preassigned virtual cell.
// It maps a virtual cell (possibly inside a preassigned one) to one of the cell inside the physical cell
// allocated to the preassigned cell. This operation keeps the inner-cell topology equivalent,
// by recursively binding the cells inside the preassigned one.
func mapNonPreassignedVirtualToPhysical(c *VirtualCell, suggestedNodes common.Set) *PhysicalCell {
	if c.GetPhysicalCell() != nil {
		return c.GetPhysicalCell()
	} else if c.GetPreBoundPhysicalCell() != nil {
		return c.GetPreBoundPhysicalCell()
	} else {
		parentPhysical := mapNonPreassignedVirtualToPhysical(c.GetParent().(*VirtualCell), suggestedNodes)
		pc := getFewestOpporPhysicalCell(parentPhysical.GetChildren(), suggestedNodes)
		if pc == nil || pc.GetPriority() > opportunisticPriority {
			panic(fmt.Sprintf("VC Safety Broken: Cannot find physical cell for %v", c.GetAddress()))
		}
		c.SetPreBoundPhysicalCell(pc)
		pc.SetPreBoundVirtualCell(c)
		return pc
	}
}

// getFewestOpporPhysicalCell selects a physical cell with the minimum number of opportunistic pods from a cell list.
// This function will try to avoid using cells whose resources are not fully within the suggested nodes if possible.
// In case there is no cell fully within the suggested nodes, we will still return a cell because some of its children
// may be within suggested nodes, and we can find them when binding the lower-level cells. We will return a random
// cell in this case to avoid getting stuck in the same cell where we cannot find a child within suggested nodes.
func getFewestOpporPhysicalCell(cl CellList, suggestedNodes common.Set) *PhysicalCell {
	var cellsNotBound []*PhysicalCell
	fewestOpporNumSuggested := int32(math.MaxInt32)
	var fewestOpporCellSuggested *PhysicalCell
	for _, c := range cl {
		if pc := c.(*PhysicalCell); pc.GetVirtualCell() == nil && pc.GetPreBoundVirtualCell() == nil {
			cellsNotBound = append(cellsNotBound, pc)
			opporNum := pc.GetUsedGpuNumAtPriorities()[opportunisticPriority]
			allNodesInSuggested := true
			nodes, _ := pc.GetPhysicalPlacement()
			for _, n := range nodes {
				if !suggestedNodes.Contains(n) {
					allNodesInSuggested = false
					break
				}
			}
			if allNodesInSuggested && opporNum < fewestOpporNumSuggested {
				fewestOpporNumSuggested = opporNum
				fewestOpporCellSuggested = pc
			}
		}
	}
	if fewestOpporCellSuggested != nil {
		nodes, _ := fewestOpporCellSuggested.GetPhysicalPlacement()
		klog.Infof("Selected a cell within suggested nodes: %v, nodes %v",
			fewestOpporCellSuggested.GetAddress(), common.ToJson(nodes))
		return fewestOpporCellSuggested
	}
	// select a random cell to avoid always picking the same cell in which there might be
	// no child fully within suggested nodes
	selectedCell := cellsNotBound[rand.Int31n(int32(len(cellsNotBound)))]
	nodes, _ := selectedCell.GetPhysicalPlacement()
	klog.Infof("Selected a cell randomly from the cell list because we cannot find a cell fully within "+
		"suggested nodes (children of the cell may be within): %v, nodes %v",
		selectedCell.GetAddress(), common.ToJson(nodes))
	return selectedCell
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
		if preassignedVirtual := getLowestPriorityCell(vccl[preassignedLevel], p); preassignedVirtual == nil {
			return nil, fmt.Sprintf("insufficient free cell in the VC at the preassigned level (%v)", preassignedLevel)
		} else {
			return preassignedVirtual.(*VirtualCell), ""
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
			return getLowestPriorityCell(parentVirtual.GetChildren(), p).(*VirtualCell), ""
		}
	}
}

// getLowestPriorityCell returns a cell with the lowest priority among the cells
// whose priorities are lower than the given priority (p).
func getLowestPriorityCell(cl CellList, p CellPriority) Cell {
	lowestPriority := maxGuaranteedPriority
	var lowestPriorityCell Cell
	for _, c := range cl {
		pp := c.GetPriority()
		if pp == freePriority {
			return c
		} else if pp < p && pp < lowestPriority {
			lowestPriority = pp
			lowestPriorityCell = c
		}
	}
	return lowestPriorityCell
}

// bindCell binds a virtual cell to a physical cell and its parent recursively.
func bindCell(pc *PhysicalCell, vc *VirtualCell) {
	for vc.GetPhysicalCell() == nil {
		pc.SetVirtualCell(vc)
		vc.SetPhysicalCell(pc)
		klog.Infof("Virtual cell %v is bound to physical cell %v", vc.GetAddress(), pc.GetAddress())
		if pc.GetAPIStatus().CellHealthiness == api.CellBad &&
			(vc.GetParent() == nil ||
				pc.GetParent().(*PhysicalCell).GetAPIStatus().CellHealthiness == api.CellHealthy) {
			// If a physical cell is marked as Bad, that means all of its children are bad. In this case, we should also
			// mark all of the virtual cell's children as bad. We need to do it explicitly because some of the virtual
			// cell's children might be not bound to a physical cell, so it won't be marked as bad by cell binding.
			// Because cell binding is done in a bottom-up manner, if we set the virtual cell's healthiness whenever
			// the physical cell is bad, it would waste computation. So we set the virtual cell only when it is the root
			// or the parent is no longer bad.
			setVirtualCellHealthiness(vc, api.CellBad)
		}
		if vc.GetParent() == nil {
			break
		}
		vc = vc.GetParent().(*VirtualCell)
		pc = pc.GetParent().(*PhysicalCell)
	}
}

// unbindCell unbinds a virtual cell with a physical cell and its parent recursively.
func unbindCell(c *PhysicalCell) {
	boundVirtual := c.GetVirtualCell()
	for !boundVirtual.GetPhysicalCell().IsReserved() {
		boundPhysical := boundVirtual.GetPhysicalCell()
		klog.Infof("Virtual cell %v is unbound from physical cell %v",
			boundVirtual.GetAddress(), boundPhysical.GetAddress())
		boundVirtual.SetPhysicalCell(nil)
		boundPhysical.SetVirtualCell(nil)
		if boundVirtual.GetParent() == nil {
			break
		} else {
			unbindParent := true
			for _, cc := range boundVirtual.GetParent().GetChildren() {
				if child := cc.(*VirtualCell); child.GetPhysicalCell() != nil {
					unbindParent = false
					break
				}
			}
			if !unbindParent {
				break
			}
			boundVirtual = boundVirtual.GetParent().(*VirtualCell)
		}
	}
	if parent := boundVirtual.GetParent(); parent != nil {
		if parent.(*VirtualCell).GetAPIStatus().CellHealthiness == api.CellBad {
			// If the unbound virtual cell's parent is still marked as Bad, which means all children should be bad,
			// we will mark the unbound virtual cell and its children as bad
			setVirtualCellHealthiness(boundVirtual, api.CellBad)
		}
	} else {
		setVirtualCellHealthiness(boundVirtual, api.CellHealthy)
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
