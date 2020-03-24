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
		c := buddyAlloc(freeList.copy(), pac.GetLevel(), suggestedNodes)
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
// This function will try to avoid using cells whose resources are not within the suggested nodes if possible.
// In case there is no cell within the suggested nodes, it will try to randomly return a healthy node with at least
// one opportunistic pod to try preemption. If that also fails, it just returns the min-opportunistic cell,
// regardless of if it is suggested or healthy.
func getFewestOpporPhysicalCell(cl CellList, suggestedNodes common.Set) *PhysicalCell {
	fewestOpporNum := int32(math.MaxInt32)
	fewestOpporNumSuggested := int32(math.MaxInt32)
	var fewestOpporCell *PhysicalCell
	var fewestOpporCellSuggested *PhysicalCell
	var preemptibleCells []*PhysicalCell
	for _, c := range cl {
		if pc := c.(*PhysicalCell); pc.GetVirtualCell() == nil && pc.GetPreBoundVirtualCell() == nil {
			opporNum := pc.GetUsedGpuNumAtPriorities()[opportunisticPriority]
			if opporNum < fewestOpporNum {
				fewestOpporNum = opporNum
				fewestOpporCell = pc
			}
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
			if opporNum > 0 {
				preemptibleCells = append(preemptibleCells, pc)
			}
		}
	}
	var selectedCell *PhysicalCell
	if fewestOpporCellSuggested != nil {
		selectedCell = fewestOpporCellSuggested
		nodes, _ := selectedCell.GetPhysicalPlacement()
		klog.Infof("Selected a cell within suggested nodes: %v, nodes %v", selectedCell.GetAddress(), common.ToJson(nodes))
	} else if len(preemptibleCells) > 0 {
		// If we cannot find a cell within suggested nodes, we will try to preempt some pods instead of
		// directly returning the fewestOpporCell (because this cell could be a bad node, we should not return it).
		// Also, we will choose a random cell, to avoid always returning the same cell (similar to above,
		// if we always return the same cell, it might be a bad node, preempting pods on a bad node won't bring
		// it back to the suggested nodes)
		selectedCell = preemptibleCells[rand.Int31n(int32(len(preemptibleCells)))]
		nodes, _ := selectedCell.GetPhysicalPlacement()
		klog.Infof("Selected a cell not within suggested nodes but preempting opportunistic pods may help: %v, nodes %v",
			selectedCell.GetAddress(), common.ToJson(nodes))
	} else if fewestOpporCell == nil {
		panic("VC Safety Broken: Cannot find any physical cell that has not been bound to a virtual cell")
	} else {
		selectedCell = fewestOpporCell
		nodes, _ := selectedCell.GetPhysicalPlacement()
		klog.Infof("Selected a cell not within suggested nodes and no preemption can help: %v, nodes %v",
			selectedCell.GetAddress(), common.ToJson(nodes))
	}
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
		message := fmt.Sprintf("Cells bound: %v and %v", vc.GetAddress(), pc.GetAddress())
		if pc.IsReserved() {
			message += " (reservation)"
		}
		klog.Info(message)
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
		klog.Infof("Cells unbound: %v and %v", boundVirtual.GetAddress(), boundPhysical.GetAddress())
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
}

// setPriority sets priority and state for a cell and its parent recursively, guaranteeing that
// (i) the priority of a cell is the max of those of its children.
// (ii) a cell is in "Used" state if any of its children is "Used", otherwise "Free".
func setPriority(c Cell, p CellPriority) {
	originalPriority := c.GetPriority()
	c.SetPriority(p)
	if parent := c.GetParent(); parent != nil {
		if p > parent.GetPriority() {
			setPriority(parent, p)
		} else if originalPriority == parent.GetPriority() && p < originalPriority {
			maxBuddyPriority := freePriority
			for _, buddy := range parent.GetChildren() {
				if buddy.GetPriority() > maxBuddyPriority {
					maxBuddyPriority = buddy.GetPriority()
				}
			}
			setPriority(parent, maxBuddyPriority)
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
