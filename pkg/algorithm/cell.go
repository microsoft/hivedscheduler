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
	"k8s.io/klog"
)

// A Cell represents a set of GPUs affinitized by their interconnection topology.
// Cells are organized as a tree through pointers to their parents / children.
type Cell interface {
	GetChain() CellChain
	GetLevel() CellLevel
	GetAddress() api.CellAddress
	GetParent() Cell
	SetParent(Cell)
	GetChildren() CellList
	SetChildren(CellList)
	AtOrHigherThanNode() bool
	GetPriority() CellPriority
	SetPriority(CellPriority)
	GetTotalGpuNum() int32
	GetUsedGpuNumAtPriorities() map[CellPriority]int32
	IncreaseUsedGpuNumAtPriority(CellPriority, int32)
}

func CellEqual(c1 Cell, c2 Cell) bool {
	if c1 == nil || c2 == nil {
		return c1 == nil && c2 == nil
	} else {
		return c1.GetAddress() == c2.GetAddress()
	}
}

type GenericCell struct {
	chain                  CellChain
	level                  CellLevel
	address                api.CellAddress
	parent                 Cell     // pointer to its parent cell
	children               CellList // pointer to its children cells
	atOrHigherThanNode     bool     // true if the cell is at or higher than node level
	priority               CellPriority
	state                  CellState
	totalGpuNum            int32                  // total GPU number of a cell
	usedGpuNumAtPriorities map[CellPriority]int32 // GPU number used by each priority
}

func (c *GenericCell) GetChain() CellChain {
	return c.chain
}

func (c *GenericCell) GetLevel() CellLevel {
	return c.level
}

func (c *GenericCell) GetAddress() api.CellAddress {
	return c.address
}

func (c *GenericCell) GetParent() Cell {
	return c.parent
}

func (c *GenericCell) SetParent(p Cell) {
	c.parent = p
}

func (c *GenericCell) GetChildren() CellList {
	return c.children
}

func (c *GenericCell) AtOrHigherThanNode() bool {
	return c.atOrHigherThanNode
}

func (c *GenericCell) GetPriority() CellPriority {
	return c.priority
}

func (c *GenericCell) GetState() CellState {
	return c.state
}

func (c *GenericCell) GetTotalGpuNum() int32 {
	return c.totalGpuNum
}

func (c *GenericCell) GetUsedGpuNumAtPriorities() map[CellPriority]int32 {
	return c.usedGpuNumAtPriorities
}

func (c *GenericCell) IncreaseUsedGpuNumAtPriority(p CellPriority, delta int32) {
	c.usedGpuNumAtPriorities[p] += delta
	if c.usedGpuNumAtPriorities[p] == 0 {
		delete(c.usedGpuNumAtPriorities, p)
	}
}

// PhysicalCell defines a cell in the physical cluster.
type PhysicalCell struct {
	GenericCell
	nodes               []string           // node names inside the cell
	gpuIndices          []int32            // [-1] for cells at levels higher than node
	usingGroup          *AlgoAffinityGroup // affinity group using this cell (i.e., has running pod on the cell)
	acquiringGroup      *AlgoAffinityGroup // affinity group that is trying to acquire, or has acquired the cell (e.g., waiting for preemption)
	virtualCell         *VirtualCell       // points to the bound virtual cell
	preBoundVirtualCell *VirtualCell       // points to the temporarily bound virtual cell (before the binding is confirmed)
	split               bool               // true when the cell has been split
	reserved            bool               // true when this is a reserved cell
	// This status only contains the statuses that need to be exposed to external,
	// and should not be used for internal status management
	apiStatus *api.PhysicalCellStatus
}

func NewPhysicalCell(c CellChain, l CellLevel, g bool, n int32, cellType api.CellType, address api.CellAddress) *PhysicalCell {
	return &PhysicalCell{
		GenericCell: GenericCell{
			chain:                  c,
			level:                  l,
			priority:               freePriority,
			address:                address,
			atOrHigherThanNode:     g,
			totalGpuNum:            n,
			usedGpuNumAtPriorities: map[CellPriority]int32{},
			state:                  cellFree,
		},
		apiStatus: &api.PhysicalCellStatus{
			CellStatus: api.CellStatus{
				CellType:        cellType,
				CellAddress:     address,
				CellState:       api.CellFree,
				CellHealthiness: api.CellHealthy,
				CellPriority:    int32(freePriority),
			},
		},
	}
}

func (c *PhysicalCell) SetChildren(children CellList) {
	c.children = children
	for _, cc := range children {
		child := cc.(*PhysicalCell)
		c.apiStatus.CellChildren = append(c.apiStatus.CellChildren, child.apiStatus)
	}
}

func (c *PhysicalCell) SetPriority(p CellPriority) {
	c.priority = p
	c.apiStatus.CellPriority = int32(p)
	if c.apiStatus.VirtualCell != nil {
		c.apiStatus.VirtualCell.CellPriority = int32(p)
	}
}

func (c *PhysicalCell) SetState(s CellState) {
	c.state = s
	c.apiStatus.CellState = api.CellState(s)
	if c.virtualCell != nil {
		c.virtualCell.state = s
		c.virtualCell.apiStatus.CellState = api.CellState(s)
		c.apiStatus.VirtualCell.CellState = api.CellState(s)
		c.virtualCell.apiStatus.PhysicalCell.CellState = api.CellState(s)
	}
}

func (c *PhysicalCell) GetPhysicalPlacement() ([]string, []int32) {
	return c.nodes, c.gpuIndices
}

func (c *PhysicalCell) GetPhysicalPlacementString() string {
	return fmt.Sprintf("%v:%v", c.nodes, c.gpuIndices)
}

func (c *PhysicalCell) SetPhysicalResources(nodes []string, gpuIndices []int32) {
	c.nodes = nodes
	c.gpuIndices = gpuIndices
}

func (c *PhysicalCell) AddUsingGroup(g *AlgoAffinityGroup) {
	if c.usingGroup != nil {
		klog.Errorf("Error when adding using affinity group %v to cell %v: already another using group %v",
			g.name, c.address, c.usingGroup.name)
	}
	c.usingGroup = g
	klog.Infof("Cell %v is now used by affinity group %v", c.address, g.name)
	c.SetState(cellUsed)
}

func (c *PhysicalCell) DeleteUsingGroup(g *AlgoAffinityGroup) {
	if c.usingGroup == nil || c.usingGroup.name != g.name {
		klog.Errorf("Error when deleting affinity group %v from cell %v: not found", g.name, c.address)
	}
	c.usingGroup = nil
	klog.Infof("Cell %v is no longer used by affinity group %v", c.address, g.name)
	if c.acquiringGroup != nil {
		c.SetState(cellAcquired)
	} else {
		c.SetState(cellFree)
	}
}

func (c *PhysicalCell) GetUsingGroup() *AlgoAffinityGroup {
	return c.usingGroup
}

func (c *PhysicalCell) AddAcquiringGroup(g *AlgoAffinityGroup) {
	if c.acquiringGroup != nil {
		klog.Errorf("Error when adding acquiring affinity group %v to cell %v: already another acquiring group %v",
			g.name, c.address, c.acquiringGroup.name)
	}
	c.acquiringGroup = g
	klog.Infof("Cell %v is now being acquired by affinity group %v", c.address, g.name)
	if c.usingGroup != nil {
		c.SetState(cellAcquiring)
	} else {
		c.SetState(cellAcquired)
	}
}

func (c *PhysicalCell) DeleteAcquiringGroup(g *AlgoAffinityGroup) {
	if c.acquiringGroup == nil || c.acquiringGroup.name != g.name {
		klog.Errorf("Error when deleting acquiring affinity group %v from cell %v: not found", g.name, c.address)
	}
	c.acquiringGroup = nil
	klog.Infof("Cell %v is no longer acquired by affinity group %v", c.address, g.name)
	if c.usingGroup != nil {
		c.SetState(cellUsed)
	} else {
		c.SetState(cellFree)
	}
}

func (c *PhysicalCell) GetAcquiringGroup() *AlgoAffinityGroup {
	return c.acquiringGroup
}

func (c *PhysicalCell) GetVirtualCell() *VirtualCell {
	return c.virtualCell
}

func (c *PhysicalCell) SetVirtualCell(cell *VirtualCell) {
	c.virtualCell = cell
	if cell == nil {
		c.apiStatus.VirtualCell = nil
		c.apiStatus.VC = ""
	} else {
		vcs := &api.VirtualCellStatus{}
		// shallow copy the status, clear the pointers to avoid reference
		*vcs = *(cell.apiStatus)
		vcs.Children = nil
		vcs.PhysicalCell = nil
		c.apiStatus.VirtualCell = vcs
		c.apiStatus.VC = cell.vc
	}
}

func (c *PhysicalCell) GetPreBoundVirtualCell() *VirtualCell {
	return c.preBoundVirtualCell
}

func (c *PhysicalCell) SetPreBoundVirtualCell(cell *VirtualCell) {
	c.preBoundVirtualCell = cell
}

func (c *PhysicalCell) IsSplit() bool {
	return c.split
}

func (c *PhysicalCell) SetSplit(split bool) {
	c.split = split
}

func (c *PhysicalCell) IsReserved() bool {
	return c.reserved
}

func (c *PhysicalCell) SetReserved(reserved bool) {
	c.reserved = reserved
}

func (c *PhysicalCell) GetAPIStatus() *api.PhysicalCellStatus {
	return c.apiStatus
}

// VirtualCell defines a cell in a VC.
type VirtualCell struct {
	GenericCell
	vc                   api.VirtualClusterName // name of its VC
	rid                  api.ReservationId      // reservation ID
	preAssignedCell      *VirtualCell           // top level cell of this cell chain
	physicalCell         *PhysicalCell          // points to the bound physical cell
	preBoundPhysicalCell *PhysicalCell          // points to the temporarily bound physical cell (before the binding is confirmed)
	// This status only contains the statuses that need to be exposed to external,
	// and should not be used for internal status management
	apiStatus *api.VirtualCellStatus
}

func NewVirtualCell(
	vcn api.VirtualClusterName,
	c CellChain,
	l CellLevel,
	g bool,
	n int32,
	pac *VirtualCell,
	cellType api.CellType,
	address api.CellAddress) *VirtualCell {

	return &VirtualCell{
		GenericCell: GenericCell{
			chain:                  c,
			level:                  l,
			priority:               freePriority,
			address:                address,
			atOrHigherThanNode:     g,
			totalGpuNum:            n,
			usedGpuNumAtPriorities: map[CellPriority]int32{},
			state:                  cellFree,
		},
		vc:              vcn,
		preAssignedCell: pac,
		apiStatus: &api.VirtualCellStatus{
			CellStatus: api.CellStatus{
				CellType:        cellType,
				CellAddress:     address,
				CellState:       api.CellFree,
				CellHealthiness: api.CellHealthy,
				CellPriority:    int32(freePriority),
			},
		},
	}
}

func (c *VirtualCell) SetChildren(children CellList) {
	c.children = children
	for _, cc := range children {
		child := cc.(*VirtualCell)
		c.apiStatus.Children = append(c.apiStatus.Children, child.apiStatus)
	}
}

func (c *VirtualCell) SetPriority(p CellPriority) {
	c.priority = p
	c.apiStatus.CellPriority = int32(p)
	if c.apiStatus.PhysicalCell != nil {
		c.apiStatus.PhysicalCell.CellPriority = int32(p)
	}
}

func (c *VirtualCell) SetReservation(rid api.ReservationId) {
	c.rid = rid
}

func (c *VirtualCell) GetPreAssignedCell() *VirtualCell {
	return c.preAssignedCell
}

func (c *VirtualCell) SetPreAssignedCell(cell *VirtualCell) {
	c.preAssignedCell = cell
}

func (c *VirtualCell) GetPhysicalCell() *PhysicalCell {
	return c.physicalCell
}

func (c *VirtualCell) SetPhysicalCell(cell *PhysicalCell) {
	c.physicalCell = cell
	if cell == nil {
		c.apiStatus.PhysicalCell = nil
		c.apiStatus.CellHealthiness = api.CellHealthy
	} else {
		pcs := &api.PhysicalCellStatus{}
		// shallow copy the status, clear the pointers to avoid reference
		*pcs = *(cell.apiStatus)
		pcs.CellChildren = nil
		pcs.VirtualCell = nil
		c.apiStatus.PhysicalCell = pcs
		c.apiStatus.CellHealthiness = pcs.CellHealthiness
	}
}

func (c *VirtualCell) GetPreBoundPhysicalCell() *PhysicalCell {
	return c.preBoundPhysicalCell
}

func (c *VirtualCell) SetPreBoundPhysicalCell(cell *PhysicalCell) {
	c.preBoundPhysicalCell = cell
}

func (c *VirtualCell) GetAPIStatus() *api.VirtualCellStatus {
	return c.apiStatus
}
