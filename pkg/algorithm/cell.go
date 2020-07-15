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
	GetTotalSkuNum() int32
	GetUsedSkuNumAtPriorities() map[CellPriority]int32
	IncreaseUsedSkuNumAtPriority(CellPriority, int32)
}

func CellEqual(c1 Cell, c2 Cell) bool {
	if c1 == nil || c2 == nil {
		return c1 == nil && c2 == nil
	} else {
		return c1.GetAddress() == c2.GetAddress()
	}
}

type GenericCell struct {
	chain              CellChain
	level              CellLevel
	address            api.CellAddress
	parent             Cell     // pointer to its parent cell
	children           CellList // pointer to its children cells
	atOrHigherThanNode bool     // true if the cell is at or higher than node level
	priority           CellPriority
	state              CellState
	// A cell is healthy if all of the cell's children are healthy (bad if any child is bad).
	// The healthy field is orthogonal to priority and state.
	healthy                bool
	totalSkuNum            int32                  // total SKU number of a cell
	usedSkuNumAtPriorities map[CellPriority]int32 // SKU number used by each priority
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

func (c *GenericCell) IsHealthy() bool {
	return c.healthy
}

func (c *GenericCell) GetTotalSkuNum() int32 {
	return c.totalSkuNum
}

func (c *GenericCell) GetUsedSkuNumAtPriorities() map[CellPriority]int32 {
	return c.usedSkuNumAtPriorities
}

func (c *GenericCell) IncreaseUsedSkuNumAtPriority(p CellPriority, delta int32) {
	c.usedSkuNumAtPriorities[p] += delta
	if c.usedSkuNumAtPriorities[p] == 0 {
		delete(c.usedSkuNumAtPriorities, p)
	}
}

// PhysicalCell defines a cell in the physical cluster.
type PhysicalCell struct {
	GenericCell
	nodes                    []string           // node names inside the cell
	gpuIndices               []int32            // [-1] for cells at levels higher than node
	usingGroup               *AlgoAffinityGroup // affinity group using this cell
	reservingOrReservedGroup *AlgoAffinityGroup // affinity group that is reserving, or has reserved the cell (e.g., waiting for preemption)
	virtualCell              *VirtualCell       // points to the bound virtual cell
	split                    bool               // true when the cell has been split
	pinned                   bool               // true when this is a pinned cell
	// This status only contains the statuses that need to be exposed to external,
	// and should not be used for internal status management
	apiStatus *api.PhysicalCellStatus
}

func NewPhysicalCell(
	c CellChain,
	l CellLevel,
	g bool,
	n int32,
	cellType api.CellType,
	address api.CellAddress,
	isNodeLevel bool) *PhysicalCell {

	return &PhysicalCell{
		GenericCell: GenericCell{
			chain:                  c,
			level:                  l,
			priority:               freePriority,
			address:                address,
			atOrHigherThanNode:     g,
			totalSkuNum:            n,
			usedSkuNumAtPriorities: map[CellPriority]int32{},
			state:                  cellFree,
			// cells are set to healthy initially, and will be all set to bad in HivedAlgorithm.initBadNodes
			healthy: true,
		},
		apiStatus: &api.PhysicalCellStatus{
			CellStatus: api.CellStatus{
				CellType:        cellType,
				IsNodeLevel:     isNodeLevel,
				CellAddress:     address,
				CellState:       api.CellState(cellFree),
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
		klog.Errorf("Found another using affinity group %v when adding "+
			"using affinity group %v to cell %v", c.usingGroup.name, g.name, c.address)
	}
	c.usingGroup = g
	klog.Infof("Cell %v is now used by affinity group %v", c.address, g.name)
}

func (c *PhysicalCell) DeleteUsingGroup(g *AlgoAffinityGroup) {
	if c.usingGroup == nil || c.usingGroup.name != g.name {
		klog.Errorf("Using affinity group %v not found when deleting it from cell %v", g.name, c.address)
	}
	c.usingGroup = nil
	klog.Infof("Cell %v is no longer used by affinity group %v", c.address, g.name)
}

func (c *PhysicalCell) GetUsingGroup() *AlgoAffinityGroup {
	return c.usingGroup
}

func (c *PhysicalCell) AddReservingOrReservedGroup(g *AlgoAffinityGroup) {
	if c.reservingOrReservedGroup != nil {
		klog.Errorf("Found another reserving or reserved affinity group %v when adding "+
			"reserving or reserved affinity group %v to cell %v", c.reservingOrReservedGroup.name, g.name, c.address)
	}
	c.reservingOrReservedGroup = g
	klog.Infof("Cell %v is now reserved (or being reserved) by affinity group %v", c.address, g.name)
}

func (c *PhysicalCell) DeleteReservingOrReservedGroup(g *AlgoAffinityGroup) {
	if c.reservingOrReservedGroup == nil || c.reservingOrReservedGroup.name != g.name {
		klog.Errorf("Reserving or reserved affinity group %v not found when deleting it from cell %v",
			g.name, c.address)
	}
	c.reservingOrReservedGroup = nil
	klog.Infof("Cell %v is no longer reserved by affinity group %v", c.address, g.name)
}

func (c *PhysicalCell) GetReservingOrReservedGroup() *AlgoAffinityGroup {
	return c.reservingOrReservedGroup
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
		vcs.CellChildren = nil
		vcs.PhysicalCell = nil
		c.apiStatus.VirtualCell = vcs
		c.apiStatus.VC = cell.vc
	}
}

func (c *PhysicalCell) IsSplit() bool {
	return c.split
}

func (c *PhysicalCell) SetSplit(split bool) {
	c.split = split
}

func (c *PhysicalCell) IsPinned() bool {
	return c.pinned
}

func (c *PhysicalCell) SetPinned(pinned bool) {
	c.pinned = pinned
}

func (c *PhysicalCell) GetAPIStatus() *api.PhysicalCellStatus {
	return c.apiStatus
}

func (c *PhysicalCell) SetHealthiness(h api.CellHealthiness) {
	klog.Infof("Cell %v is set to %v", c.address, h)
	c.healthy = h == api.CellHealthy
	c.apiStatus.CellHealthiness = h
	if c.virtualCell != nil {
		c.virtualCell.healthy = c.healthy
		c.apiStatus.VirtualCell.CellHealthiness = h
		c.virtualCell.GetAPIStatus().CellHealthiness = h
		c.virtualCell.GetAPIStatus().PhysicalCell.CellHealthiness = h
	}
}

// VirtualCell defines a cell in a VC.
type VirtualCell struct {
	GenericCell
	vc              api.VirtualClusterName // name of its VC
	pid             api.PinnedCellId       // pinned cell ID
	preassignedCell *VirtualCell           // top-level ancestor of this cell
	physicalCell    *PhysicalCell          // points to the bound physical cell
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
	address api.CellAddress,
	isNodeLevel bool) *VirtualCell {

	return &VirtualCell{
		GenericCell: GenericCell{
			chain:                  c,
			level:                  l,
			priority:               freePriority,
			address:                address,
			atOrHigherThanNode:     g,
			totalSkuNum:            n,
			usedSkuNumAtPriorities: map[CellPriority]int32{},
			state:                  cellFree,
			// cells are set to healthy initially, and will be all set to bad in HivedAlgorithm.initBadNodes
			healthy: true,
		},
		vc:              vcn,
		preassignedCell: pac,
		apiStatus: &api.VirtualCellStatus{
			CellStatus: api.CellStatus{
				CellType:        cellType,
				IsNodeLevel:     isNodeLevel,
				CellAddress:     address,
				CellState:       api.CellState(cellFree),
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
		c.apiStatus.CellChildren = append(c.apiStatus.CellChildren, child.apiStatus)
	}
}

func (c *VirtualCell) SetPriority(p CellPriority) {
	c.priority = p
	c.apiStatus.CellPriority = int32(p)
	if c.apiStatus.PhysicalCell != nil {
		c.apiStatus.PhysicalCell.CellPriority = int32(p)
	}
}

func (c *VirtualCell) GetVirtualCluster() api.VirtualClusterName {
	return c.vc
}

func (c *VirtualCell) SetPinnedCellId(pid api.PinnedCellId) {
	c.pid = pid
}

func (c *VirtualCell) GetPreassignedCell() *VirtualCell {
	return c.preassignedCell
}

func (c *VirtualCell) SetPreAssignedCell(cell *VirtualCell) {
	c.preassignedCell = cell
}

func (c *VirtualCell) GetPhysicalCell() *PhysicalCell {
	return c.physicalCell
}

func (c *VirtualCell) SetPhysicalCell(cell *PhysicalCell) {
	c.physicalCell = cell
	if cell == nil {
		c.apiStatus.PhysicalCell = nil
		c.state = cellFree
		c.healthy = true
		c.apiStatus.CellHealthiness = api.CellHealthy
		c.apiStatus.CellState = api.CellState(cellFree)
	} else {
		c.healthy = cell.healthy
		pcs := &api.PhysicalCellStatus{}
		// shallow copy the status, clear the pointers to avoid reference
		*pcs = *(cell.apiStatus)
		pcs.CellChildren = nil
		pcs.VirtualCell = nil
		c.apiStatus.PhysicalCell = pcs
		c.apiStatus.CellHealthiness = pcs.CellHealthiness
	}
}

func (c *VirtualCell) GetAPIStatus() *api.VirtualCellStatus {
	return c.apiStatus
}
