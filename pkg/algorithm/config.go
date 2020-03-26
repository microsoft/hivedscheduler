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
	"strings"
)

// internal wrapper for spec cellTypes
type cellChainElement struct {
	cellType      api.CellType // current cell type
	level         CellLevel    // current cell level, leaf cell is 1
	childCellType api.CellType // child cell type
	childNumber   int32        // child number
	hasNode       bool         // current cell type is a node or above cell
	isMultiNodes  bool         // current cell type is a multiple node cell
	gpuType       string       // current cell gpu type
	gpuNumber     int32        // how many gpu in current cell
}

type cellTypeConstructor struct {
	// input: raw spec from config
	cellTypeSpecs map[api.CellType]api.CellTypeSpec
	// output: converted wrapper
	cellChainElements map[api.CellType]*cellChainElement
}

func newCellTypeConstructor(cellTypes map[api.CellType]api.CellTypeSpec) *cellTypeConstructor {
	return &cellTypeConstructor{
		cellTypeSpecs:     cellTypes,
		cellChainElements: map[api.CellType]*cellChainElement{},
	}
}

func (c *cellTypeConstructor) addCellChain(ct api.CellType) {
	_, ok := c.cellChainElements[ct]
	if ok {
		// already added
		return
	}

	ctSpec, ok := c.cellTypeSpecs[ct]
	if !ok {
		// not found in raw spec, it's leaf cell
		c.cellChainElements[ct] = &cellChainElement{
			cellType:      ct,
			level:         lowestLevel,
			childCellType: "",
			childNumber:   0,
			hasNode:       false,
			isMultiNodes:  false,
			gpuType:       string(ct),
			gpuNumber:     1,
		}
		return
	}

	// recursively add children
	child := ctSpec.ChildCellType
	if _, ok := c.cellChainElements[child]; !ok {
		c.addCellChain(child)
	}

	// child cell type has been added, added current element,
	cct := c.cellChainElements[child]
	c.cellChainElements[ct] = &cellChainElement{
		cellType:      ct,
		level:         cct.level + 1,
		childCellType: cct.cellType,
		childNumber:   ctSpec.ChildCellNumber,
		hasNode:       cct.hasNode || ctSpec.IsNodeLevel,
		isMultiNodes:  cct.hasNode,
		gpuType:       cct.gpuType,
		gpuNumber:     cct.gpuNumber * ctSpec.ChildCellNumber,
	}
	return
}

func (c *cellTypeConstructor) buildCellChains() map[api.CellType]*cellChainElement {
	for p := range c.cellTypeSpecs {
		c.addCellChain(p)
	}
	return c.cellChainElements
}

type physicalCellConstructor struct {
	// input
	cellChainElements map[api.CellType]*cellChainElement
	cellChainSpecs    []api.PhysicalCellSpec
	// output
	fullCellList  map[CellChain]ChainCellList
	freeCellList  map[CellChain]ChainCellList
	reservedCells map[api.ReservationId]*PhysicalCell
	// internal status
	buildingChain CellChain            // current build chain, it the top cell type in physicalCells
	buildingSpec  api.PhysicalCellSpec // current building spec instance
}

func newPhysicalCellConstructor(
	cellChainElements map[api.CellType]*cellChainElement,
	cellChainSpecs []api.PhysicalCellSpec) *physicalCellConstructor {

	return &physicalCellConstructor{
		cellChainElements: cellChainElements,
		cellChainSpecs:    cellChainSpecs,
		fullCellList:      map[CellChain]ChainCellList{},
		freeCellList:      map[CellChain]ChainCellList{},
		reservedCells:     map[api.ReservationId]*PhysicalCell{},
	}
}

func (c *physicalCellConstructor) updateInternalStatus(buildingChain CellChain, buildingSpec api.PhysicalCellSpec) {
	c.buildingChain = buildingChain
	c.buildingSpec = buildingSpec
}

func (c *physicalCellConstructor) buildChildCell(
	spec api.PhysicalCellSpec,
	ct api.CellType,
	currentNode string) *PhysicalCell {

	ce := c.cellChainElements[ct]
	splitAddress := strings.Split(string(spec.CellAddress), "/")
	if ce.hasNode && !ce.isMultiNodes {
		// node-level cell pass address to children as node
		currentNode = splitAddress[len(splitAddress)-1]
	}
	cellInstance := c.addCell(c.buildingChain, ce, spec.ReservationId, spec.CellAddress)
	if ce.level == 1 {
		cellInstance.SetPhysicalResources(
			[]string{currentNode}, []int32{common.StringToInt32(splitAddress[len(splitAddress)-1])})
		return cellInstance
	}
	var currentCellNodes []string
	var currentCellGpuIndices []int32
	var currentCellChildren CellList
	for _, childSpec := range spec.CellChildren {
		childCellInstance := c.buildChildCell(childSpec, ce.childCellType, currentNode)
		childCellInstance.SetParent(cellInstance)
		currentCellChildren = append(currentCellChildren, childCellInstance)
		if ce.isMultiNodes {
			// super-node cell merge child nodes
			currentCellNodes = append(currentCellNodes, childCellInstance.nodes...)
		} else {
			// sub-node cell merge child node gpu indices
			currentCellGpuIndices = append(currentCellGpuIndices, childCellInstance.gpuIndices...)
		}
	}
	// update current cell children and resource
	cellInstance.SetChildren(currentCellChildren)
	if ce.isMultiNodes {
		currentCellGpuIndices = []int32{-1}
	} else {
		currentCellNodes = []string{string(currentNode)}
	}
	cellInstance.SetPhysicalResources(currentCellNodes, currentCellGpuIndices)

	return cellInstance
}

func (c *physicalCellConstructor) addCell(
	chain CellChain,
	ce *cellChainElement,
	reservationId api.ReservationId,
	address api.CellAddress) *PhysicalCell {

	cellInstance := NewPhysicalCell(
		c.buildingChain, ce.level, ce.hasNode, ce.gpuNumber, ce.cellType, address, ce.hasNode && !ce.isMultiNodes)
	if _, ok := c.fullCellList[chain]; !ok {
		c.fullCellList[chain] = ChainCellList{}
	}
	c.fullCellList[chain][ce.level] = append(c.fullCellList[chain][ce.level], cellInstance)
	// record and mark reserved cell
	if reservationId != "" {
		c.reservedCells[reservationId] = cellInstance
		cellInstance.SetReserved(true)
	}
	return cellInstance
}

func (c *physicalCellConstructor) buildFullTree() *PhysicalCell {
	cc := c.buildingChain
	ce, ok := c.cellChainElements[api.CellType(cc)]
	if !ok {
		panic(fmt.Sprintf("cellType %v in PhysicalCells is not found in cell types definition", cc))
	}
	if !ce.hasNode {
		panic(fmt.Sprintf("top cell must be node-level or above: %v", cc))
	}
	cellInstance := c.buildChildCell(c.buildingSpec, api.CellType(cc), "")
	// set GPU type only for top-level cells (as a chain shares the same GPU type)
	cellInstance.GetAPIStatus().GpuType = ce.gpuType
	return cellInstance
}

func (c *physicalCellConstructor) build() (
	map[CellChain]ChainCellList,
	map[CellChain]ChainCellList,
	map[api.ReservationId]*PhysicalCell) {

	for _, spec := range c.cellChainSpecs {
		c.updateInternalStatus(CellChain(spec.CellType), spec)
		rootCell := c.buildFullTree()
		if _, ok := c.freeCellList[rootCell.chain]; !ok {
			c.freeCellList[rootCell.chain] = NewChainCellList(rootCell.level)
		}
		c.freeCellList[rootCell.chain][rootCell.level] = append(
			c.freeCellList[rootCell.chain][rootCell.level], rootCell)
	}
	return c.fullCellList, c.freeCellList, c.reservedCells
}

type virtualCellConstructor struct {
	// input
	cellChainElements        map[api.CellType]*cellChainElement
	specs                    map[api.VirtualClusterName]api.VirtualClusterSpec
	rawReservedPhysicalCells map[api.ReservationId]*PhysicalCell // rId:physicalCell
	// output
	vcFreeCellNum         map[api.VirtualClusterName]map[CellChain]map[CellLevel]int32   // vc:cellChain:cellLevel:numCells
	nonReservedFullList   map[api.VirtualClusterName]map[CellChain]ChainCellList         // vc:cellChain:cellLevel:virtualCells
	nonReservedFreeList   map[api.VirtualClusterName]map[CellChain]ChainCellList         // vc:cellChain:cellLevel:virtualCells
	reservedCellList      map[api.VirtualClusterName]map[api.ReservationId]ChainCellList // vc:rId:cellLevel:virtualCells
	reservedPhysicalCells map[api.VirtualClusterName]map[api.ReservationId]*PhysicalCell // vc:rId:physicalCell
	// internal status
	buildingVc          api.VirtualClusterName // current building vc
	buildingChain       CellChain              // current building chain, it's a in a.b.c
	buildingChild       api.CellType           // current building child, it's c in a.b.c
	buildingRoot        *VirtualCell           // current building root cell, it's instance of c in a.b.c
	buildingReservation api.ReservationId      // current building is a reservation
}

func newVirtualCellConstructor(
	cellChains map[api.CellType]*cellChainElement,
	specs map[api.VirtualClusterName]api.VirtualClusterSpec,
	reservedCells map[api.ReservationId]*PhysicalCell) *virtualCellConstructor {

	return &virtualCellConstructor{
		cellChainElements:        cellChains,
		specs:                    specs,
		rawReservedPhysicalCells: reservedCells,
		vcFreeCellNum:            map[api.VirtualClusterName]map[CellChain]map[CellLevel]int32{},
		nonReservedFullList:      map[api.VirtualClusterName]map[CellChain]ChainCellList{},
		nonReservedFreeList:      map[api.VirtualClusterName]map[CellChain]ChainCellList{},
		reservedCellList:         map[api.VirtualClusterName]map[api.ReservationId]ChainCellList{},
		reservedPhysicalCells:    map[api.VirtualClusterName]map[api.ReservationId]*PhysicalCell{},
	}
}

func (c *virtualCellConstructor) updateInternalStatus(buildingVc api.VirtualClusterName, buildingChain CellChain,
	buildingChild api.CellType, buildingRoot *VirtualCell, buildingReservation api.ReservationId) {
	c.buildingVc = buildingVc
	c.buildingChain = buildingChain
	c.buildingChild = buildingChild
	c.buildingRoot = buildingRoot
	c.buildingReservation = buildingReservation
}

func (c *virtualCellConstructor) addCell(
	chain CellChain,
	vc api.VirtualClusterName,
	ce *cellChainElement,
	address api.CellAddress) *VirtualCell {

	cellInstance := NewVirtualCell(
		vc, c.buildingChain, ce.level, ce.hasNode, ce.gpuNumber, nil, ce.cellType, address, ce.hasNode && !ce.isMultiNodes)
	if c.buildingReservation == "" {
		if _, ok := c.nonReservedFullList[vc][chain]; !ok {
			c.nonReservedFullList[vc][chain] = ChainCellList{}
		}
		c.nonReservedFullList[vc][chain][ce.level] = append(
			c.nonReservedFullList[vc][chain][ce.level], cellInstance)
	} else {
		rId := c.buildingReservation
		if _, ok := c.reservedCellList[vc][rId]; !ok {
			c.reservedCellList[vc][rId] = ChainCellList{}
		}
		c.reservedCellList[vc][rId][ce.level] = append(
			c.reservedCellList[vc][rId][ce.level], cellInstance)
	}
	if c.buildingRoot == nil {
		c.buildingRoot = cellInstance
	}
	cellInstance.SetPreAssignedCell(c.buildingRoot)
	return cellInstance
}

func (c *virtualCellConstructor) buildChildCell(ct api.CellType, address api.CellAddress) *VirtualCell {
	ce := c.cellChainElements[ct]
	cellInstance := c.addCell(c.buildingChain, c.buildingVc, ce, address)
	if ce.level == 1 {
		return cellInstance
	}
	var currentCellChildren CellList
	splitAddress := strings.Split(string(address), "/")
	var offset int32
	if len(splitAddress) == 2 {
		offset = 0 // offset starts from 0 for each preassigned cell in a VC
	} else {
		offset = common.StringToInt32(splitAddress[len(splitAddress)-1]) * ce.childNumber
	}
	for i := int32(0); i < ce.childNumber; i++ {
		childCellInstance := c.buildChildCell(ce.childCellType,
			api.CellAddress(fmt.Sprintf("%v/%v", address, offset+i)))
		childCellInstance.SetParent(cellInstance)
		currentCellChildren = append(currentCellChildren, childCellInstance)
	}
	cellInstance.SetChildren(currentCellChildren)
	return cellInstance
}

func (c *virtualCellConstructor) buildFullTree(address api.CellAddress) *VirtualCell {
	ce, ok := c.cellChainElements[c.buildingChild]
	if !ok {
		panic(fmt.Sprintf("cellType %v in VirtualCells is not found in cell types definition", c.buildingChild))
	}
	cellInstance := c.buildChildCell(c.buildingChild, address)
	// set GPU type only for top-level cells (as a chain shares the same GPU type)
	cellInstance.GetAPIStatus().GpuType = ce.gpuType
	return cellInstance
}

func (c *virtualCellConstructor) build() (
	map[api.VirtualClusterName]map[CellChain]map[CellLevel]int32,
	map[api.VirtualClusterName]map[CellChain]ChainCellList,
	map[api.VirtualClusterName]map[CellChain]ChainCellList,
	map[api.VirtualClusterName]map[api.ReservationId]ChainCellList,
	map[api.VirtualClusterName]map[api.ReservationId]*PhysicalCell) {

	for vc, spec := range c.specs {
		c.vcFreeCellNum[vc] = map[CellChain]map[CellLevel]int32{}
		c.nonReservedFullList[vc] = map[CellChain]ChainCellList{}
		c.nonReservedFreeList[vc] = map[CellChain]ChainCellList{}
		c.reservedCellList[vc] = map[api.ReservationId]ChainCellList{}
		c.reservedPhysicalCells[vc] = map[api.ReservationId]*PhysicalCell{}

		numCells := int32(0)
		for _, virtualCell := range spec.VirtualCells {
			sl := strings.Split(string(virtualCell.CellType), ".")
			chain := CellChain(sl[0])
			rootType := api.CellType(sl[len(sl)-1])
			rootLevel := c.cellChainElements[rootType].level
			if _, ok := c.vcFreeCellNum[vc][chain]; !ok {
				c.vcFreeCellNum[vc][chain] = map[CellLevel]int32{}
			}
			c.vcFreeCellNum[vc][chain][rootLevel] += virtualCell.CellNumber
			for i := int32(0); i < virtualCell.CellNumber; i++ {
				c.updateInternalStatus(vc, CellChain(sl[0]), rootType, nil, "")
				rootCell := c.buildFullTree(api.CellAddress(fmt.Sprintf("%v/%v", vc, numCells)))
				if _, ok := c.nonReservedFreeList[vc][rootCell.chain]; !ok {
					c.nonReservedFreeList[vc][rootCell.chain] = ChainCellList{}
				}
				c.nonReservedFreeList[vc][rootCell.chain][rootCell.level] = append(
					c.nonReservedFreeList[vc][rootCell.chain][rootCell.level], rootCell)
				numCells++
			}
		}

		for _, reservedCell := range spec.ReservedCells {
			rid := reservedCell.ReservationId
			pc, ok := c.rawReservedPhysicalCells[rid]
			if !ok {
				panic(fmt.Sprintf("reservationId not found in physicalCells: VC: %v, ID: %v", vc, rid))
			}
			c.reservedPhysicalCells[vc][rid] = pc
			// get cellType by reservationId
			buildingChild := api.CellType(pc.chain)
			for c.cellChainElements[buildingChild].level > pc.level {
				buildingChild = c.cellChainElements[buildingChild].childCellType
			}

			if _, ok := c.vcFreeCellNum[vc][pc.chain]; !ok {
				c.vcFreeCellNum[vc][pc.chain] = map[CellLevel]int32{}
			}
			c.vcFreeCellNum[vc][pc.chain][pc.level]++
			c.updateInternalStatus(vc, pc.chain, buildingChild, nil, rid)
			c.buildFullTree(api.CellAddress(fmt.Sprintf("%v/%v", vc, numCells)))
			numCells++
		}
	}
	return c.vcFreeCellNum, c.nonReservedFullList, c.nonReservedFreeList, c.reservedCellList, c.reservedPhysicalCells
}

func parseCellChainInfo(
	cellChainElements map[api.CellType]*cellChainElement,
	chains []CellChain) (
	map[CellChain]map[CellLevel]int32,
	map[CellChain]map[CellLevel]api.CellType,
	map[string][]CellChain) {

	cellLevelToGpuNum := map[CellChain]map[CellLevel]int32{}
	cellLevelToType := map[CellChain]map[CellLevel]api.CellType{}
	gpuTypeToChain := map[string][]CellChain{}
	for _, chain := range chains {
		ce := cellChainElements[api.CellType(chain)]
		gpuTypeToChain[ce.gpuType] = append(gpuTypeToChain[ce.gpuType], chain)

		cellLevelToGpuNum[chain] = map[CellLevel]int32{}
		cellLevelToType[chain] = map[CellLevel]api.CellType{}
		ce, ok := cellChainElements[api.CellType(chain)]
		for ok {
			cellLevelToGpuNum[chain][ce.level] = ce.gpuNumber
			cellLevelToType[chain][ce.level] = ce.cellType
			ce, ok = cellChainElements[ce.childCellType]
		}
	}
	return cellLevelToGpuNum, cellLevelToType, gpuTypeToChain

}

func ParseConfig(sConfig *api.Config) (
	physicalFullCellList map[CellChain]ChainCellList, // chain:level:[]physicalCell
	physicalFreeCellList map[CellChain]ChainCellList, // chain:level:[]physicalCell
	vcFreeCellNum map[api.VirtualClusterName]map[CellChain]map[CellLevel]int32, // vc:chain:level:numCells
	virtualNonReservedFullCellList map[api.VirtualClusterName]map[CellChain]ChainCellList, // vc:chain:level:[]virtualCell
	virtualNonReservedFreeCellList map[api.VirtualClusterName]map[CellChain]ChainCellList, // vc:chain:level:[]virtualCell
	virtualReservedFullCellList map[api.VirtualClusterName]map[api.ReservationId]ChainCellList, // vc:reservationId:level:[]virtualCell
	reservedPhysicalCells map[api.VirtualClusterName]map[api.ReservationId]*PhysicalCell, // vc:reservationId:PhysicalCell
	cellLevelToGpuNum map[CellChain]map[CellLevel]int32, // chain:level:gpuNumber
	gpuTypeToChain map[string][]CellChain, // gpuType:[]chain
	cellLevelToType map[CellChain]map[CellLevel]api.CellType, // chain:level:cellType
) {

	cellTypes := sConfig.PhysicalCluster.CellTypes
	cellChainElements := newCellTypeConstructor(cellTypes).buildCellChains()

	physicalSpecs := sConfig.PhysicalCluster.PhysicalCells
	// physicalFullCellList is a full cell list containing ALL cells in the physical cluster,
	// i.e., including both the top-level cells and also their children at the lower levels.
	// On the contrary, physicalFreeCellList only contains the free cells (i.e., the top-level ones).
	// This difference also exists in the virtual cell lists below.
	physicalFullCellList, physicalFreeCellList, rawReservedPhysicalCells :=
		newPhysicalCellConstructor(cellChainElements, physicalSpecs).build()

	virtualSpecs := sConfig.VirtualClusters
	vcFreeCellNum, virtualNonReservedFullCellList, virtualNonReservedFreeCellList, virtualReservedFullCellList, reservedPhysicalCells =
		newVirtualCellConstructor(cellChainElements, *virtualSpecs, rawReservedPhysicalCells).build()

	cellChains := make([]CellChain, 0, len(physicalFullCellList))
	for k := range physicalFullCellList {
		cellChains = append(cellChains, k)
	}
	cellLevelToGpuNum, cellLevelToType, gpuTypeToChain = parseCellChainInfo(cellChainElements, cellChains)

	return
}
