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
	"strings"

	"github.com/microsoft/hivedscheduler/pkg/api"
	"github.com/microsoft/hivedscheduler/pkg/common"
)

// internal wrapper for spec cellTypes
type cellChainElement struct {
	cellType       api.CellType // current cell type
	level          CellLevel    // current cell level, leaf cell is 1
	childCellType  api.CellType // child cell type
	childNumber    int32        // child number
	hasNode        bool         // current cell type is a node or above cell
	isMultiNodes   bool         // current cell type is a multiple node cell
	leafCellType   string       // current cell leaf cell type
	leafCellNumber int32        // how many leaf cell in current cell
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
			cellType:       ct,
			level:          lowestLevel,
			childCellType:  "",
			childNumber:    0,
			hasNode:        false,
			isMultiNodes:   false,
			leafCellType:   string(ct),
			leafCellNumber: 1,
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
		cellType:       ct,
		level:          cct.level + 1,
		childCellType:  cct.cellType,
		childNumber:    ctSpec.ChildCellNumber,
		hasNode:        cct.hasNode || ctSpec.IsNodeLevel,
		isMultiNodes:   cct.hasNode,
		leafCellType:   cct.leafCellType,
		leafCellNumber: cct.leafCellNumber * ctSpec.ChildCellNumber,
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
	fullCellList map[CellChain]ChainCellList
	freeCellList map[CellChain]ChainCellList
	pinnedCells  map[api.PinnedCellId]*PhysicalCell
	// internal status
	buildingChain CellChain            // current building chain, it is the top cell type in physicalCells
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
		pinnedCells:       map[api.PinnedCellId]*PhysicalCell{},
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
	cellInstance := c.addCell(c.buildingChain, ce, spec.PinnedCellId, spec.CellAddress)
	if ce.level == 1 {
		cellInstance.SetPhysicalResources(
			[]string{currentNode}, []int32{common.StringToInt32(splitAddress[len(splitAddress)-1])})
		return cellInstance
	}
	var currentCellNodes []string
	var currentCellLeafCellIndices []int32
	var currentCellChildren CellList
	for _, childSpec := range spec.CellChildren {
		childCellInstance := c.buildChildCell(childSpec, ce.childCellType, currentNode)
		childCellInstance.SetParent(cellInstance)
		currentCellChildren = append(currentCellChildren, childCellInstance)
		if ce.isMultiNodes {
			// super-node cell merge child nodes
			currentCellNodes = append(currentCellNodes, childCellInstance.nodes...)
		} else {
			// sub-node cell merge child node leaf cell indices
			currentCellLeafCellIndices = append(currentCellLeafCellIndices, childCellInstance.leafCellIndices...)
		}
	}
	// update current cell children and resource
	cellInstance.SetChildren(currentCellChildren)
	if ce.isMultiNodes {
		currentCellLeafCellIndices = []int32{-1}
	} else {
		currentCellNodes = []string{currentNode}
	}
	cellInstance.SetPhysicalResources(currentCellNodes, currentCellLeafCellIndices)

	return cellInstance
}

func (c *physicalCellConstructor) addCell(
	chain CellChain,
	ce *cellChainElement,
	pid api.PinnedCellId,
	address api.CellAddress) *PhysicalCell {

	cellInstance := NewPhysicalCell(
		c.buildingChain, ce.level, ce.hasNode, ce.leafCellNumber, ce.cellType, address, ce.hasNode && !ce.isMultiNodes)
	if _, ok := c.fullCellList[chain]; !ok {
		c.fullCellList[chain] = ChainCellList{}
	}
	c.fullCellList[chain][ce.level] = append(c.fullCellList[chain][ce.level], cellInstance)
	// record and mark pinned cell
	if pid != "" {
		c.pinnedCells[pid] = cellInstance
		cellInstance.SetPinned(true)
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
	// set leaf cell type only for top-level cells (as a chain shares the same leaf cell type)
	cellInstance.GetAPIStatus().LeafCellType = ce.leafCellType
	return cellInstance
}

func (c *physicalCellConstructor) build() (
	map[CellChain]ChainCellList,
	map[CellChain]ChainCellList,
	map[api.PinnedCellId]*PhysicalCell) {

	for _, spec := range c.cellChainSpecs {
		c.updateInternalStatus(CellChain(spec.CellType), spec)
		rootCell := c.buildFullTree()
		if _, ok := c.freeCellList[rootCell.chain]; !ok {
			c.freeCellList[rootCell.chain] = NewChainCellList(rootCell.level)
		}
		c.freeCellList[rootCell.chain][rootCell.level] = append(
			c.freeCellList[rootCell.chain][rootCell.level], rootCell)
	}
	return c.fullCellList, c.freeCellList, c.pinnedCells
}

type virtualCellConstructor struct {
	// input
	cellChainElements     map[api.CellType]*cellChainElement
	specs                 map[api.VirtualClusterName]api.VirtualClusterSpec
	rawPinnedPhysicalList map[api.PinnedCellId]*PhysicalCell // pid:physicalCell
	// output
	vcFreeCellNum      map[api.VirtualClusterName]map[CellChain]map[CellLevel]int32  // vc:cellChain:cellLevel:numCells
	nonPinnedFullList  map[api.VirtualClusterName]map[CellChain]ChainCellList        // vc:cellChain:cellLevel:virtualCells
	nonPinnedFreeList  map[api.VirtualClusterName]map[CellChain]ChainCellList        // vc:cellChain:cellLevel:virtualCells
	pinnedList         map[api.VirtualClusterName]map[api.PinnedCellId]ChainCellList // vc:pid:cellLevel:virtualCells
	pinnedPhysicalList map[api.VirtualClusterName]map[api.PinnedCellId]*PhysicalCell // vc:pid:physicalCell
	// internal status
	buildingVc    api.VirtualClusterName // current building vc
	buildingChain CellChain              // current building chain, it's a in a.b.c
	buildingChild api.CellType           // current building child, it's c in a.b.c
	buildingRoot  *VirtualCell           // current building root cell, it's instance of c in a.b.c
	buildingPId   api.PinnedCellId       // current building pinned cell
}

func newVirtualCellConstructor(
	cellChains map[api.CellType]*cellChainElement,
	specs map[api.VirtualClusterName]api.VirtualClusterSpec,
	pinnedCells map[api.PinnedCellId]*PhysicalCell) *virtualCellConstructor {

	return &virtualCellConstructor{
		cellChainElements:     cellChains,
		specs:                 specs,
		rawPinnedPhysicalList: pinnedCells,
		vcFreeCellNum:         map[api.VirtualClusterName]map[CellChain]map[CellLevel]int32{},
		nonPinnedFullList:     map[api.VirtualClusterName]map[CellChain]ChainCellList{},
		nonPinnedFreeList:     map[api.VirtualClusterName]map[CellChain]ChainCellList{},
		pinnedList:            map[api.VirtualClusterName]map[api.PinnedCellId]ChainCellList{},
		pinnedPhysicalList:    map[api.VirtualClusterName]map[api.PinnedCellId]*PhysicalCell{},
	}
}

func (c *virtualCellConstructor) updateInternalStatus(buildingVc api.VirtualClusterName, buildingChain CellChain,
	buildingChild api.CellType, buildingRoot *VirtualCell, buildingPinnedCell api.PinnedCellId) {
	c.buildingVc = buildingVc
	c.buildingChain = buildingChain
	c.buildingChild = buildingChild
	c.buildingRoot = buildingRoot
	c.buildingPId = buildingPinnedCell
}

func (c *virtualCellConstructor) addCell(
	chain CellChain,
	vc api.VirtualClusterName,
	ce *cellChainElement,
	address api.CellAddress) *VirtualCell {

	cellInstance := NewVirtualCell(
		vc,
		c.buildingChain,
		ce.level,
		ce.hasNode,
		ce.leafCellNumber,
		nil,
		ce.cellType,
		address,
		ce.hasNode && !ce.isMultiNodes)
	if c.buildingPId == "" {
		if _, ok := c.nonPinnedFullList[vc][chain]; !ok {
			c.nonPinnedFullList[vc][chain] = ChainCellList{}
		}
		c.nonPinnedFullList[vc][chain][ce.level] = append(
			c.nonPinnedFullList[vc][chain][ce.level], cellInstance)
	} else {
		pid := c.buildingPId
		if _, ok := c.pinnedList[vc][pid]; !ok {
			c.pinnedList[vc][pid] = ChainCellList{}
		}
		c.pinnedList[vc][pid][ce.level] = append(
			c.pinnedList[vc][pid][ce.level], cellInstance)
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
	// set leaf cell type only for top-level cells (as a chain shares the same leaf cell type)
	cellInstance.GetAPIStatus().LeafCellType = ce.leafCellType
	return cellInstance
}

func (c *virtualCellConstructor) build() (
	map[api.VirtualClusterName]map[CellChain]map[CellLevel]int32,
	map[api.VirtualClusterName]map[CellChain]ChainCellList,
	map[api.VirtualClusterName]map[CellChain]ChainCellList,
	map[api.VirtualClusterName]map[api.PinnedCellId]ChainCellList,
	map[api.VirtualClusterName]map[api.PinnedCellId]*PhysicalCell) {

	for vc, spec := range c.specs {
		c.vcFreeCellNum[vc] = map[CellChain]map[CellLevel]int32{}
		c.nonPinnedFullList[vc] = map[CellChain]ChainCellList{}
		c.nonPinnedFreeList[vc] = map[CellChain]ChainCellList{}
		c.pinnedList[vc] = map[api.PinnedCellId]ChainCellList{}
		c.pinnedPhysicalList[vc] = map[api.PinnedCellId]*PhysicalCell{}

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
				if _, ok := c.nonPinnedFreeList[vc][rootCell.chain]; !ok {
					c.nonPinnedFreeList[vc][rootCell.chain] = ChainCellList{}
				}
				c.nonPinnedFreeList[vc][rootCell.chain][rootCell.level] = append(
					c.nonPinnedFreeList[vc][rootCell.chain][rootCell.level], rootCell)
				numCells++
			}
		}

		for _, pinnedCell := range spec.PinnedCells {
			pid := pinnedCell.PinnedCellId
			pc, ok := c.rawPinnedPhysicalList[pid]
			if !ok {
				panic(fmt.Sprintf("pinned cell not found in physicalCells: VC: %v, ID: %v", vc, pid))
			}
			c.pinnedPhysicalList[vc][pid] = pc
			// get cellType by pinnedCellId
			buildingChild := api.CellType(pc.chain)
			for c.cellChainElements[buildingChild].level > pc.level {
				buildingChild = c.cellChainElements[buildingChild].childCellType
			}

			if _, ok := c.vcFreeCellNum[vc][pc.chain]; !ok {
				c.vcFreeCellNum[vc][pc.chain] = map[CellLevel]int32{}
			}
			c.vcFreeCellNum[vc][pc.chain][pc.level]++
			c.updateInternalStatus(vc, pc.chain, buildingChild, nil, pid)
			c.buildFullTree(api.CellAddress(fmt.Sprintf("%v/%v", vc, numCells)))
			numCells++
		}
	}
	return c.vcFreeCellNum, c.nonPinnedFullList, c.nonPinnedFreeList, c.pinnedList, c.pinnedPhysicalList
}

func parseCellChainInfo(
	cellChainElements map[api.CellType]*cellChainElement,
	chains []CellChain) (
	map[CellChain]map[CellLevel]int32,
	map[CellChain]map[CellLevel]api.CellType,
	map[CellChain]map[api.CellType]CellLevel,
	map[string][]CellChain,
	map[string][]CellChain) {

	cellLevelToLeafCellNum := map[CellChain]map[CellLevel]int32{}
	cellLevelToType := map[CellChain]map[CellLevel]api.CellType{}
	cellTypeToLevel := map[CellChain]map[api.CellType]CellLevel{}
	cellTypeToChain := map[string][]CellChain{}
	leafCellTypeToChain := map[string][]CellChain{}
	for _, chain := range chains {
		ce := cellChainElements[api.CellType(chain)]
		leafCellTypeToChain[ce.leafCellType] = append(leafCellTypeToChain[ce.leafCellType], chain)

		cellLevelToLeafCellNum[chain] = map[CellLevel]int32{}
		cellLevelToType[chain] = map[CellLevel]api.CellType{}
		cellTypeToLevel[chain] = map[api.CellType]CellLevel{}
		ce, ok := cellChainElements[api.CellType(chain)]
		for ok {
			cellLevelToLeafCellNum[chain][ce.level] = ce.leafCellNumber
			cellLevelToType[chain][ce.level] = ce.cellType
			cellTypeToLevel[chain][ce.cellType] = ce.level
			if !ce.isMultiNodes {
				cellTypeToChain[string(ce.cellType)] = append(cellTypeToChain[string(ce.cellType)], chain)
			}
			ce, ok = cellChainElements[ce.childCellType]
		}
	}
	return cellLevelToLeafCellNum, cellLevelToType, cellTypeToLevel, cellTypeToChain, leafCellTypeToChain
}

func ParseConfig(sConfig *api.Config) (
	physicalFullList map[CellChain]ChainCellList, // chain:level:[]physicalCell
	physicalFreeList map[CellChain]ChainCellList, // chain:level:[]physicalCell
	vcFreeCellNum map[api.VirtualClusterName]map[CellChain]map[CellLevel]int32, // vc:chain:level:numCells
	virtualNonPinnedFullList map[api.VirtualClusterName]map[CellChain]ChainCellList, // vc:chain:level:[]virtualCell
	virtualNonPinnedFreeList map[api.VirtualClusterName]map[CellChain]ChainCellList, // vc:chain:level:[]virtualCell
	virtualPinnedCells map[api.VirtualClusterName]map[api.PinnedCellId]ChainCellList, // vc:pinnedCellId:level:[]virtualCell
	physicalPinnedCells map[api.VirtualClusterName]map[api.PinnedCellId]*PhysicalCell, // vc:pinnedCellId:PhysicalCell
	cellLevelToLeafCellNum map[CellChain]map[CellLevel]int32, // chain:level:leafCellNumber
	leafCellTypeToChain map[string][]CellChain, // leafCellType:[]chain
	cellTypeToChain map[string][]CellChain, // cellType:[]chain
	cellLevelToType map[CellChain]map[CellLevel]api.CellType, // chain:level:cellType
	cellTypeToLevel map[CellChain]map[api.CellType]CellLevel, // chain:cellType:level
) {

	cellTypes := sConfig.PhysicalCluster.CellTypes
	cellChainElements := newCellTypeConstructor(cellTypes).buildCellChains()

	physicalSpecs := sConfig.PhysicalCluster.PhysicalCells
	// physicalFullList is a full cell list containing ALL cells in the physical cluster,
	// i.e., including both the top-level cells and also their children at the lower levels.
	// On the contrary, physicalFreeList only contains the free cells (i.e., the top-level ones).
	// This difference also exists in the virtual cell lists below.
	physicalFullList, physicalFreeList, rawPinnedPhysicalList :=
		newPhysicalCellConstructor(cellChainElements, physicalSpecs).build()

	virtualSpecs := sConfig.VirtualClusters
	vcFreeCellNum, virtualNonPinnedFullList, virtualNonPinnedFreeList, virtualPinnedCells, physicalPinnedCells =
		newVirtualCellConstructor(cellChainElements, *virtualSpecs, rawPinnedPhysicalList).build()

	cellChains := make([]CellChain, 0, len(physicalFullList))
	for k := range physicalFullList {
		cellChains = append(cellChains, k)
	}
	cellLevelToLeafCellNum, cellLevelToType, cellTypeToLevel, cellTypeToChain, leafCellTypeToChain =
		parseCellChainInfo(cellChainElements, cellChains)

	return
}
