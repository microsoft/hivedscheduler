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
)

type (
	CellChain          string // name of a cell chain (type of the top-level cell)
	CellLevel          int32
	CellPriority       int32
	CellState          string
	AffinityGroupState string
)

// CellList is a list of cells at a certain level of a chain.
type CellList []Cell

func (cl CellList) String() string {
	names := make([]string, len(cl))
	for i, c := range cl {
		if cc, ok := c.(*PhysicalCell); ok {
			names[i] = fmt.Sprintf("%v(%v)(%v)", cc.GetAddress(), cc.GetPriority(), cc.GetPhysicalPlacementString())
		} else {
			names[i] = fmt.Sprintf("%v(%v)", c.GetAddress(), c.GetPriority())
		}
	}
	return strings.Join(names, ", ")
}

func (cl CellList) contains(c Cell) bool {
	for _, cc := range cl {
		if CellEqual(cc, c) {
			return true
		}
	}
	return false
}

func (cl CellList) remove(c Cell) CellList {
	index := -1
	for i, cc := range cl {
		if CellEqual(cc, c) {
			index = i
			break
		}
	}
	if index < 0 {
		panic(fmt.Sprintf("Cell not not found in list when removing: %v",
			c.GetAddress()))
	}
	length := len(cl)
	cl[index] = cl[length-1]
	cl[length-1] = nil
	return cl[:length-1]
}

// ChainCellList maps each level in a chain to a CellList.
type ChainCellList map[CellLevel]CellList

func NewChainCellList(top CellLevel) ChainCellList {
	ccl := ChainCellList{}
	for i := CellLevel(1); i <= top; i++ {
		ccl[i] = CellList{}
	}
	return ccl
}

func (ccl ChainCellList) String() string {
	str := ""
	for i := 1; i <= len(ccl); i++ {
		str += fmt.Sprintf("level %v: %v\n", i, ccl[CellLevel(i)])
	}
	return str
}

func (ccl ChainCellList) contains(c Cell, l CellLevel) bool {
	return ccl[l].contains(c)
}

func (ccl ChainCellList) remove(c Cell, l CellLevel) {
	ccl[l] = ccl[l].remove(c)
}

func (ccl ChainCellList) shallowCopy() ChainCellList {
	copied := ChainCellList{}
	for l := CellLevel(1); l <= CellLevel(len(ccl)); l++ {
		copied[l] = make(CellList, len(ccl[l]))
		copy(copied[l], ccl[l])
	}
	return copied
}

// cellBindingPathVertex is a single vertex in the tree of a cell binding path,
// containing the vertices of its children to bind.
type cellBindingPathVertex struct {
	cell           *VirtualCell
	childrenToBind []*cellBindingPathVertex
}
