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
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

type (
	CellChain          string // name of a cell chain (type of the top-level cell)
	CellLevel          int32
	CellPriority       int32
	CellState          string
	AffinityGroupState string
)

type schedulingRequest struct {
	vc                   api.VirtualClusterName
	pinnedCellId         api.PinnedCellId
	chain                CellChain
	affinityGroupName    string
	affinityGroupPodNums map[int32]int32 // leaf cell number -> pod number
	priority             CellPriority
	suggestedNodes       common.Set
	ignoreSuggestedNodes bool
	quota                api.Quota
}

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

// AlgoAffinityGroup is the algorithm-internal representation of an affinity group.
type AlgoAffinityGroup struct {
	name                 string
	vc                   api.VirtualClusterName
	lazyPreemptionEnable bool
	// Whether we should ignore K8s suggested nodes. If false, we will avoid binding cells to non-suggested nodes.
	// Note that we always avoid using bad nodes; avoiding non-suggested nodes is optional and best-effort.
	ignoreK8sSuggestedNodes   bool
	priority                  int32
	totalPodNums              map[int32]int32       // LeafCellNum -> PodNum
	allocatedPods             map[int32][]*core.Pod // LeafCellNum -> a list of allocated pods
	preemptingPods            map[types.UID]*core.Pod
	physicalLeafCellPlacement groupPhysicalPlacement
	virtualLeafCellPlacement  groupVirtualPlacement
	state                     AffinityGroupState
	lazyPreemptionStatus      *api.LazyPreemptionStatus
	quota                     api.Quota
}

func newAlgoAffinityGroup(
	g *api.AffinityGroupSpec,
	vc api.VirtualClusterName,
	lazyPreemptionEnable bool,
	priority int32,
	quota api.Quota,
	state AffinityGroupState) *AlgoAffinityGroup {

	podNums := make(map[int32]int32)
	for _, m := range g.Members {
		podNums[m.LeafCellNumber] += m.PodNumber
	}
	group := &AlgoAffinityGroup{
		name:                      g.Name,
		vc:                        vc,
		lazyPreemptionEnable:      lazyPreemptionEnable,
		priority:                  priority,
		totalPodNums:              podNums,
		allocatedPods:             map[int32][]*core.Pod{},
		physicalLeafCellPlacement: groupPhysicalPlacement{},
		virtualLeafCellPlacement:  groupVirtualPlacement{},
		state:                     state,
		quota:                     quota,
	}
	if state == groupPreempting {
		group.preemptingPods = map[types.UID]*core.Pod{}
	}
	for leafCellNum, podNum := range podNums {
		group.physicalLeafCellPlacement[leafCellNum] = make([]CellList, podNum)
		group.virtualLeafCellPlacement[leafCellNum] = make([]CellList, podNum)
		group.allocatedPods[leafCellNum] = make([]*core.Pod, podNum)
		for i := int32(0); i < podNum; i++ {
			group.physicalLeafCellPlacement[leafCellNum][i] = make(CellList, leafCellNum)
			group.virtualLeafCellPlacement[leafCellNum][i] = make(CellList, leafCellNum)
		}
	}
	return group
}

func (aag *AlgoAffinityGroup) ToAffinityGroup() api.AffinityGroup {
	ag := api.AffinityGroup{
		ObjectMeta: api.ObjectMeta{Name: aag.name},
		Status: api.AffinityGroupStatus{
			VC:                   aag.vc,
			Priority:             aag.priority,
			State:                api.AffinityGroupState(aag.state),
			LazyPreemptionStatus: aag.lazyPreemptionStatus,
		},
	}
	if aag.physicalLeafCellPlacement != nil {
		ag.Status.PhysicalPlacement = aag.physicalLeafCellPlacement.nodeToLeafCellIndices()
	}
	if aag.virtualLeafCellPlacement != nil {
		ag.Status.VirtualPlacement = aag.virtualLeafCellPlacement.preassignedCellToLeafCells()
	}
	for _, pods := range aag.allocatedPods {
		for _, p := range pods {
			if p != nil {
				ag.Status.AllocatedPods = append(ag.Status.AllocatedPods, p.UID)
			}
		}
	}
	for p := range aag.preemptingPods {
		ag.Status.PreemptingPods = append(ag.Status.PreemptingPods, p)
	}
	return ag
}

type groupPhysicalPlacement map[int32][]CellList // LeafCellNum -> a list of pods -> a list of physical leaf cells of each pod
type groupVirtualPlacement map[int32][]CellList  // LeafCellNum -> a list of pods -> a list of virtual leaf cells of each pod

func (p groupPhysicalPlacement) String() string {
	return common.ToJson(p.nodeToLeafCellIndices())
}

func (p groupPhysicalPlacement) nodeToLeafCellIndices() map[string][]int32 {
	nodeToLeafCellIndices := map[string][]int32{}
	for _, podPlacements := range p {
		for _, podPlacement := range podPlacements {
			for _, leafCell := range podPlacement {
				pLeafCell := leafCell.(*PhysicalCell)
				nodes, leafCellIndices := pLeafCell.GetPhysicalPlacement()
				if _, ok := nodeToLeafCellIndices[nodes[0]]; !ok {
					nodeToLeafCellIndices[nodes[0]] = []int32{}
				}
				nodeToLeafCellIndices[nodes[0]] = append(nodeToLeafCellIndices[nodes[0]], leafCellIndices[0])
			}
		}
	}
	return nodeToLeafCellIndices
}

func (p groupVirtualPlacement) String() string {
	return common.ToJson(p.preassignedCellToLeafCells())
}

func (p groupVirtualPlacement) preassignedCellToLeafCells() map[api.CellAddress][]api.CellAddress {
	preassignedCellToLeafCells := map[api.CellAddress][]api.CellAddress{}
	for _, podPlacements := range p {
		for _, podPlacement := range podPlacements {
			for _, leafCell := range podPlacement {
				vLeafCell := leafCell.(*VirtualCell)
				address := vLeafCell.GetAddress()
				preassignedAddress := vLeafCell.GetPreassignedCell().GetAddress()
				if _, ok := preassignedCellToLeafCells[preassignedAddress]; !ok {
					preassignedCellToLeafCells[preassignedAddress] = []api.CellAddress{}
				}
				preassignedCellToLeafCells[preassignedAddress] = append(
					preassignedCellToLeafCells[preassignedAddress], address)
			}
		}
	}
	return preassignedCellToLeafCells
}

func (p groupVirtualPlacement) toPhysicalPlacement(
	bindings map[api.CellAddress]*PhysicalCell,
	leafCellNums []int32) groupPhysicalPlacement {

	physicalPlacement := groupPhysicalPlacement{}
	for _, podLeafCellNum := range leafCellNums {
		podPlacements := p[podLeafCellNum]
		physicalPlacement[podLeafCellNum] = make([]CellList, len(podPlacements))
		for i, podPlacement := range podPlacements {
			physicalPlacement[podLeafCellNum][i] = make(CellList, len(podPlacement))
			for j, leafCell := range podPlacement {
				pLeafCell := bindings[leafCell.GetAddress()]
				physicalPlacement[podLeafCellNum][i][j] = pLeafCell
			}
		}
	}
	return physicalPlacement
}

// A binding path is a tree consisting of all cells that should be bound for binding a set of
// lowest-level cells in a physical placement. It is generated by collecting all the unbound
// ancestors for these cells and group them in a tree.
func (p groupVirtualPlacement) toBindingPaths(
	leafCellNums []int32,
	bindings map[api.CellAddress]*PhysicalCell) (
	preassignedCells []*cellBindingPathVertex,
	nonPreassignedCells [][]*cellBindingPathVertex) {

	allBindingPathVertices := map[api.CellAddress]*cellBindingPathVertex{}
	for _, podLeafCellNum := range leafCellNums {
		podPlacements := p[podLeafCellNum]
		for _, podPlacement := range podPlacements {
			for _, leafCell := range podPlacement {
				if pLeafCell := leafCell.(*VirtualCell).GetPhysicalCell(); pLeafCell != nil {
					bindings[leafCell.GetAddress()] = pLeafCell
					continue
				}
				var bindingPath []*VirtualCell
				for c := leafCell; c != nil; c = c.GetParent() {
					vc := c.(*VirtualCell)
					if vc.GetPhysicalCell() != nil || allBindingPathVertices[vc.GetAddress()] != nil {
						break
					}
					bindingPath = append(bindingPath, vc)
				}
				pathRoot := bindingPath[len(bindingPath)-1]
				n := &cellBindingPathVertex{cell: pathRoot}
				allBindingPathVertices[pathRoot.GetAddress()] = n
				if parent := pathRoot.GetParent(); parent == nil {
					preassignedCells = append(preassignedCells, n)
				} else if parent.(*VirtualCell).GetPhysicalCell() != nil {
					buddyExist := false
					for i := range nonPreassignedCells {
						if CellEqual(parent, nonPreassignedCells[i][0].cell.GetParent()) {
							buddyExist = true
							nonPreassignedCells[i] = append(nonPreassignedCells[i], n)
							break
						}
					}
					if !buddyExist {
						nonPreassignedCells = append(nonPreassignedCells, []*cellBindingPathVertex{n})
					}
				} else {
					parentNode := allBindingPathVertices[pathRoot.GetParent().GetAddress()]
					parentNode.childrenToBind = append(parentNode.childrenToBind, n)
				}
				for i := len(bindingPath) - 2; i >= 0; i-- {
					c := bindingPath[i]
					n := &cellBindingPathVertex{cell: c}
					parentNode := allBindingPathVertices[c.GetParent().GetAddress()]
					parentNode.childrenToBind = append(parentNode.childrenToBind, n)
					allBindingPathVertices[c.GetAddress()] = n
				}
			}
		}
	}
	return preassignedCells, nonPreassignedCells
}

// cellBindingPathVertex is a single vertex in the tree of a cell binding path,
// containing the vertices of its children to bind.
type cellBindingPathVertex struct {
	cell           *VirtualCell
	childrenToBind []*cellBindingPathVertex
}
