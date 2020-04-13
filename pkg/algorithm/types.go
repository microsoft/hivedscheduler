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
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"strings"
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
	affinityGroupPodNums map[int32]int32 // gpu number -> pod number
	priority             CellPriority
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
	priority             int32
	totalPodNums         map[int32]int32       // GpuNum -> PodNum
	allocatedPods        map[int32][]*core.Pod // GpuNum -> a list of allocated pods
	preemptingPods       map[types.UID]*core.Pod
	physicalGpuPlacement groupPhysicalPlacement
	virtualGpuPlacement  groupVirtualPlacement
	state                AffinityGroupState
	lazyPreemptionStatus *api.LazyPreemptionStatus
}

func newAlgoAffinityGroup(
	g *api.AffinityGroupSpec,
	vc api.VirtualClusterName,
	lazyPreemptionEnable bool,
	priority int32,
	state AffinityGroupState) *AlgoAffinityGroup {

	podNums := make(map[int32]int32)
	for _, m := range g.Members {
		podNums[m.GpuNumber] += m.PodNumber
	}
	group := &AlgoAffinityGroup{
		name:                 g.Name,
		vc:                   vc,
		lazyPreemptionEnable: lazyPreemptionEnable,
		priority:             priority,
		totalPodNums:         podNums,
		allocatedPods:        map[int32][]*core.Pod{},
		physicalGpuPlacement: groupPhysicalPlacement{},
		virtualGpuPlacement:  groupVirtualPlacement{},
		state:                state,
	}
	if state == groupPreempting {
		group.preemptingPods = map[types.UID]*core.Pod{}
	}
	for gpuNum, podNum := range podNums {
		group.physicalGpuPlacement[gpuNum] = make([]CellList, podNum)
		group.virtualGpuPlacement[gpuNum] = make([]CellList, podNum)
		group.allocatedPods[gpuNum] = make([]*core.Pod, podNum)
		for i := int32(0); i < podNum; i++ {
			group.physicalGpuPlacement[gpuNum][i] = make(CellList, gpuNum)
			group.virtualGpuPlacement[gpuNum][i] = make(CellList, gpuNum)
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
	if aag.physicalGpuPlacement != nil {
		ag.Status.PhysicalPlacement = aag.physicalGpuPlacement.nodeToGpuIndices()
	}
	if aag.virtualGpuPlacement != nil {
		ag.Status.VirtualPlacement = aag.virtualGpuPlacement.preassignedCellToLeafCells()
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

type groupPhysicalPlacement map[int32][]CellList // GpuNum -> a list of pods -> a list of physical GPU cells of each pod
type groupVirtualPlacement map[int32][]CellList  // GpuNum -> a list of pods -> a list of virtual GPU cells of each pod

func (p groupPhysicalPlacement) String() string {
	return common.ToJson(p.nodeToGpuIndices())
}

func (p groupPhysicalPlacement) nodeToGpuIndices() map[string][]int32 {
	nodeToGpuIndices := map[string][]int32{}
	for _, podPlacements := range p {
		for _, pod := range podPlacements {
			for _, gpu := range pod {
				pGpu := gpu.(*PhysicalCell)
				nodes, gpuIndices := pGpu.GetPhysicalPlacement()
				if _, ok := nodeToGpuIndices[nodes[0]]; !ok {
					nodeToGpuIndices[nodes[0]] = []int32{}
				}
				nodeToGpuIndices[nodes[0]] = append(nodeToGpuIndices[nodes[0]], gpuIndices[0])
			}
		}
	}
	return nodeToGpuIndices
}

func (p groupVirtualPlacement) String() string {
	return common.ToJson(p.preassignedCellToLeafCells())
}

func (p groupVirtualPlacement) preassignedCellToLeafCells() map[api.CellAddress][]api.CellAddress {
	preassignedCellToLeafCells := map[api.CellAddress][]api.CellAddress{}
	for _, podPlacements := range p {
		for _, pod := range podPlacements {
			for _, gpu := range pod {
				vGpu := gpu.(*VirtualCell)
				address := vGpu.GetAddress()
				preassignedAddress := vGpu.GetPreAssignedCell().GetAddress()
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
