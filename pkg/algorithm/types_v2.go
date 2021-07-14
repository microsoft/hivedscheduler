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
	"github.com/microsoft/hivedscheduler/pkg/api"
	apiv2 "github.com/microsoft/hivedscheduler/pkg/api/v2"
	"github.com/microsoft/hivedscheduler/pkg/common"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// PodGroupState represents the internal state of pod group.
type PodGroupState string

// PodGroupSchedulingRequest represents request of pod group.
type PodGroupSchedulingRequest struct {
	vc           api.VirtualClusterName
	pinnedCellId api.PinnedCellId
	podRootGroup apiv2.PodGroupSpec
	chain        CellChain
	priority     CellPriority
}

// PodGroupSchedulingStatus represents internal scheduling status of pod group.
type PodGroupSchedulingStatus struct {
	name                 string
	vc                   api.VirtualClusterName
	priority             CellPriority
	lazyPreemptionEnable bool
	preemptingPods       map[types.UID]*core.Pod
	allocatedPodGroup    AllocatedPodGroup
	virtualPlacement     PodGroupVirtualPlacement
	physicalPlacement    PodGroupPhysicalPlacement
	state                PodGroupState
	lazyPreemptionStatus *api.LazyPreemptionStatus
}

func (podGroupSchedStatus *PodGroupSchedulingStatus) DumpPodGroup() apiv2.PodGroupItem {
	podGroupItem := apiv2.PodGroupItem{
		ObjectMeta: api.ObjectMeta{Name: podGroupSchedStatus.name},
		Status: apiv2.PodGroupStatus{
			VC:                   podGroupSchedStatus.vc,
			Priority:             int32(podGroupSchedStatus.priority),
			State:                apiv2.PodGroupState(podGroupSchedStatus.state),
			LazyPreemptionStatus: podGroupSchedStatus.lazyPreemptionStatus,
		},
	}
	if !PodGroupPlacement(podGroupSchedStatus.physicalPlacement).IsEmpty() {
		podGroupItem.Status.PhysicalPlacement = podGroupSchedStatus.physicalPlacement.nodeToLeafCellIndices()
	}
	if !PodGroupPlacement(podGroupSchedStatus.virtualPlacement).IsEmpty() {
		podGroupItem.Status.VirtualPlacement = podGroupSchedStatus.virtualPlacement.preassignedCellToLeafCells()
	}
	for iter := podGroupSchedStatus.allocatedPodGroup.Iterator(); iter.HasNext(); {
		pod := iter.Next()
		if pod != nil {
			podGroupItem.Status.AllocatedPods = append(podGroupItem.Status.AllocatedPods, pod.UID)
		}
	}
	for uid := range podGroupSchedStatus.preemptingPods {
		podGroupItem.Status.PreemptingPods = append(podGroupItem.Status.PreemptingPods, uid)
	}
	return podGroupItem
}

func newPodGroupSchedulingStatus(
	podSchedSpec *apiv2.PodSchedulingSpec,
	leafCellNums map[CellLevel]int32,
	cellLevel map[api.CellType]CellLevel,
	state PodGroupState) *PodGroupSchedulingStatus {

	podGroupSchedStatus := &PodGroupSchedulingStatus{
		name:                 podSchedSpec.PodRootGroup.Name,
		vc:                   podSchedSpec.VirtualCluster,
		priority:             CellPriority(podSchedSpec.Priority),
		lazyPreemptionEnable: podSchedSpec.LazyPreemptionEnable,
		allocatedPodGroup:    AllocatedPodGroup{},
		virtualPlacement:     PodGroupVirtualPlacement{},
		physicalPlacement:    PodGroupPhysicalPlacement{},
		state:                state,
	}
	if state == podGroupPreempting {
		podGroupSchedStatus.preemptingPods = map[types.UID]*core.Pod{}
	}
	podGroupSpecQueue := []*apiv2.PodGroupSpec{podSchedSpec.PodRootGroup}
	allocatedPodGroupQueue := []*AllocatedPodGroup{&podGroupSchedStatus.allocatedPodGroup}
	virtualPlacementQueue := []*PodGroupPlacement{(*PodGroupPlacement)(&podGroupSchedStatus.virtualPlacement)}
	physicalPlacementQueue := []*PodGroupPlacement{(*PodGroupPlacement)(&podGroupSchedStatus.physicalPlacement)}
	for len(podGroupSpecQueue) > 0 {
		newPodGroupSpecQueue := []*apiv2.PodGroupSpec{}
		newAllocatedPodGroupQueue := []*AllocatedPodGroup{}
		newVirtualPlacementQueue := []*PodGroupPlacement{}
		newPhysicalPlacementQueue := []*PodGroupPlacement{}
		for index, podGroup := range podGroupSpecQueue {
			podNum := int32(0)
			for _, pod := range podGroup.Pods {
				podNum += pod.PodMinNumber
			}
			allocatedPodGroupQueue[index].pods = make([]*core.Pod, podNum)
			virtualPlacementQueue[index].podsPlacement = make([]CellList, podNum)
			physicalPlacementQueue[index].podsPlacement = make([]CellList, podNum)
			podNumIndex := int32(0)
			for _, pod := range podGroup.Pods {
				for i := int32(0); i < pod.PodMinNumber; i++ {
					if level, ok := cellLevel[pod.CellsPerPod.CellType]; ok {
						virtualPlacementQueue[index].podsPlacement[podNumIndex] = make(CellList, pod.CellsPerPod.CellNumber*leafCellNums[level])
						physicalPlacementQueue[index].podsPlacement[podNumIndex] = make(CellList, pod.CellsPerPod.CellNumber*leafCellNums[level])
					} else {
						virtualPlacementQueue[index].podsPlacement[podNumIndex] = make(CellList, pod.CellsPerPod.CellNumber)
						physicalPlacementQueue[index].podsPlacement[podNumIndex] = make(CellList, pod.CellsPerPod.CellNumber)
					}
					podNumIndex++
				}
			}
			if podGroup.ChildGroups != nil {
				allocatedPodGroupQueue[index].allocatedChildGroup = make([]*AllocatedPodGroup, len(podGroup.ChildGroups))
				virtualPlacementQueue[index].childGroupsPlacement = make([]*PodGroupPlacement, len(podGroup.ChildGroups))
				physicalPlacementQueue[index].childGroupsPlacement = make([]*PodGroupPlacement, len(podGroup.ChildGroups))
				for childIndex := range podGroup.ChildGroups {
					allocatedPodGroupQueue[index].allocatedChildGroup[childIndex] = &AllocatedPodGroup{}
					virtualPlacementQueue[index].childGroupsPlacement[childIndex] = &PodGroupPlacement{}
					physicalPlacementQueue[index].childGroupsPlacement[childIndex] = &PodGroupPlacement{}
				}
			}
			newPodGroupSpecQueue = append(newPodGroupSpecQueue, podGroup.ChildGroups...)
			newAllocatedPodGroupQueue = append(newAllocatedPodGroupQueue, allocatedPodGroupQueue[index].allocatedChildGroup...)
			newVirtualPlacementQueue = append(newVirtualPlacementQueue, virtualPlacementQueue[index].childGroupsPlacement...)
			newPhysicalPlacementQueue = append(newPhysicalPlacementQueue, physicalPlacementQueue[index].childGroupsPlacement...)
		}
		podGroupSpecQueue = newPodGroupSpecQueue
		allocatedPodGroupQueue = newAllocatedPodGroupQueue
		virtualPlacementQueue = newVirtualPlacementQueue
		newPhysicalPlacementQueue = physicalPlacementQueue
	}
	return podGroupSchedStatus
}

// AllocatedPodGroup represents a tree structure of allocated pod group.
type AllocatedPodGroup struct {
	pods                []*core.Pod
	allocatedChildGroup []*AllocatedPodGroup
}

type allocatedPodGroupIterator struct {
	pods   []*core.Pod
	index  int
	length int
}

// Next returns the next item in iteration.
func (i *allocatedPodGroupIterator) Next() *core.Pod {
	i.index++
	return i.pods[i.index-1]
}

// HasNext return true if iteration not finishes.
func (i *allocatedPodGroupIterator) HasNext() bool {
	return i.index < i.length
}

// Iterator returns a stateful iterator for AllocatedPodGroup
func (podRootGroup AllocatedPodGroup) Iterator(args ...int32) *allocatedPodGroupIterator {
	index := int32(0)
	pods := []*core.Pod{}
	queue := []*AllocatedPodGroup{&podRootGroup}
	for len(queue) > 0 {
		newQueue := []*AllocatedPodGroup{}
		for _, podGroup := range queue {
			if len(args) == 1 && args[0] == index {
				return &allocatedPodGroupIterator{podGroup.pods, 0, len(podGroup.pods)}
			}
			pods = append(pods, podGroup.pods...)
			index++
			newQueue = append(newQueue, podGroup.allocatedChildGroup...)
		}
		queue = newQueue
	}
	return &allocatedPodGroupIterator{pods, 0, len(pods)}
}

// SetPod sets allocated pod in AllocatedPodGroup.
func (podRootGroup AllocatedPodGroup) SetPod(pod *core.Pod, podGroupIndex int32, podIndex int32) {
	index := int32(0)
	queue := []*AllocatedPodGroup{&podRootGroup}
	for len(queue) > 0 {
		newQueue := []*AllocatedPodGroup{}
		for _, podGroup := range queue {
			if index == podGroupIndex {
				podGroup.pods[podIndex] = pod
			}
			index++
			newQueue = append(newQueue, podGroup.allocatedChildGroup...)
		}
		queue = newQueue
	}
}

// PodGroupPlacement represents a tree structure of intra VC scheduled placement.
type PodGroupPlacement struct {
	podsPlacement        []CellList
	childGroupsPlacement []*PodGroupPlacement
}

// PodGroupPhysicalPlacement represents physical placement of pod group.
type PodGroupPhysicalPlacement PodGroupPlacement

// PodGroupVirtualPlacement represents virtual placement of pod group.
type PodGroupVirtualPlacement PodGroupPlacement

// IsEmpty checks whether PodGroupPlacement is empty
func (placement PodGroupPlacement) IsEmpty() bool {
	return ((placement.podsPlacement == nil || len(placement.podsPlacement) == 0) &&
		(placement.childGroupsPlacement == nil || len(placement.childGroupsPlacement) == 0))
}

type podGroupPlacementIterator struct {
	cellLists []*CellList
	index     int
	length    int
}

// Next returns the next item in iteration.
func (i *podGroupPlacementIterator) Next() *CellList {
	i.index++
	return i.cellLists[i.index-1]
}

// HasNext return true if iteration not finishes.
func (i *podGroupPlacementIterator) HasNext() bool {
	return i.index < i.length
}

// Iterator returns a stateful iterator for PodGroupPlacement
func (placement PodGroupPlacement) Iterator() *podGroupPlacementIterator {
	cellLists := []*CellList{}
	queue := []*PodGroupPlacement{&placement}
	for len(queue) > 0 {
		newQueue := []*PodGroupPlacement{}
		for _, groupPlacement := range queue {
			for podIndex := range groupPlacement.podsPlacement {
				cellLists = append(cellLists, &groupPlacement.podsPlacement[podIndex])
			}
			newQueue = append(newQueue, groupPlacement.childGroupsPlacement...)
		}
		queue = newQueue
	}
	return &podGroupPlacementIterator{cellLists, 0, len(cellLists)}
}

func (physicalPlacement PodGroupPhysicalPlacement) String() string {
	return common.ToJson(physicalPlacement.nodeToLeafCellIndices())
}

func (physicalPlacement PodGroupPhysicalPlacement) nodeToLeafCellIndices() map[string][]int32 {
	nodeToLeafCellIndices := map[string][]int32{}
	for iter := PodGroupPlacement(physicalPlacement).Iterator(); iter.HasNext(); {
		for _, leafCell := range *iter.Next() {
			pLeafCell := leafCell.(*PhysicalCell)
			nodes, leafCellIndices := pLeafCell.GetPhysicalPlacement()
			if _, ok := nodeToLeafCellIndices[nodes[0]]; !ok {
				nodeToLeafCellIndices[nodes[0]] = []int32{}
			}
			nodeToLeafCellIndices[nodes[0]] = append(nodeToLeafCellIndices[nodes[0]], leafCellIndices[0])
		}
	}
	return nodeToLeafCellIndices
}

func (virtualPlacement PodGroupVirtualPlacement) String() string {
	return common.ToJson(virtualPlacement.preassignedCellToLeafCells())
}

func (virtualPlacement PodGroupVirtualPlacement) preassignedCellToLeafCells() map[api.CellAddress][]api.CellAddress {
	preassignedCellToLeafCells := map[api.CellAddress][]api.CellAddress{}
	for iter := PodGroupPlacement(virtualPlacement).Iterator(); iter.HasNext(); {
		for _, leafCell := range *iter.Next() {
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
	return preassignedCellToLeafCells
}

func (virtualPlacement PodGroupVirtualPlacement) toPhysicalPlacement(
	bindings map[api.CellAddress]*PhysicalCell) PodGroupPhysicalPlacement {

	physicalPlacement := PodGroupPhysicalPlacement{}

	virtualPlacementQueue := []*PodGroupPlacement{(*PodGroupPlacement)(&virtualPlacement)}
	physicalPlacementQueue := []*PodGroupPlacement{(*PodGroupPlacement)(&physicalPlacement)}
	for len(virtualPlacementQueue) > 0 {
		newVirtualPlacementQueue := []*PodGroupPlacement{}
		newPhysicalPlacementQueue := []*PodGroupPlacement{}
		for index, placement := range virtualPlacementQueue {
			physicalPlacementQueue[index].podsPlacement = make([]CellList, len(placement.podsPlacement))
			for i, podPlacement := range placement.podsPlacement {
				physicalPlacementQueue[index].podsPlacement[i] = make(CellList, len(podPlacement))
				for j, leafCell := range podPlacement {
					pLeafCell := bindings[leafCell.GetAddress()]
					physicalPlacementQueue[index].podsPlacement[i][j] = pLeafCell
				}
			}
			if placement.childGroupsPlacement != nil {
				physicalPlacementQueue[index].childGroupsPlacement = make([]*PodGroupPlacement, len(placement.childGroupsPlacement))
				for childIndex := range placement.childGroupsPlacement {
					physicalPlacementQueue[index].childGroupsPlacement[childIndex] = &PodGroupPlacement{}
				}
			}
			newVirtualPlacementQueue = append(newVirtualPlacementQueue, virtualPlacementQueue[index].childGroupsPlacement...)
			newPhysicalPlacementQueue = append(newPhysicalPlacementQueue, physicalPlacementQueue[index].childGroupsPlacement...)
		}
		virtualPlacementQueue = newVirtualPlacementQueue
		newPhysicalPlacementQueue = physicalPlacementQueue
	}
	return physicalPlacement
}

// A binding path is a tree consisting of all cells that should be bound for binding a set of
// lowest-level cells in a physical placement. It is generated by collecting all the unbound
// ancestors for these cells and group them in a tree.
func (virtualPlacement PodGroupVirtualPlacement) toBindingPaths(
	bindings map[api.CellAddress]*PhysicalCell) (
	preassignedCells []*cellBindingPathVertex,
	nonPreassignedCells [][]*cellBindingPathVertex) {

	allBindingPathVertices := map[api.CellAddress]*cellBindingPathVertex{}
	for iter := PodGroupPlacement(virtualPlacement).Iterator(); iter.HasNext(); {
		for _, leafCell := range *iter.Next() {
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
	return preassignedCells, nonPreassignedCells
}
