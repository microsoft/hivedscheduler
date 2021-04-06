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
	"sort"

	"github.com/microsoft/hivedscheduler/pkg/api"
	apiv2 "github.com/microsoft/hivedscheduler/pkg/api/v2"
)

// sku cell type for selected level cell in virtual cluster view
type skuCell struct {
	c                             Cell
	freeLeafCellNumAtPriority     int32
	usedLeafCellNumAtPriority     int32
	usedLeafCellNumHigherPriority int32
}

type skuClusterView []*skuCell

type skuScheduler struct {
	ccl               ChainCellList
	levelLeafCellNum  map[CellLevel]int32
	cellLevels        map[api.CellType]CellLevel
	crossPriorityPack bool
}

func NewSkuScheduler(
	ccl ChainCellList,
	levelLeafCellNum map[CellLevel]int32,
	cellLevels map[api.CellType]CellLevel,
	crossPriorityPack bool) *skuScheduler {

	return &skuScheduler{
		ccl:               ccl,
		levelLeafCellNum:  levelLeafCellNum,
		cellLevels:        cellLevels,
		crossPriorityPack: crossPriorityPack,
	}
}

func (s *skuScheduler) Schedule(
	podRootGroup *apiv2.PodGroupSpec,
	p CellPriority) (
	placement PodGroupPlacement,
	failedReason string) {

	// sort pods in descending order by couting leaf cell number
	s.sortPodGroup(podRootGroup)

	// disable preemption first to reduce preemption
	priority := opportunisticPriority
	// try to schedule
	placement, failedReason = s.findCellsForPodGroup(podRootGroup, priority, &skuCell{c: nil}, nil)

	// enable preemption if scheduling failed
	if failedReason != "" && p > priority {
		placement, failedReason = s.findCellsForPodGroup(podRootGroup, p, &skuCell{c: nil}, nil)
	}
	return placement, failedReason
}

func (s *skuScheduler) sortPodGroup(podGroup *apiv2.PodGroupSpec) {
	sort.SliceStable(podGroup.Pods, func(i, j int) bool {
		return s.countLeafCellNums(podGroup.Pods[i]) > s.countLeafCellNums(podGroup.Pods[j])
	})
	sortedPods := []apiv2.PodGroupMemberSpec{}
	for _, p := range podGroup.Pods {
		for i := int32(0); i < p.PodMinNumber; i++ {
			sortedPods = append(sortedPods, p)
		}
	}
	podGroup.Pods = sortedPods

	sort.SliceStable(podGroup.ChildGroups, func(i, j int) bool {
		return s.countLeafCellNums(podGroup.ChildGroups[i]) > s.countLeafCellNums(podGroup.ChildGroups[j])
	})
	for _, g := range podGroup.ChildGroups {
		s.sortPodGroup(g)
	}
}

func (s *skuScheduler) countLeafCellNums(x interface{}) int32 {
	count := int32(0)
	switch p := x.(type) {
	case apiv2.PodGroupMemberSpec:
		count = s.levelLeafCellNum[s.cellLevels[p.CellsPerPod.CellType]] * p.CellsPerPod.CellNumber
	case []apiv2.PodGroupMemberSpec:
		for _, pp := range p {
			count += s.countLeafCellNums(pp)
		}
	case *apiv2.PodGroupSpec:
		count += s.countLeafCellNums(p.Pods) + s.countLeafCellNums(p.ChildGroups)
	case []*apiv2.PodGroupSpec:
		for _, pp := range p {
			count += s.countLeafCellNums(pp)
		}
	}
	return count
}

func (s *skuScheduler) findCellsForPodGroup(
	podGroup *apiv2.PodGroupSpec,
	p CellPriority,
	within *skuCell,
	allocated *PodGroupPlacement) (
	placement PodGroupPlacement,
	failedReason string) {

	placement = PodGroupPlacement{
		podsPlacement:        []CellList{},
		childGroupsPlacement: []*PodGroupPlacement{},
	}
	failedReason = ""

	cv := s.createSkuClusterView(within, s.cellLevels[podGroup.WithinOneCell], p)
	for _, c := range cv {
		placement.podsPlacement, failedReason = s.findCellsForPods(podGroup.Pods, c, allocated)
		if failedReason == "" {
			for _, childGroup := range podGroup.ChildGroups {
				childPodsPlacement, childFailedReason := s.findCellsForPodGroup(childGroup, p, c, &placement)
				if childFailedReason != "" {
					placement.childGroupsPlacement = []*PodGroupPlacement{}
					failedReason = childFailedReason
					break
				}
				placement.childGroupsPlacement = append(placement.childGroupsPlacement, &childPodsPlacement)
			}
			if failedReason == "" {
				break
			}
		}
	}
	return placement, failedReason
}

func (s *skuScheduler) findCellsForPods(
	pods []apiv2.PodGroupMemberSpec,
	within *skuCell,
	allocated *PodGroupPlacement) (
	placement []CellList,
	failedReason string) {

	placement = []CellList{}
	failedReason = ""

	for _, p := range pods {
		// within level, node level
		candidates := getLevelCells(within.c, s.cellLevels[p.CellsPerPod.CellType], CellList{})
		placement = append(placement, candidates[:p.CellsPerPod.CellNumber])
	}

	return placement, failedReason
}

func getLevelCells(c Cell, l CellLevel, freeCells CellList) CellList {
	if c.GetLevel() > l {
		for _, cc := range c.GetChildren() {
			freeCells = getLevelCells(cc, l, freeCells)
		}
	} else if c.GetLevel() == l {
		if c.GetPriority() == freePriority {
			freeCells = append(freeCells, c)
		}
	}
	return freeCells
}

func (s *skuScheduler) createSkuClusterView(
	withinCell *skuCell,
	withinLevel CellLevel,
	p CellPriority) skuClusterView {

	cv := skuClusterView{}
	for l := withinLevel; l >= CellLevel(1); l-- {
		for _, c := range s.ccl[l] {
			if (withinCell.c == nil || isAncestor(withinCell, c)) && !containsAncestor(cv, c) {
				cell := &skuCell{
					c:                             c,
					freeLeafCellNumAtPriority:     c.GetTotalLeafCellNum(),
					usedLeafCellNumAtPriority:     0,
					usedLeafCellNumHigherPriority: 0,
				}
				for priority, num := range c.GetUsedLeafCellNumAtPriorities() {
					if priority >= p {
						cell.freeLeafCellNumAtPriority -= num
					}
					if s.crossPriorityPack {
						cell.usedLeafCellNumAtPriority += num
					} else {
						if priority == p {
							cell.usedLeafCellNumAtPriority += num
						}
						if priority > p {
							cell.usedLeafCellNumHigherPriority += num
						}
					}
				}
				cv = append(cv, cell)
			}
		}
	}
	sort.Stable(cv)
	return cv
}

// Len method for sorting sku cells in cluster view
func (cv skuClusterView) Len() int {
	return len(cv)
}

// Less method for sorting sku cells in cluster view
// sort in the following order:
// 1. cell level (prefer lower)
// 2. usedLeafCellNumAtPriority (prefer higher)
// 3. usedLeafCellNumHigherPriority (prefer lower)
func (cv skuClusterView) Less(i, j int) bool {
	if cv[i].c.GetLevel() != cv[j].c.GetLevel() {
		return cv[i].c.GetLevel() < cv[j].c.GetLevel()
	}
	if cv[i].usedLeafCellNumAtPriority != cv[j].usedLeafCellNumAtPriority {
		return cv[i].usedLeafCellNumAtPriority > cv[j].usedLeafCellNumAtPriority
	}
	if cv[i].usedLeafCellNumHigherPriority != cv[j].usedLeafCellNumHigherPriority {
		return cv[i].usedLeafCellNumHigherPriority < cv[j].usedLeafCellNumHigherPriority
	}
	return true
}

// Swap method for sorting sku cells in cluster view
func (cv skuClusterView) Swap(i int, j int) {
	cv[i], cv[j] = cv[j], cv[i]
}

func isAncestor(ancestor *skuCell, c Cell) bool {
	if CellEqual(ancestor.c, c) {
		return true
	}
	if c.GetParent() == nil {
		return false
	}
	return isAncestor(ancestor, c.GetParent())
}

func containsAncestor(cv skuClusterView, c Cell) bool {
	if c.GetParent() != nil {
		return containsAncestor(cv, c.GetParent())
	}
	for _, skuCell := range cv {
		if CellEqual(c, skuCell.c) {
			return true
		}
	}
	return false
}
