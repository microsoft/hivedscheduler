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
	crossPriorityPack bool
}

func NewSkuScheduler(
	ccl ChainCellList,
	levelLeafCellNum map[CellLevel]int32,
	crossPriorityPack bool) *skuScheduler {

	return &skuScheduler{
		ccl:               ccl,
		levelLeafCellNum:  levelLeafCellNum,
		crossPriorityPack: crossPriorityPack,
	}
}

func (s *skuScheduler) SkuSchedule(
	podRootGroup *apiv2.PodGroupSpec,
	p CellPriority) (
	placement podGroupPlacement,
	failedReason string) {

	// sort pods in descending order by couting leaf cell number
	sortPodGroup(s.levelLeafCellNum, podRootGroup)

	// disable preemption first to reduce preemption
	priority := opportunisticPriority
	// try to schedule
	placement, failedReason = findCellsForPodGroup(s.ccl, podRootGroup, nil, nil, priority, s.crossPriorityPack)

	// enable preemption if scheduling failed
	if failedReason != "" && p > priority {
		placement, failedReason = findCellsForPodGroup(s.ccl, podRootGroup, nil, nil, p, s.crossPriorityPack)
	}
	return placement, failedReason
}

func sortPodGroup(levelLeafCellNum map[CellLevel]int32, podGroup *apiv2.PodGroupSpec) {
	sort.SliceStable(podGroup.Pods, func(i, j int) bool {
		return countLeafCellNums(levelLeafCellNum, podGroup.Pods[i]) > countLeafCellNums(levelLeafCellNum, podGroup.Pods[j])
	})
	sortedPods := []apiv2.PodGroupMemberSpec{}
	for _, p := range podGroup.Pods {
		for i := int32(0); i < p.PodMinNumber; i++ {
			sortedPods = append(sortedPods, p)
		}
	}
	podGroup.Pods = sortedPods

	sort.SliceStable(podGroup.ChildGroups, func(i, j int) bool {
		return countLeafCellNums(levelLeafCellNum, podGroup.ChildGroups[i]) > countLeafCellNums(levelLeafCellNum, podGroup.ChildGroups[j])
	})
	for _, g := range podGroup.ChildGroups {
		sortPodGroup(levelLeafCellNum, g)
	}
}

func countLeafCellNums(levelLeafCellNum map[CellLevel]int32, x interface{}) int32 {
	count := int32(0)
	switch p := x.(type) {
	case apiv2.PodGroupMemberSpec:
		count = levelLeafCellNum[cellTypeToLevel(p.CellsPerPod.CellType)] * p.CellsPerPod.CellNumber
	case []apiv2.PodGroupMemberSpec:
		for _, pp := range p {
			count += countLeafCellNums(levelLeafCellNum, pp)
		}
	case *apiv2.PodGroupSpec:
		count += countLeafCellNums(levelLeafCellNum, p.Pods) + countLeafCellNums(levelLeafCellNum, p.ChildGroups)
	case []*apiv2.PodGroupSpec:
		for _, pp := range p {
			count += countLeafCellNums(levelLeafCellNum, pp)
		}
	}
	return count
}

func findCellsForPodGroup(
	ccl ChainCellList,
	podGroup *apiv2.PodGroupSpec,
	within *skuCell,
	allocated *podGroupPlacement,
	p CellPriority,
	crossPriorityPack bool) (
	placement podGroupPlacement,
	failedReason string) {

	placement = podGroupPlacement{
		podsPlacement:        []CellList{},
		childGroupsPlacement: []*podGroupPlacement{},
	}
	failedReason = ""

	cv := newSkuClusterView(ccl, within, cellTypeToLevel(podGroup.WithinOneCell), p, crossPriorityPack)
	for _, c := range cv {
		placement.podsPlacement, failedReason = findCellsForPods(podGroup.Pods, c, allocated)
		if failedReason == "" {
			for _, childGroup := range podGroup.ChildGroups {
				childPodsPlacement, childFailedReason := findCellsForPodGroup(ccl, childGroup, c, &placement, p, crossPriorityPack)
				if childFailedReason != "" {
					placement.childGroupsPlacement = []*podGroupPlacement{}
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

func findCellsForPods(
	pods []apiv2.PodGroupMemberSpec,
	within *skuCell,
	allocated *podGroupPlacement) (
	placement []CellList,
	failedReason string) {

	placement = []CellList{}
	failedReason = ""

	for _, p := range pods {
		// within level, node level
		candidates := getLevelCells(within.c, cellTypeToLevel(p.CellsPerPod.CellType), CellList{})
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

func newSkuClusterView(
	ccl ChainCellList,
	within *skuCell,
	level CellLevel,
	p CellPriority,
	crossPriorityPack bool) skuClusterView {

	cv := skuClusterView{}
	for l := level; l >= CellLevel(1); l-- {
		for _, c := range ccl[l] {
			if (within.c == nil || isAncestor(within, c)) && !containsAncestor(cv, c) {
				skuCell := &skuCell{
					c:                             c,
					freeLeafCellNumAtPriority:     c.GetTotalLeafCellNum(),
					usedLeafCellNumAtPriority:     0,
					usedLeafCellNumHigherPriority: 0,
				}
				for priority, num := range c.GetUsedLeafCellNumAtPriorities() {
					if priority >= p {
						skuCell.freeLeafCellNumAtPriority -= num
					}
					if crossPriorityPack {
						skuCell.usedLeafCellNumAtPriority += num
					} else {
						if priority == p {
							skuCell.usedLeafCellNumAtPriority += num
						}
						if priority > p {
							skuCell.usedLeafCellNumHigherPriority += num
						}
					}
				}
				cv = append(cv, skuCell)
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

func cellTypeToLevel(cellType string) CellLevel {
	// TODO
	return CellLevel(1)
}
