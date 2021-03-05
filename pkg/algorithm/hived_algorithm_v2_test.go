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
	"net/http"
	"sort"
	"testing"

	"github.com/microsoft/hivedscheduler/pkg/api"
	apiv2 "github.com/microsoft/hivedscheduler/pkg/api/v2"
	"github.com/microsoft/hivedscheduler/pkg/common"
	"github.com/microsoft/hivedscheduler/pkg/internal"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

var allPods = map[string]*core.Pod{}

func init() {
	common.InitAll()
	common.FromYaml(pssData, &pss)
	for i := 1; i <= len(pss); i++ {
		podName := fmt.Sprintf("pod%v", i)
		allPods[podName] = &core.Pod{
			ObjectMeta: meta.ObjectMeta{
				Name:        podName,
				Namespace:   "test",
				UID:         types.UID(podName),
				Annotations: map[string]string{},
			},
		}
	}
}

var allNodes []string

func initNodes(h *HivedAlgorithm) {
	for _, ccl := range h.fullCellList {
		for _, c := range ccl[CellLevel(len(ccl))] {
			allNodes = append(allNodes, c.(*PhysicalCell).nodes...)
		}
	}
}

type pssType map[types.UID]apiv2.PodSchedulingSpec
var pssData = `pod1:
  version: v2
  virtualCluster: VC1
  priority: 0
  pinnedCellId: ""
  cellType: DGX2-V100
  cellNumber: 1
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group1
    withinOneCell: ""
    pods:
    - podMinNumber: 1
      podMaxNumber: 1
      cellsPerPod:
        cellType: DGX2-V100
        cellNumber: 1
      containsCurrentPod: true
    childGroups: []
pod2:
  version: v2
  virtualCluster: VC1
  priority: 1
  pinnedCellId: ""
  cellType: DGX2-V100
  cellNumber: 1
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group2
    withinOneCell: ""
    pods:
    - podMinNumber: 1
      podMaxNumber: 1
      cellsPerPod:
        cellType: DGX2-V100
        cellNumber: 1
      containsCurrentPod: true
    childGroups: []
pod3:
  version: v2
  virtualCluster: VC1
  priority: 2
  pinnedCellId: ""
  cellType: DGX2-V100
  cellNumber: 8
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group3
    withinOneCell: ""
    pods:
    - podMinNumber: 1
      podMaxNumber: 1
      cellsPerPod:
        cellType: DGX2-V100
        cellNumber: 8
      containsCurrentPod: true
    childGroups: []
pod4:
  version: v2
  virtualCluster: VC1
  priority: -1
  pinnedCellId: ""
  cellType: DGX2-V100
  cellNumber: 1
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group4
    withinOneCell: ""
    pods:
    - podMinNumber: 1
      podMaxNumber: 1
      cellsPerPod:
        cellType: DGX2-V100
        cellNumber: 1
      containsCurrentPod: true
    childGroups: []
pod5:
  version: v2
  virtualCluster: VC1
  priority: 1
  pinnedCellId: VC1-YQW-DGX2
  cellType: DGX2-V100
  cellNumber: 16
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group5
    withinOneCell: ""
    pods:
    - podMinNumber: 2
      podMaxNumber: 2
      cellsPerPod:
        cellType: DGX2-V100
        cellNumber: 16
      containsCurrentPod: true
    childGroups: []
pod6:
  version: v2
  virtualCluster: VC1
  priority: 1
  pinnedCellId: VC1-YQW-DGX2
  cellType: DGX2-V100
  cellNumber: 16
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group5
    withinOneCell: ""
    pods:
    - podMinNumber: 2
      podMaxNumber: 2
      cellsPerPod:
        cellType: DGX2-V100
        cellNumber: 16
      containsCurrentPod: true
    childGroups: []
pod7:
  version: v2
  virtualCluster: VC2
  priority: 1
  pinnedCellId: ""
  cellType: DGX1-P100
  cellNumber: 8
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group7
    withinOneCell: ""
    pods:
    - podMinNumber: 3
      podMaxNumber: 3
      cellsPerPod:
        cellType: DGX1-P100
        cellNumber: 8
      containsCurrentPod: true
    childGroups: []
pod8:
  version: v2
  virtualCluster: VC2
  priority: 1
  pinnedCellId: ""
  cellType: ""
  cellNumber: 7
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group9
    withinOneCell: ""
    pods:
    - podMinNumber: 1
      podMaxNumber: 1
      cellsPerPod:
        cellType: ""
        cellNumber: 7
      containsCurrentPod: true
    - podMinNumber: 1
      podMaxNumber: 1
      cellsPerPod:
        cellType: ""
        cellNumber: 5
      containsCurrentPod: false
    childGroups: []
pod9:
  version: v2
  virtualCluster: VC2
  priority: 1
  pinnedCellId: ""
  cellType: ""
  cellNumber: 5
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group9
    withinOneCell: ""
    pods:
    - podMinNumber: 1
      podMaxNumber: 1
      cellsPerPod:
        cellType: ""
        cellNumber: 7
      containsCurrentPod: false
    - podMinNumber: 1
      podMaxNumber: 1
      cellsPerPod:
        cellType: ""
        cellNumber: 5
      containsCurrentPod: true
    childGroups: []
pod10:
  version: v2
  virtualCluster: VC2
  priority: 1
  pinnedCellId: ""
  cellType: DGX2-V100
  cellNumber: 1
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group6
    withinOneCell: ""
    pods:
    - podMinNumber: 1
      podMaxNumber: 1
      cellsPerPod:
        cellType: DGX2-V100
        cellNumber: 1
      containsCurrentPod: true
    childGroups: []
pod11:
  version: v2
  virtualCluster: VC2
  priority: 1
  pinnedCellId: ""
  cellType: DGX1-P100
  cellNumber: 2
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group8
    withinOneCell: ""
    pods:
    - podMinNumber: 1
      podMaxNumber: 1
      cellsPerPod:
        cellType: DGX1-P100
        cellNumber: 8
      containsCurrentPod: false
    childGroups: []
pod12:
  version: v2
  virtualCluster: VC2
  priority: 1
  pinnedCellId: ""
  cellType: DGX1-P100
  cellNumber: 2
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group8
    withinOneCell: ""
    pods:
    - podMinNumber: 1
      podMaxNumber: 1
      cellsPerPod:
        cellType: DGX1-P100
        cellNumber: 8
      containsCurrentPod: false
    childGroups: []
pod13:
  version: v2
  virtualCluster: surprise!
  priority: 1
  pinnedCellId: ""
  cellType: DGX1-P100
  cellNumber: 1
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group10
    withinOneCell: ""
    pods:
    - podMinNumber: 1
      podMaxNumber: 1
      cellsPerPod:
        cellType: DGX1-P100
        cellNumber: 1
      containsCurrentPod: true
    childGroups: []
pod14:
  version: v2
  virtualCluster: VC2
  priority: 1
  pinnedCellId: surprise!
  cellType: DGX1-P100
  cellNumber: 1
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group10
    withinOneCell: ""
    pods:
    - podMinNumber: 1
      podMaxNumber: 1
      cellsPerPod:
        cellType: DGX1-P100
        cellNumber: 1
      containsCurrentPod: true
    childGroups: []
pod15:
  version: v2
  virtualCluster: VC2
  priority: 1001
  pinnedCellId: ""
  cellType: DGX1-P100
  cellNumber: 1
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group10
    withinOneCell: ""
    pods:
    - podMinNumber: 1
      podMaxNumber: 1
      cellsPerPod:
        cellType: DGX1-P100
        cellNumber: 1
      containsCurrentPod: true
    childGroups: []
pod16:
  version: v2
  virtualCluster: VC1
  priority: 2
  pinnedCellId: VC1-YQW-DGX2
  cellType: DGX2-V100
  cellNumber: 16
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group11
    withinOneCell: ""
    pods:
    - podMinNumber: 2
      podMaxNumber: 2
      cellsPerPod:
        cellType: DGX2-V100
        cellNumber: 16
      containsCurrentPod: true
    childGroups: []
pod17:
  version: v2
  virtualCluster: VC1
  priority: 2
  pinnedCellId: VC1-YQW-DGX2
  cellType: DGX2-V100
  cellNumber: 16
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group11
    withinOneCell: ""
    pods:
    - podMinNumber: 2
      podMaxNumber: 2
      cellsPerPod:
        cellType: DGX2-V100
        cellNumber: 16
      containsCurrentPod: true
    childGroups: []
pod18:
  version: v2
  virtualCluster: VC1
  priority: 1
  pinnedCellId: ""
  cellType: DGX2-V100
  cellNumber: 16
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group12
    withinOneCell: ""
    pods:
    - podMinNumber: 2
      podMaxNumber: 2
      cellsPerPod:
        cellType: DGX2-V100
        cellNumber: 16
      containsCurrentPod: true
    childGroups: []
pod19:
  version: v2
  virtualCluster: VC1
  priority: 1
  pinnedCellId: ""
  cellType: DGX2-V100
  cellNumber: 16
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group12
    withinOneCell: ""
    pods:
    - podMinNumber: 2
      podMaxNumber: 2
      cellsPerPod:
        cellType: DGX2-V100
        cellNumber: 16
      containsCurrentPod: true
    childGroups: []
pod20:
  version: v2
  virtualCluster: VC1
  priority: 1
  pinnedCellId: ""
  cellType: DGX2-V100
  cellNumber: 16
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group13
    withinOneCell: ""
    pods:
    - podMinNumber: 2
      podMaxNumber: 2
      cellsPerPod:
        cellType: DGX2-V100
        cellNumber: 16
      containsCurrentPod: true
    childGroups: []
pod21:
  version: v2
  virtualCluster: VC1
  priority: 1
  pinnedCellId: ""
  cellType: DGX2-V100
  cellNumber: 16
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group13
    withinOneCell: ""
    pods:
    - podMinNumber: 2
      podMaxNumber: 2
      cellsPerPod:
        cellType: DGX2-V100
        cellNumber: 16
      containsCurrentPod: true
    childGroups: []
pod22:
  version: v2
  virtualCluster: VC1
  priority: -1
  pinnedCellId: ""
  cellType: DGX2-V100
  cellNumber: 16
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group14
    withinOneCell: ""
    pods:
    - podMinNumber: 2
      podMaxNumber: 2
      cellsPerPod:
        cellType: DGX2-V100
        cellNumber: 16
      containsCurrentPod: true
    childGroups: []
pod23:
  version: v2
  virtualCluster: VC1
  priority: -1
  pinnedCellId: ""
  cellType: DGX2-V100
  cellNumber: 16
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group14
    withinOneCell: ""
    pods:
    - podMinNumber: 2
      podMaxNumber: 2
      cellsPerPod:
        cellType: DGX2-V100
        cellNumber: 16
      containsCurrentPod: true
    childGroups: []
pod24:
  version: v2
  virtualCluster: VC2
  priority: 0
  pinnedCellId: ""
  cellType: CT1
  cellNumber: 2
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group15
    withinOneCell: ""
    pods:
    - podMinNumber: 1
      podMaxNumber: 1
      cellsPerPod:
        cellType: CT1
        cellNumber: 2
      containsCurrentPod: true
    childGroups: []
pod25:
  version: v2
  virtualCluster: VC2
  priority: 1
  pinnedCellId: ""
  cellType: CT1
  cellNumber: 2
  gangReleaseEnable: false
  lazyPreemptionEnable: false
  podRootGroup:
    name: group16
    withinOneCell: ""
    pods:
    - podMinNumber: 1
      podMaxNumber: 1
      cellsPerPod:
        cellType: CT1
        cellNumber: 2
      containsCurrentPod: true
    childGroups: []
pod26:
  version: v2
  virtualCluster: VC2
  priority: 2
  pinnedCellId: ""
  cellType: CT1
  cellNumber: 2
  gangReleaseEnable: false
  lazyPreemptionEnable: false
  podRootGroup:
    name: group17
    withinOneCell: ""
    pods:
    - podMinNumber: 1
      podMaxNumber: 1
      cellsPerPod:
        cellType: CT1
        cellNumber: 2
      containsCurrentPod: true
    childGroups: []
pod27:
  version: v2
  virtualCluster: VC1
  priority: 1
  pinnedCellId: VC1-YQW-DGX2
  cellType: DGX2-V100
  cellNumber: 16
  gangReleaseEnable: false
  lazyPreemptionEnable: false
  podRootGroup:
    name: group18
    withinOneCell: ""
    pods:
    - podMinNumber: 2
      podMaxNumber: 2
      cellsPerPod:
        cellType: DGX2-V100
        cellNumber: 16
      containsCurrentPod: true
    childGroups: []
pod28:
  version: v2
  virtualCluster: VC1
  priority: 1
  pinnedCellId: VC1-YQW-DGX2
  cellType: DGX2-V100
  cellNumber: 16
  gangReleaseEnable: false
  lazyPreemptionEnable: false
  podRootGroup:
    name: group19
    withinOneCell: ""
    pods:
    - podMinNumber: 2
      podMaxNumber: 2
      cellsPerPod:
        cellType: DGX2-V100
        cellNumber: 16
      containsCurrentPod: true
    childGroups: []
pod29:
  version: v2
  virtualCluster: VC1
  priority: 2
  pinnedCellId: VC1-YQW-DGX2
  cellType: DGX2-V100
  cellNumber: 16
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group20
    withinOneCell: ""
    pods:
    - podMinNumber: 1
      podMaxNumber: 1
      cellsPerPod:
        cellType: DGX2-V100
        cellNumber: 16
      containsCurrentPod: true
    childGroups: []
pod30:
  version: v2
  virtualCluster: VC1
  priority: 1
  pinnedCellId: VC1-YQW-DGX2
  cellType: DGX2-V100
  cellNumber: 16
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group21
    withinOneCell: ""
    pods:
    - podMinNumber: 1
      podMaxNumber: 1
      cellsPerPod:
        cellType: DGX2-V100
        cellNumber: 16
      containsCurrentPod: true
    childGroups: []
pod31:
  version: v2
  virtualCluster: VC1
  priority: 2
  pinnedCellId: VC1-YQW-DGX2
  cellType: DGX2-V100
  cellNumber: 16
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group22
    withinOneCell: ""
    pods:
    - podMinNumber: 1
      podMaxNumber: 1
      cellsPerPod:
        cellType: DGX2-V100
        cellNumber: 16
      containsCurrentPod: true
    childGroups: []
pod32:
  version: v2
  virtualCluster: VC1
  priority: 2
  pinnedCellId: VC1-YQW-DGX2
  cellType: DGX2-V100
  cellNumber: 16
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group23
    withinOneCell: ""
    pods:
    - podMinNumber: 1
      podMaxNumber: 1
      cellsPerPod:
        cellType: DGX2-V100
        cellNumber: 16
      containsCurrentPod: true
    childGroups: []
pod33:
  version: v2
  virtualCluster: VC1
  priority: 3
  pinnedCellId: VC1-YQW-DGX2
  cellType: DGX2-V100
  cellNumber: 16
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group24
    withinOneCell: ""
    pods:
    - podMinNumber: 2
      podMaxNumber: 2
      cellsPerPod:
        cellType: DGX2-V100
        cellNumber: 16
      containsCurrentPod: true
    childGroups: []
pod34:
  version: v2
  virtualCluster: VC1
  priority: 4
  pinnedCellId: VC1-YQW-DGX2
  cellType: DGX2-V100
  cellNumber: 16
  gangReleaseEnable: false
  lazyPreemptionEnable: false
  podRootGroup:
    name: group25
    withinOneCell: ""
    pods:
    - podMinNumber: 1
      podMaxNumber: 1
      cellsPerPod:
        cellType: DGX2-V100
        cellNumber: 16
      containsCurrentPod: true
    childGroups: []
pod35:
  version: v2
  virtualCluster: VC1
  priority: 5
  pinnedCellId: VC1-YQW-DGX2
  cellType: DGX2-V100
  cellNumber: 16
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group26
    withinOneCell: ""
    pods:
    - podMinNumber: 2
      podMaxNumber: 2
      cellsPerPod:
        cellType: DGX2-V100
        cellNumber: 16
      containsCurrentPod: true
    childGroups: []
pod36:
  version: v2
  virtualCluster: VC1
  priority: -1
  pinnedCellId: ""
  cellType: ""
  cellNumber: 1
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group1
    withinOneCell: ""
    pods:
    - podMinNumber: 1
      podMaxNumber: 1
      cellsPerPod:
        cellType: ""
        cellNumber: 1
      containsCurrentPod: true
    childGroups: []
pod37:
  version: v2
  virtualCluster: VC1
  priority: 1
  pinnedCellId: VC1-YQW-DGX2
  cellType: DGX2-V100
  cellNumber: 1
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group1
    withinOneCell: ""
    pods:
    - podMinNumber: 1
      podMaxNumber: 1
      cellsPerPod:
        cellType: DGX2-V100
        cellNumber: 1
      containsCurrentPod: true
    childGroups: []
pod38:
  version: v2
  virtualCluster: VC1
  priority: 1
  pinnedCellId: VC1-YQW-DGX2
  cellType: DGX2-V100
  cellNumber: 1
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group2
    withinOneCell: ""
    pods:
    - podMinNumber: 1
      podMaxNumber: 1
      cellsPerPod:
        cellType: DGX2-V100
        cellNumber: 1
      containsCurrentPod: true
    childGroups: []
pod39:
  version: v2
  virtualCluster: VC1
  priority: 1
  pinnedCellId: ""
  cellType: DGX2-V100
  cellNumber: 16
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group27
    withinOneCell: ""
    pods:
    - podMinNumber: 2
      podMaxNumber: 2
      cellsPerPod:
        cellType: DGX2-V100
        cellNumber: 16
      containsCurrentPod: true
    childGroups: []
pod40:
  version: v2
  virtualCluster: VC1
  priority: 1
  pinnedCellId: ""
  cellType: DGX2-V100
  cellNumber: 16
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group28
    withinOneCell: ""
    pods:
    - podMinNumber: 1
      podMaxNumber: 1
      cellsPerPod:
        cellType: DGX2-V100
        cellNumber: 16
      containsCurrentPod: true
    childGroups: []
pod41:
  version: v2
  virtualCluster: VC1
  priority: 2
  pinnedCellId: ""
  cellType: DGX2-V100
  cellNumber: 16
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group29
    withinOneCell: ""
    pods:
    - podMinNumber: 4
      podMaxNumber: 4
      cellsPerPod:
        cellType: DGX2-V100
        cellNumber: 16
      containsCurrentPod: true
    childGroups: []
pod42:
  version: v2
  virtualCluster: VC1
  priority: 0
  pinnedCellId: ""
  cellType: DGX2-V100
  cellNumber: 16
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group30
    withinOneCell: ""
    pods:
    - podMinNumber: 1
      podMaxNumber: 1
      cellsPerPod:
        cellType: DGX2-V100
        cellNumber: 16
      containsCurrentPod: true
    childGroups: []
pod43:
  version: v2
  virtualCluster: VC2
  priority: 0
  pinnedCellId: ""
  cellType: DGX2-V100
  cellNumber: 16
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group31
    withinOneCell: ""
    pods:
    - podMinNumber: 1
      podMaxNumber: 1
      cellsPerPod:
        cellType: DGX2-V100
        cellNumber: 16
      containsCurrentPod: true
    childGroups: []
pod44:
  version: v2
  virtualCluster: VC1
  priority: 0
  pinnedCellId: ""
  cellType: DGX2-V100
  cellNumber: 16
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group32
    withinOneCell: ""
    pods:
    - podMinNumber: 1
      podMaxNumber: 1
      cellsPerPod:
        cellType: DGX2-V100
        cellNumber: 16
      containsCurrentPod: true
    childGroups: []
pod45:
  version: v2
  virtualCluster: VC1
  priority: 0
  pinnedCellId: ""
  cellType: DGX2-V100
  cellNumber: 16
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group33
    withinOneCell: ""
    pods:
    - podMinNumber: 1
      podMaxNumber: 1
      cellsPerPod:
        cellType: DGX2-V100
        cellNumber: 16
      containsCurrentPod: true
    childGroups: []
pod46:
  version: v2
  virtualCluster: VC1
  priority: 0
  pinnedCellId: ""
  cellType: DGX2-V100
  cellNumber: 16
  gangReleaseEnable: false
  lazyPreemptionEnable: true
  podRootGroup:
    name: group34
    withinOneCell: ""
    pods:
    - podMinNumber: 1
      podMaxNumber: 1
      cellsPerPod:
        cellType: DGX2-V100
        cellNumber: 16
      containsCurrentPod: true
    childGroups: []
`

var pss = pssType{}


var casesThatShouldSucceed = []string{
	"pod1", "pod2", "pod3", "pod4", "pod5", "pod6", "pod7", "pod8", "pod9", "pod16", "pod17", "pod18", "pod19", "pod20",
	"pod21", "pod22", "pod23", "pod24", "pod25",
}

var casesThatShouldFail = [][]string{
	{"pod10"}, {"pod11", "pod12"}, {"pod13"}, {"pod14"}, {"pod15"},
}

var casesThatShouldBeLazyPreempted = []string{
	"pod8", "pod9", "pod20", "pod21", "pod24",
}

var casesForStatefulPreemption = []string{
	"pod28", "pod29", "pod30", "pod31", "pod32", "pod33", "pod34", "pod35",
}

type result struct {
	node              string
	leafCellIsolation []int32
}

var expectedBindInfos = map[string]result{
	"pod1":  {node: "0.0.1.0", leafCellIsolation: []int32{0}},
	"pod2":  {node: "0.0.1.0", leafCellIsolation: []int32{1}},
	"pod3":  {node: "0.0.1.0", leafCellIsolation: []int32{8, 9, 10, 11, 12, 13, 14, 15}},
	"pod4":  {node: "0.0.5.0", leafCellIsolation: []int32{0}},
	"pod5":  {node: "0.0.3.0", leafCellIsolation: []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}},
	"pod6":  {node: "0.0.3.1", leafCellIsolation: []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}},
	"pod8":  {node: "1.0.0.0", leafCellIsolation: []int32{1, 3, 4, 7, 0, 2, 6}},
	"pod9":  {node: "1.0.0.2", leafCellIsolation: []int32{0, 1, 2, 3, 4}},
	"pod18": {node: "0.0.3.2", leafCellIsolation: []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}},
	"pod19": {node: "0.0.3.3", leafCellIsolation: []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}},
	"pod20": {node: "0.0.4.0", leafCellIsolation: []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}},
	"pod21": {node: "0.0.4.1", leafCellIsolation: []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}},
	"pod22": {node: "0.0.4.2", leafCellIsolation: []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}},
	"pod23": {node: "0.0.4.3", leafCellIsolation: []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}},
	"pod24": {node: "0.0.0.1", leafCellIsolation: []int32{0, 1}},
	"pod25": {node: "0.0.0.0", leafCellIsolation: []int32{0, 1}},
	"pod28": {node: "0.0.3.0", leafCellIsolation: []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}},
	"pod34": {node: "0.0.3.0", leafCellIsolation: []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}},
	"pod36": {node: "0.0.1.0", leafCellIsolation: []int32{0}},
	"pod37": {node: "0.0.3.0", leafCellIsolation: []int32{0}},
	"pod38": {node: "0.0.3.1", leafCellIsolation: []int32{0}},
	"pod39": {node: "0.0.3.2", leafCellIsolation: []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}},
	"pod40": {node: "0.0.4.3", leafCellIsolation: []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}},
	"pod44": {node: "0.0.3.2", leafCellIsolation: []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}},
	"pod45": {node: "0.0.4.2", leafCellIsolation: []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}},
}

var expectedPreemptInfos = map[string]common.Set{
	"pod16": common.NewSet("pod5", "pod6"),
	"pod17": common.NewSet("pod5", "pod6"),
	"pod26": common.NewSet("pod25"),
	"pod29": common.NewSet("pod28"),
	"pod31": common.NewSet("pod28"),
	"pod33": common.NewSet("pod28"),
	"pod35": common.NewSet("pod34"),
}

var deletedPreemptorGroups = map[string][]string{
	"pod33": {"group20", "group22"},
	"pod34": {"group24"},
	"pod35": {"group26"},
}

var allocatedPods []*core.Pod
var preemptingPods []*core.Pod

func TestHivedAlgorithm(t *testing.T) {
	configFilePath := "../../example/config/design/hivedscheduler.yaml"
	sConfig := api.NewConfig(api.InitRawConfig(&configFilePath))
	h := NewHivedAlgorithm(sConfig)
	initNodes(h)
	// sort chains of each leaf cell type for stability of the test
	for _, chains := range h.cellChains {
		sortChains(chains)
	}
	setHealthyNodes(h)

	printConfig(t, h)
	testNormalOperations(t, h)
	testSuggestedNodes(t, configFilePath)
	testStatefulPreemption(t, configFilePath)
	testBadNodes(t, configFilePath)
	testSafeRelaxedBuddyAlloc(t, configFilePath)
	testReconfiguration(t, configFilePath)
	testInvalidInitialAssignment(t, sConfig)
}

func sortChains(chains []CellChain) {
	var chainsTemp []string
	for _, c := range chains {
		chainsTemp = append(chainsTemp, string(c))
	}
	sort.Strings(chainsTemp)
	for i := range chains {
		chains[i] = CellChain(chainsTemp[len(chainsTemp)-i-1])
	}
}

func setHealthyNodes(h *HivedAlgorithm) {
	for _, ccl := range h.fullCellList {
		for _, c := range ccl[CellLevel(len(ccl))] {
			nodes, _ := c.(*PhysicalCell).GetPhysicalPlacement()
			for _, n := range nodes {
				h.setHealthyNode(n)
			}
		}
	}
}

func printConfig(t *testing.T, h *HivedAlgorithm) {
	for chain, ccl := range h.fullCellList {
		t.Logf("%v", chain)
		t.Logf("%v", ccl)
	}
	for vc, vcs := range h.vcSchedulers {
		t.Logf("%v", vc)
		for chain, ccl := range vcs.getNonPinnedFullCellList() {
			t.Logf("%v", chain)
			t.Logf("%v", ccl)
		}
		t.Logf("Pinned cells")
		for pid, ccl := range vcs.getPinnedCells() {
			t.Logf(string(pid))
			t.Logf("%v", ccl)
		}
	}
	for leafCellType, chains := range h.cellChains {
		t.Logf("%v: %v", leafCellType, chains)
	}
}

func testNormalOperations(t *testing.T, h *HivedAlgorithm) {
	testCasesThatShouldSucceed(t, h)
	testCasesThatShouldFail(t, h)
	testDeletePods(t, h)
}

func testCasesThatShouldSucceed(t *testing.T, h *HivedAlgorithm) {
	allocatedPods = []*core.Pod{}
	preemptingPods = []*core.Pod{}
	var psr internal.PodScheduleResult
	for _, podName := range casesThatShouldSucceed {
		pod := allPods[podName]
		pod.Annotations[api.AnnotationKeyPodSchedulingSpec] = common.ToYaml(pss[pod.UID])
		psr = h.Schedule(pod, allNodes, internal.PreemptingPhase)
		compareSchedulingResult(t, pod, psr)
		if psr.PodBindInfo != nil {
			allocatedPod := internal.NewBindingPod(pod, psr.PodBindInfo)
			h.AddAllocatedPod(allocatedPod)
			allocatedPods = append(allocatedPods, allocatedPod)
		} else if psr.PodPreemptInfo != nil {
			preemptingPods = append(preemptingPods, pod)
		}
	}
}

func testOneCaseThatShouldFail(t *testing.T, h *HivedAlgorithm, podNames []string) {
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(*api.WebServerError); ok &&
				err.Code >= http.StatusBadRequest &&
				err.Code < http.StatusInternalServerError {
				t.Logf("Got User Error Panic as expected: %v", err)
			} else {
				t.Errorf("Expected User Error Panic, but got %v", r)
			}
		} else {
			t.Errorf("Expected User Error Panic, but got none")
		}
	}()
	var psr internal.PodScheduleResult
	for _, podName := range podNames {
		pod := allPods[podName]
		pod.Annotations[api.AnnotationKeyPodSchedulingSpec] = common.ToYaml(pss[pod.UID])
		psr = h.Schedule(pod, allNodes, internal.PreemptingPhase)
		allocatedPod := internal.NewBindingPod(pod, psr.PodBindInfo)
		h.AddAllocatedPod(allocatedPod)
		allocatedPods = append(allocatedPods, allocatedPod)
	}
}

func testCasesThatShouldFail(t *testing.T, h *HivedAlgorithm) {
	for _, pods := range casesThatShouldFail {
		testOneCaseThatShouldFail(t, h, pods)
	}
}

func testDeletePods(t *testing.T, h *HivedAlgorithm) {
	for i := len(allocatedPods) - 1; i >= 0; i-- {
		h.DeleteAllocatedPod(allocatedPods[i])
	}
	for _, pod := range allocatedPods {
		if _, ok := h.podGroups[pss[pod.UID].PodRootGroup.Name]; ok {
			t.Errorf("Group %v is expected to be deleted in scheduler, but not", pss[pod.UID].PodRootGroup.Name)
		}
	}
	for i := len(preemptingPods) - 1; i >= 0; i-- {
		h.DeleteUnallocatedPod(preemptingPods[i])
	}
	for _, pod := range preemptingPods {
		if _, ok := h.podGroups[pss[pod.UID].PodRootGroup.Name]; ok {
			t.Errorf("Group %v is expected to be deleted in scheduler, but not", pss[pod.UID].PodRootGroup.Name)
		}
	}
}

func testSuggestedNodes(t *testing.T, configFilePath string) {
	sConfig := api.NewConfig(api.InitRawConfig(&configFilePath))
	h := NewHivedAlgorithm(sConfig)
	for _, chains := range h.cellChains {
		sortChains(chains)
	}
	setHealthyNodes(h)
	pod := allPods["pod36"]
	pod.Annotations[api.AnnotationKeyPodSchedulingSpec] = common.ToYaml(pss[pod.UID])
	psr := h.Schedule(pod, []string{"0.0.1.0"}, internal.PreemptingPhase)
	compareSchedulingResult(t, pod, psr)

	pod = allPods["pod37"]
	pod.Annotations[api.AnnotationKeyPodSchedulingSpec] = common.ToYaml(pss[pod.UID])
	psr = h.Schedule(pod, []string{"0.0.3.0"}, internal.PreemptingPhase)
	compareSchedulingResult(t, pod, psr)
	allocatedPod := internal.NewBindingPod(pod, psr.PodBindInfo)
	h.AddAllocatedPod(allocatedPod)
	pod = allPods["pod38"]
	pod.Annotations[api.AnnotationKeyPodSchedulingSpec] = common.ToYaml(pss[pod.UID])
	psr = h.Schedule(pod, []string{"0.0.3.1"}, internal.PreemptingPhase)
	compareSchedulingResult(t, pod, psr)
	h.DeleteAllocatedPod(allocatedPod)

	var nodes []string
	for _, node := range allNodes {
		if node != "0.0.3.1" {
			nodes = append(nodes, node)
		}
	}
	pod = allPods["pod27"]
	pod.Annotations[api.AnnotationKeyPodSchedulingSpec] = common.ToYaml(pss[pod.UID])
	psr = h.Schedule(pod, nodes, internal.PreemptingPhase)
	compareSchedulingResult(t, pod, psr)
	nodes = append(nodes, "0.0.3.1")
	pod.Annotations[api.AnnotationKeyPodSchedulingSpec] = common.ToYaml(pss[pod.UID])
	// this time scheduling will succeed
	psr = h.Schedule(pod, nodes, internal.PreemptingPhase)
	h.AddAllocatedPod(internal.NewBindingPod(pod, psr.PodBindInfo))
	pod = allPods["pod33"]
	pod.Annotations[api.AnnotationKeyPodSchedulingSpec] = common.ToYaml(pss[pod.UID])
	psr = h.Schedule(pod, nodes, internal.FilteringPhase)
	// group should not be preempting because this is Filtering phase
	if g := h.podGroups[pss[pod.UID].PodRootGroup.Name]; g != nil {
		t.Errorf("Group %v should not exist but it does", pss[pod.UID].PodRootGroup.Name)
	}
	psr = h.Schedule(pod, nodes[:len(nodes)-1], internal.PreemptingPhase)
	// group should not be preempting because the placement is not fully within Preempting-phase suggested nodes
	if g := h.podGroups[pss[pod.UID].PodRootGroup.Name]; g != nil {
		t.Errorf("Group %v should not exist but it does", pss[pod.UID].PodRootGroup.Name)
	}
	// this time group will be preempting
	psr = h.Schedule(pod, nodes, internal.PreemptingPhase)
	if g := h.podGroups[pss[pod.UID].PodRootGroup.Name]; g == nil {
		t.Errorf("Group %v should be preempting but does not exist",
			pss[pod.UID].PodRootGroup.Name)
	} else if g.state != podGroupPreempting {
		t.Errorf("Group %v should be in Preempting state but not", pss[pod.UID].PodRootGroup.Name)
	}
	psr = h.Schedule(pod, nodes[:len(nodes)-1], internal.PreemptingPhase)
	// group should have been deleted because the placement is not within Preempting-phase suggested nodes
	if g := h.podGroups[pss[pod.UID].PodRootGroup.Name]; g != nil {
		t.Errorf("Group %v should have been deleted, but not", pss[pod.UID].PodRootGroup.Name)
	}

	// test backtracking search for cell binding
	newConfig := api.NewConfig(api.InitRawConfig(&configFilePath))
	(*newConfig.VirtualClusters)["VC1"].VirtualCells[0].CellNumber = 0
	(*newConfig.VirtualClusters)["VC1"].VirtualCells[3].CellNumber = 3
	h = NewHivedAlgorithm(newConfig)
	for _, chains := range h.cellChains {
		sortChains(chains)
	}
	setHealthyNodes(h)
	pod = allPods["pod39"]
	pod.Annotations[api.AnnotationKeyPodSchedulingSpec] = common.ToYaml(pss[pod.UID])
	psr = h.Schedule(pod, []string{"0.0.3.2", "0.0.3.3"}, internal.PreemptingPhase)
	compareSchedulingResult(t, pod, psr)
	h.AddAllocatedPod(internal.NewBindingPod(pod, psr.PodBindInfo))
	pod = allPods["pod40"]
	pod.Annotations[api.AnnotationKeyPodSchedulingSpec] = common.ToYaml(pss[pod.UID])
	psr = h.Schedule(pod, []string{"0.0.4.3"}, internal.PreemptingPhase)
	compareSchedulingResult(t, pod, psr)
	h.AddAllocatedPod(internal.NewBindingPod(pod, psr.PodBindInfo))
	pod = allPods["pod41"]
	pod.Annotations[api.AnnotationKeyPodSchedulingSpec] = common.ToYaml(pss[pod.UID])
	psr = h.Schedule(pod, []string{"0.0.3.2", "0.0.3.3", "0.0.4.3"}, internal.PreemptingPhase)
	// the pod tries to lazy preempt group27 and group28, but is reverted
	if g := h.podGroups["group27"]; g == nil {
		t.Errorf("Group %v should be allocated but does not exist",
			pss[pod.UID].PodRootGroup.Name)
	} else if g.state != podGroupAllocated {
		t.Errorf("Group %v should be in Allocated state but not", "group27")
	}
	if g := h.podGroups["group28"]; g == nil {
		t.Errorf("Group %v should be allocated but does not exist",
			pss[pod.UID].PodRootGroup.Name)
	} else if g.state != podGroupAllocated {
		t.Errorf("Group %v should be in Allocated state but not", "group28")
	}
}

func testStatefulPreemption(t *testing.T, configFilePath string) {
	sConfig := api.NewConfig(api.InitRawConfig(&configFilePath))
	h := NewHivedAlgorithm(sConfig)
	for _, chains := range h.cellChains {
		sortChains(chains)
	}
	setHealthyNodes(h)
	allocatedPods = []*core.Pod{}
	var psr internal.PodScheduleResult
	for _, podName := range casesForStatefulPreemption {
		pod := allPods[podName]
		pod.Annotations[api.AnnotationKeyPodSchedulingSpec] = common.ToYaml(pss[pod.UID])
		psr = h.Schedule(pod, allNodes, internal.PreemptingPhase)
		compareSchedulingResult(t, pod, psr)
		if psr.PodBindInfo != nil {
			allocatedPod := internal.NewBindingPod(pod, psr.PodBindInfo)
			h.AddAllocatedPod(allocatedPod)
			allocatedPods = append(allocatedPods, allocatedPod)
		}
		if podName == "pod33" {
			h.DeleteAllocatedPod(allocatedPods[0])
		}
		if podName == "pod35" {
			p := &groupPhysicalPlacement{}
			*p = h.podGroups[pss[pod.UID].PodRootGroup.Name].physicalLeafCellPlacement
			h.DeleteUnallocatedPod(pod)
			// test correctness of preemption cancellation
			for _, podPlacements := range *p {
				for _, podLeafCells := range podPlacements {
					for _, leafCell := range podLeafCells {
						pLeafCell := leafCell.(*PhysicalCell)
						if pLeafCell.GetState() == cellUsed {
							if int32(pLeafCell.GetPriority()) != pss["pod34"].Priority {
								t.Errorf("Cell %v's priority should be pod34's priority, but is %v",
									pLeafCell.GetAddress(), pLeafCell.GetPriority())
							}
						} else if pLeafCell.GetState() != cellFree {
							t.Errorf("Cell %v should be in Free state, but is %v",
								pLeafCell.GetAddress(), pLeafCell.GetState())
						}
					}
				}
			}
		}
		if deletedGroups := deletedPreemptorGroups[podName]; deletedGroups != nil {
			for _, g := range deletedGroups {
				if _, ok := h.podGroups[g]; ok {
					t.Errorf("Group %v is expected to be deleted in scheduler, but not", g)
				}
			}
		}
	}
}

func testBadNodes(t *testing.T, configFilePath string) {
	sConfig := api.NewConfig(api.InitRawConfig(&configFilePath))
	(*sConfig.VirtualClusters)["VC2"].VirtualCells[2].CellType = "3-DGX2-V100-NODE.DGX2-V100-NODE"
	h := NewHivedAlgorithm(sConfig)
	for _, chains := range h.cellChains {
		sortChains(chains)
	}
	setHealthyNodes(h)
	allocatedPods = []*core.Pod{}

	pod := allPods["pod42"]
	pod.Annotations[api.AnnotationKeyPodSchedulingSpec] = common.ToYaml(pss[pod.UID])
	psr := h.Schedule(pod, []string{"0.0.2.0"}, internal.PreemptingPhase)
	bindingPod := internal.NewBindingPod(pod, psr.PodBindInfo)
	h.AddAllocatedPod(bindingPod)
	allocatedPods = append(allocatedPods, bindingPod)
	h.setBadNode("0.0.2.1")
	for _, c := range h.vcSchedulers["VC1"].getNonPinnedPreassignedCells()["3-DGX2-V100-NODE"][5] {
		if c.(*VirtualCell).GetAPIStatus().CellHealthiness == api.CellBad {
			t.Errorf(
				"All free cells in VC1 chain 3-DGX2-V100-NODE should be healthy, but %v is bad", c.GetAddress())
		}
	}
	for _, c := range h.vcSchedulers["VC2"].getNonPinnedPreassignedCells()["3-DGX2-V100-NODE"][5] {
		if c.(*VirtualCell).GetAPIStatus().CellHealthiness == api.CellBad {
			t.Errorf(
				"All free cells in VC2 chain 3-DGX2-V100-NODE should be healthy, but %v is bad", c.GetAddress())
		}
	}
	pod = allPods["pod43"]
	pod.Annotations[api.AnnotationKeyPodSchedulingSpec] = common.ToYaml(pss[pod.UID])
	psr = h.Schedule(pod, []string{"0.0.2.2"}, internal.PreemptingPhase)
	bindingPod = internal.NewBindingPod(pod, psr.PodBindInfo)
	h.AddAllocatedPod(bindingPod)
	allocatedPods = append(allocatedPods, bindingPod)
	for _, c := range h.vcSchedulers["VC1"].getNonPinnedPreassignedCells()["3-DGX2-V100-NODE"][5] {
		if c.GetPriority() == freePriority && c.(*VirtualCell).GetAPIStatus().CellHealthiness == api.CellHealthy {
			t.Errorf(
				"All free cells in VC1 chain 3-DGX2-V100-NODE should be bad, but %v is healthy", c.GetAddress())
		}
	}
	h.DeleteAllocatedPod(allocatedPods[1])
	for _, c := range h.vcSchedulers["VC1"].getNonPinnedPreassignedCells()["3-DGX2-V100-NODE"][5] {
		if c.(*VirtualCell).GetAPIStatus().CellHealthiness == api.CellBad {
			t.Errorf(
				"All free cells in VC1 chain 3-DGX2-V100-NODE should be healthy, but %v is bad", c.GetAddress())
		}
	}
	h.setBadNode("0.0.2.2")
	for _, c := range h.vcSchedulers["VC1"].getNonPinnedPreassignedCells()["3-DGX2-V100-NODE"][5] {
		if c.GetPriority() == freePriority && c.(*VirtualCell).GetAPIStatus().CellHealthiness == api.CellHealthy {
			t.Errorf(
				"All free cells in VC1 chain 3-DGX2-V100-NODE should be bad, but %v is healthy", c.GetAddress())
		}
	}
	for _, c := range h.vcSchedulers["VC2"].getNonPinnedPreassignedCells()["3-DGX2-V100-NODE"][5] {
		if c.GetPriority() == freePriority && c.(*VirtualCell).GetAPIStatus().CellHealthiness == api.CellHealthy {
			t.Errorf(
				"All free cells in VC2 chain 3-DGX2-V100-NODE should be bad, but %v is healthy", c.GetAddress())
		}
	}
	h.setHealthyNode("0.0.2.2")
	for _, c := range h.vcSchedulers["VC1"].getNonPinnedPreassignedCells()["3-DGX2-V100-NODE"][5] {
		if c.(*VirtualCell).GetAPIStatus().CellHealthiness == api.CellBad {
			t.Errorf(
				"All free cells in VC1 chain 3-DGX2-V100-NODE should be healthy, but %v is bad", c.GetAddress())
		}
	}
	for _, c := range h.vcSchedulers["VC2"].getNonPinnedPreassignedCells()["3-DGX2-V100-NODE"][5] {
		if c.(*VirtualCell).GetAPIStatus().CellHealthiness == api.CellBad {
			t.Errorf(
				"All free cells in VC2 chain 3-DGX2-V100-NODE should be healthy, but %v is bad", c.GetAddress())
		}
	}
	h.setBadNode("0.0.2.0")
	h.setBadNode("0.0.2.2")
	h.DeleteAllocatedPod(allocatedPods[0])
	// after the pod is deleted from 0.0.2.0, the node should still be doomed bad
	for _, c := range h.vcSchedulers["VC1"].getNonPinnedPreassignedCells()["3-DGX2-V100-NODE"][5] {
		if c.(*VirtualCell).GetAPIStatus().CellHealthiness == api.CellHealthy {
			t.Errorf(
				"All free cells in VC1 chain 3-DGX2-V100-NODE should be bad, but %v is healthy", c.GetAddress())
		}
	}
	for _, c := range h.vcSchedulers["VC2"].getNonPinnedPreassignedCells()["3-DGX2-V100-NODE"][5] {
		if c.(*VirtualCell).GetAPIStatus().CellHealthiness == api.CellHealthy {
			t.Errorf(
				"All free cells in VC2 chain 3-DGX2-V100-NODE should be bad, but %v is healthy", c.GetAddress())
		}
	}
}

func testSafeRelaxedBuddyAlloc(t *testing.T, configFilePath string) {
	sConfig := api.NewConfig(api.InitRawConfig(&configFilePath))
	(*sConfig.VirtualClusters)["VC1"].VirtualCells[0].CellNumber = 4
	(*sConfig.VirtualClusters)["VC1"].VirtualCells[2].CellNumber = 0
	(*sConfig.VirtualClusters)["VC1"].VirtualCells[3].CellNumber = 0
	(*sConfig.VirtualClusters)["VC2"].VirtualCells[2].CellType = "4-DGX2-V100-NODE.2-DGX2-V100-NODE"
	(*sConfig.VirtualClusters)["VC2"].VirtualCells[2].CellNumber = 1
	h := NewHivedAlgorithm(sConfig)
	for _, chains := range h.cellChains {
		sortChains(chains)
	}
	setHealthyNodes(h)
	allocatedPods = []*core.Pod{}

	pod := allPods["pod44"]
	pod.Annotations[api.AnnotationKeyPodSchedulingSpec] = common.ToYaml(pss[pod.UID])
	psr := h.Schedule(pod, []string{"0.0.3.2", "0.0.3.3", "0.0.4.2", "0.0.4.3"}, internal.PreemptingPhase)
	bindingPod := internal.NewBindingPod(pod, psr.PodBindInfo)
	h.AddAllocatedPod(bindingPod)
	allocatedPods = append(allocatedPods, bindingPod)
	compareSchedulingResult(t, pod, psr)

	h.setBadNode("0.0.3.3")
	pod = allPods["pod45"]
	pod.Annotations[api.AnnotationKeyPodSchedulingSpec] = common.ToYaml(pss[pod.UID])
	psr = h.Schedule(pod, []string{"0.0.3.2", "0.0.3.3", "0.0.4.2", "0.0.4.3"}, internal.PreemptingPhase)
	if psr.PodBindInfo == nil {
		t.Errorf("Cannot split higher level cells when requested level cell is bad")
	}
	bindingPod = internal.NewBindingPod(pod, psr.PodBindInfo)
	h.AddAllocatedPod(bindingPod)
	allocatedPods = append(allocatedPods, bindingPod)
	compareSchedulingResult(t, pod, psr)

	h.setBadNode("0.0.4.3")
	pod = allPods["pod46"]
	pod.Annotations[api.AnnotationKeyPodSchedulingSpec] = common.ToYaml(pss[pod.UID])
	psr = h.Schedule(pod, []string{"0.0.3.2", "0.0.3.3", "0.0.4.0", "0.0.4.1", "0.0.4.2", "0.0.4.3"}, internal.PreemptingPhase)
	compareSchedulingResult(t, pod, psr)
}

func testReconfiguration(t *testing.T, configFilePath string) {
	oldConfig := api.NewConfig(api.InitRawConfig(&configFilePath))
	h := NewHivedAlgorithm(oldConfig)
	for _, chains := range h.cellChains {
		sortChains(chains)
	}
	setHealthyNodes(h)
	testCasesThatShouldSucceed(t, h)

	newConfig := api.InitRawConfig(&configFilePath)
	// case: shorten cell chain
	(*newConfig.PhysicalCluster).CellTypes["DGX2-V100-NODE"] = api.CellTypeSpec{
		ChildCellType:   "DGX2-V100",
		ChildCellNumber: 16,
		IsNodeLevel:     true,
	}
	newConfig = api.NewConfig(newConfig)
	// case: physical cell not found
	(*newConfig.PhysicalCluster).PhysicalCells[7].CellChildren[0].CellChildren[0].CellAddress = "0.0.3.100"
	// case: insufficient VC cells
	(*newConfig.VirtualClusters)["VC2"].VirtualCells[0].CellNumber = 1
	// case: physical cells are split to smaller ones in the spec so that
	// they cannot be bound to the virtual cells previously allocated
	originalCell := (*newConfig.PhysicalCluster).PhysicalCells[8]
	(*newConfig.PhysicalCluster).PhysicalCells[8] = originalCell.CellChildren[0].CellChildren[0]
	(*newConfig.PhysicalCluster).PhysicalCells = append((*newConfig.PhysicalCluster).PhysicalCells, originalCell.CellChildren[0].CellChildren[1])
	(*newConfig.PhysicalCluster).PhysicalCells = append((*newConfig.PhysicalCluster).PhysicalCells, originalCell.CellChildren[1].CellChildren[0])
	(*newConfig.PhysicalCluster).PhysicalCells = append((*newConfig.PhysicalCluster).PhysicalCells, originalCell.CellChildren[1].CellChildren[1])
	originalCell.CellChildren[0].CellChildren[0].CellAddress = "0.0.4.100"
	originalCell.CellChildren[0].CellChildren[1].CellAddress = "0.0.4.101"
	originalCell.CellChildren[1].CellChildren[0].CellAddress = "0.0.4.102"
	originalCell.CellChildren[1].CellChildren[1].CellAddress = "0.0.4.103"
	(*newConfig.PhysicalCluster).PhysicalCells = append((*newConfig.PhysicalCluster).PhysicalCells, originalCell)

	h = NewHivedAlgorithm(newConfig)
	for _, chains := range h.cellChains {
		sortChains(chains)
	}
	setHealthyNodes(h)
	for _, pod := range allocatedPods {
		h.AddAllocatedPod(pod)
	}
	for _, podName := range casesThatShouldBeLazyPreempted {
		pod := allPods[podName]
		g := h.podGroups[pss[pod.UID].PodRootGroup.Name]
		if g.virtualLeafCellPlacement != nil {
			t.Errorf("Group %v is expected to be lazy preempted, but not", pss[pod.UID].PodRootGroup.Name)
		}
	}
	testDeletePods(t, h)
}

func testInvalidInitialAssignment(t *testing.T, sConfig *api.Config) {
	defer func() {
		if err := recover(); err != nil {
			t.Logf("Initial assignment validation failed as expected: %v", err)
		} else {
			t.Errorf("Expected error in initial assignment validation, but got none")
		}
	}()
	(*sConfig.VirtualClusters)["VC1"].VirtualCells[0].CellType = "CT1-NODE"
	(*sConfig.VirtualClusters)["VC1"].VirtualCells[1].CellType = "CT1-NODE.CT1"
	(*sConfig.VirtualClusters)["VC1"].VirtualCells[1].CellNumber = 2
	NewHivedAlgorithm(sConfig)
}

func compareLeafCellIsolation(a []int32, b []int32) bool {
	if len(a) == len(b) {
		for i := 0; i < len(a); i++ {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}
	return false
}

func containsPods(a []*core.Pod, b common.Set) bool {
	for _, p := range a {
		if !b.Contains(p.Name) {
			return false
		}
	}
	return true
}

func compareSchedulingResult(t *testing.T, pod *core.Pod, psr internal.PodScheduleResult) {
	if expected, ok := expectedBindInfos[pod.Name]; !ok {
		if psr.PodBindInfo != nil {
			t.Errorf("[%v]: wrong pod scheduling result: expected empty, but got %v:%v",
				internal.Key(pod), psr.PodBindInfo.Node, psr.PodBindInfo.LeafCellIsolation)
		}
		if !expectedPreemptInfos[pod.Name].IsEmpty() && !containsPods(psr.PodPreemptInfo.VictimPods, expectedPreemptInfos[pod.Name]) {
			t.Errorf("[%v]: wrong preempt victims: expected %v, but got %v",
				internal.Key(pod), expectedPreemptInfos[pod.Name], psr.PodPreemptInfo.VictimPods)
		}
	} else if psr.PodBindInfo.Node != expected.node ||
		!compareLeafCellIsolation(psr.PodBindInfo.LeafCellIsolation, expected.leafCellIsolation) {
		t.Errorf("[%v]: wrong pod bind info: expected %v:%v, but got %v:%v",
			internal.Key(pod), expected.node, expected.leafCellIsolation, psr.PodBindInfo.Node, psr.PodBindInfo.LeafCellIsolation)
	}
}
