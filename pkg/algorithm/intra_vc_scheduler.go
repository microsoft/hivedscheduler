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
	"k8s.io/klog"
)

// intraVCScheduler is an interface for scheduling pods inside a VC.
// It stores two maps of ChainCellList, one for pinned cells, the other for non-pinned ones.
// It should be able to return a set of GPU placements in the VC for a scheduling request.
type intraVCScheduler interface {
	getNonPinnedFullCellList() map[CellChain]ChainCellList
	getNonPinnedPreassignedCells() map[CellChain]ChainCellList
	getPinnedCells() map[api.PinnedCellId]ChainCellList

	// Schedule an affinity group inside a VC. We use topologyAwareScheduler by default.
	schedule(schedulingRequest) (groupVirtualPlacement, string)
}

type defaultIntraVCScheduler struct {
	nonPinnedFullCellList     map[CellChain]ChainCellList
	nonPinnedPreassignedCells map[CellChain]ChainCellList
	pinnedCells               map[api.PinnedCellId]ChainCellList
	// Currently we create a topologyAwareScheduler for each cluster view (each chain, each pinned cell).
	// We plan to support multiple cluster views in one scheduler, and to support schedule pods
	// across different cluster views.
	// TODO: Support an affinity group can relax to be allocated across multiple chains.
	nonPinnedCellSchedulers map[CellChain]*topologyAwareScheduler
	pinnedCellSchedulers    map[api.PinnedCellId]*topologyAwareScheduler
}

func newDefaultIntraVCScheduler(
	nonPinnedFullList map[CellChain]ChainCellList,
	nonPinnedFreeList map[CellChain]ChainCellList,
	pinnedList map[api.PinnedCellId]ChainCellList,
	gpuNums map[CellChain]map[CellLevel]int32) *defaultIntraVCScheduler {

	snr := map[CellChain]*topologyAwareScheduler{}
	sr := map[api.PinnedCellId]*topologyAwareScheduler{}
	for chain, ccl := range nonPinnedFullList {
		snr[chain] = NewTopologyAwareScheduler(ccl, gpuNums[chain], true)
	}
	for pid, ccl := range pinnedList {
		sr[pid] = NewTopologyAwareScheduler(ccl, gpuNums[ccl[CellLevel(1)][0].GetChain()], true)
	}
	return &defaultIntraVCScheduler{
		nonPinnedFullCellList:     nonPinnedFullList,
		nonPinnedPreassignedCells: nonPinnedFreeList,
		pinnedCells:               pinnedList,
		nonPinnedCellSchedulers:   snr,
		pinnedCellSchedulers:      sr,
	}
}

func (s *defaultIntraVCScheduler) getNonPinnedFullCellList() map[CellChain]ChainCellList {
	return s.nonPinnedFullCellList
}

func (s *defaultIntraVCScheduler) getNonPinnedPreassignedCells() map[CellChain]ChainCellList {
	return s.nonPinnedPreassignedCells
}

func (s *defaultIntraVCScheduler) getPinnedCells() map[api.PinnedCellId]ChainCellList {
	return s.pinnedCells
}

func (s *defaultIntraVCScheduler) schedule(
	sr schedulingRequest) (
	placement groupVirtualPlacement,
	failedReason string) {

	scheduler := s.nonPinnedCellSchedulers[sr.chain]
	str := fmt.Sprintf("chain %v", sr.chain)
	if sr.pinnedCellId != "" {
		scheduler = s.pinnedCellSchedulers[sr.pinnedCellId]
		str = fmt.Sprintf("pinned cell %v", sr.pinnedCellId)
	}
	klog.Infof("Processing scheduling request in VC %v: %v, GPU numbers %v, priority %v",
		sr.vc, str, common.ToJson(sr.affinityGroupPodNums), sr.priority)
	if scheduler != nil {
		placement, failedReason = scheduler.Schedule(
			sr.affinityGroupPodNums,
			sr.priority,
			sr.suggestedNodes,
			sr.ignoreSuggestedNodes)
	}
	if placement == nil {
		return nil, fmt.Sprintf("%v when scheduling in VC %v", failedReason, sr.vc)
	}
	klog.Infof("Found placement in VC %v: %v", sr.vc, placement)
	return placement, ""
}
