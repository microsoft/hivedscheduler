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
// It stores two maps of ChainCellList, one for reserved cells, the other for non-reserved ones.
// It should be able to return a set of GPU placements in the VC for a scheduling request.
type intraVCScheduler interface {
	getNonReservedFullCellList() map[CellChain]ChainCellList
	getNonReservedFreeCellList() map[CellChain]ChainCellList
	getReservedCellList() map[api.ReservationId]ChainCellList

	// Scheduling an affinity group inside a VC. We use topologyAwareScheduler by default.
	schedule(schedulingRequest) groupVirtualPlacement
}

type defaultIntraVCScheduler struct {
	nonReservedFullCellList map[CellChain]ChainCellList
	nonReservedFreeCellList map[CellChain]ChainCellList
	reservedCellList        map[api.ReservationId]ChainCellList
	// currently we create a topologyAwareScheduler for each cluster view (each chain, each reservation).
	// we plan to support multiple cluster views in one scheduler, and to support schedule pods
	// across different cluster views.
	// TODO: Support an affinity group can relax to be allocated across multiple chains.
	nonReservedSchedulers map[CellChain]*topologyAwareScheduler
	reservedSchedulers    map[api.ReservationId]*topologyAwareScheduler
}

func newDefaultIntraVCScheduler(
	nonReservedFull map[CellChain]ChainCellList,
	nonReservedFree map[CellChain]ChainCellList,
	reservedVcl map[api.ReservationId]ChainCellList,
	gpuNums map[CellChain]map[CellLevel]int32) *defaultIntraVCScheduler {

	snr := map[CellChain]*topologyAwareScheduler{}
	sr := map[api.ReservationId]*topologyAwareScheduler{}
	for chain, ccl := range nonReservedFull {
		snr[chain] = NewTopologyAwareScheduler(ccl, gpuNums[chain], true, false)
	}
	for rid, ccl := range reservedVcl {
		sr[rid] = NewTopologyAwareScheduler(ccl, gpuNums[ccl[CellLevel(1)][0].GetChain()], true, false)
	}
	return &defaultIntraVCScheduler{
		nonReservedFullCellList: nonReservedFull,
		nonReservedFreeCellList: nonReservedFree,
		reservedCellList:        reservedVcl,
		nonReservedSchedulers:   snr,
		reservedSchedulers:      sr,
	}
}

func (s *defaultIntraVCScheduler) getNonReservedFullCellList() map[CellChain]ChainCellList {
	return s.nonReservedFullCellList
}

func (s *defaultIntraVCScheduler) getNonReservedFreeCellList() map[CellChain]ChainCellList {
	return s.nonReservedFreeCellList
}

func (s *defaultIntraVCScheduler) getReservedCellList() map[api.ReservationId]ChainCellList {
	return s.reservedCellList
}

func (s *defaultIntraVCScheduler) schedule(sr schedulingRequest) (placement groupVirtualPlacement) {
	scheduler := s.nonReservedSchedulers[sr.chain]
	str := fmt.Sprintf("chain %v", sr.chain)
	if sr.reservationId != "" {
		scheduler = s.reservedSchedulers[sr.reservationId]
		str = fmt.Sprintf("reservation %v", sr.reservationId)
	}
	klog.Infof("Processing scheduling request in VC %v: %v, GPU numbers %v, priority %v",
		sr.vc, str, common.ToJson(sr.affinityGroupPodNums), sr.priority)
	if scheduler != nil {
		placement = scheduler.Schedule(sr.affinityGroupPodNums, sr.priority, common.NewSet())
	}
	if placement == nil {
		klog.Infof("Cannot find placement in VC %v", sr.vc)
	} else {
		klog.Infof("Found placement in VC %v: %v",
			sr.vc, placement)
	}
	return placement
}
