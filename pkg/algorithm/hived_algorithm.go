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
	"github.com/microsoft/hivedscheduler/pkg/internal"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog"
	"sync"
)

// HivedAlgorithm implements an internal.SchedulerAlgorithm. It schedules affinity groups using the algorithm of HiveD.
// Note that the topologyAwareScheduler used in this struct is not another implementation of SchedulerAlgorithm;
// that is a specific algorithm for pod placement, used in intra-VC scheduling and opportunistic pod scheduling.
type HivedAlgorithm struct {
	// scheduler in each VC
	vcSchedulers map[api.VirtualClusterName]intraVCScheduler
	// scheduler for opportunistic pods
	opportunisticSchedulers map[CellChain]*topologyAwareScheduler
	// ChainCellLists of physical cells of each cell chain (including the children of the free cells)
	fullCellList map[CellChain]ChainCellList
	// ChainCellLists of free physical cells of each cell chain (used in buddy alloc)
	freeCellList map[CellChain]ChainCellList
	// all affinity groups that have been allocated cells
	allocatedAffinityGroups map[string]*AlgoAffinityGroup
	// all affinity groups that are preempting other groups
	preemptingAffinityGroups map[string]*AlgoAffinityGroup

	// vcFreeCellNum, allVCFreeCellNum, and totalLeftCellNum are used to track cell usage of the VCs.
	// Note that these numbers count both healthy and bad cells.

	// number of free preassigned cells of each VC of each cell type
	vcFreeCellNum map[api.VirtualClusterName]map[CellChain]map[CellLevel]int32
	// total number of preassigned free cells in all the VCs of each cell type
	allVCFreeCellNum map[CellChain]map[CellLevel]int32
	// number of cells left in the physical cluster of each cell type
	// (i.e., the cell is either in the free list or could be obtained by splitting such a free cell.)
	totalLeftCellNum map[CellChain]map[CellLevel]int32

	// number of bad free cells in the physical cluster of each cell type
	badFreeCellNum map[CellChain]map[CellLevel]int32
	// bad nodes in the physical cluster
	badNodes common.Set
	// map each GPU type to all chains that contain this type
	chains map[string][]CellChain
	// map each level in a chain to the specific cell type name
	cellTypes map[CellChain]map[CellLevel]api.CellType
	// cluster status exposed to external
	apiClusterStatus api.ClusterStatus
	// lock
	algorithmLock sync.RWMutex
}

// NewHivedAlgorithm initializes a HivedAlgorithm from the config file.
func NewHivedAlgorithm(sConfig *api.Config) *HivedAlgorithm {
	fullPcl, freePcl, vcFreeCellNum, nonReservedFullVcl, nonReservedFreeVcl, reservedVcl, reservedPc,
		gpuNums, gpuTypeToChain, cellLevelToType := ParseConfig(sConfig)

	h := &HivedAlgorithm{
		vcSchedulers:             map[api.VirtualClusterName]intraVCScheduler{},
		opportunisticSchedulers:  map[CellChain]*topologyAwareScheduler{},
		fullCellList:             fullPcl,
		freeCellList:             freePcl,
		vcFreeCellNum:            vcFreeCellNum,
		totalLeftCellNum:         map[CellChain]map[CellLevel]int32{},
		badFreeCellNum:           map[CellChain]map[CellLevel]int32{},
		badNodes:                 common.NewSet(),
		chains:                   gpuTypeToChain,
		cellTypes:                cellLevelToType,
		allocatedAffinityGroups:  map[string]*AlgoAffinityGroup{},
		preemptingAffinityGroups: map[string]*AlgoAffinityGroup{},
		apiClusterStatus: api.ClusterStatus{
			PhysicalCluster: api.PhysicalClusterStatus{},
			VirtualClusters: map[api.VirtualClusterName]api.VirtualClusterStatus{},
		},
	}
	for vcName := range nonReservedFullVcl {
		// TODO: Support per-VC configurable intra VC scheduling algo.
		h.vcSchedulers[vcName] = newDefaultIntraVCScheduler(
			nonReservedFullVcl[vcName], nonReservedFreeVcl[vcName], reservedVcl[vcName], gpuNums)
	}
	for chain, ccl := range h.fullCellList {
		h.opportunisticSchedulers[chain] = NewTopologyAwareScheduler(ccl, gpuNums[chain], false, true)
	}
	h.validateInitialAssignment()
	h.initAPIClusterStatus()
	h.initReservations(reservedPc)
	h.initBadNodes()
	return h
}

func (h *HivedAlgorithm) AddNode(node *core.Node) {
	h.algorithmLock.Lock()
	defer h.algorithmLock.Unlock()

	if !internal.IsNodeHealthy(node) {
		// adding a bad node
		h.setBadNode(node.Name)
	} else {
		// possibly a bad node comes back again
		h.setHealthyNode(node.Name)
	}
}

func (h *HivedAlgorithm) UpdateNode(oldNode, newNode *core.Node) {
	h.algorithmLock.Lock()
	defer h.algorithmLock.Unlock()

	if oldHealthy := internal.IsNodeHealthy(oldNode); oldHealthy != internal.IsNodeHealthy(newNode) {
		if oldHealthy {
			h.setBadNode(newNode.Name)
		} else {
			h.setHealthyNode(newNode.Name)
		}
	}
}

func (h *HivedAlgorithm) DeleteNode(node *core.Node) {
	h.algorithmLock.Lock()
	defer h.algorithmLock.Unlock()

	h.setBadNode(node.Name)
}

func (h *HivedAlgorithm) Schedule(pod *core.Pod, suggestedNodes []string) internal.PodScheduleResult {
	h.algorithmLock.Lock()
	defer h.algorithmLock.Unlock()

	klog.Infof("[%v]: Scheduling pod...", internal.Key(pod))
	s := internal.ExtractPodSchedulingSpec(pod)
	suggestedNodeSet := common.NewSet()
	for _, n := range suggestedNodes {
		suggestedNodeSet.Add(n)
	}
	var (
		groupPhysicalPlacement groupPhysicalPlacement // GPU number -> a set of pods -> a set of GPUs of each pod
		groupVirtualPlacement  groupVirtualPlacement  // GPU number -> a set of pods -> a set of GPUs of each pod
		preemptionVictims      map[string]common.Set  // node -> pods
		podIndex               int32                  // index of among the pods with the same GPU number in the group, 0 by default
	)

	if g := h.preemptingAffinityGroups[s.AffinityGroup.Name]; g != nil {
		klog.Infof("[%v]: Pod from preempting affinity group: %v", internal.Key(pod), s.AffinityGroup.Name)
		groupPhysicalPlacement = g.physicalGpuPlacement
		groupVirtualPlacement = g.virtualGpuPlacement
		preemptionVictims = collectPreemptionVictims(groupPhysicalPlacement)
	} else if g = h.allocatedAffinityGroups[s.AffinityGroup.Name]; g != nil {
		klog.Infof("[%v]: Pod from allocated affinity group: %v", internal.Key(pod), s.AffinityGroup.Name)
		groupPhysicalPlacement = g.physicalGpuPlacement
		groupVirtualPlacement = g.virtualGpuPlacement
		if podIndex = getNewPodIndex(g.allocatedPods[s.GpuNumber]); podIndex == -1 {
			panic(internal.NewBadRequestError(fmt.Sprintf(
				"Requesting more pods than the configured number for %v GPUs (%v pods) in affinity group %v",
				s.GpuNumber, g.totalPodNums[s.GpuNumber], s.AffinityGroup.Name)))
		}
	} else {
		klog.Infof("[%v]: Scheduling new affinity group %v", internal.Key(pod), s.AffinityGroup.Name)
		groupPhysicalPlacement, groupVirtualPlacement = h.scheduleNewAffinityGroup(pod, s, suggestedNodeSet)
		if preemptionVictims = collectPreemptionVictims(groupPhysicalPlacement); len(preemptionVictims) != 0 {
			h.createPreemptingAffinityGroup(s, groupPhysicalPlacement, groupVirtualPlacement, pod)
		}
	}

	return generatePodScheduleResult(
		groupPhysicalPlacement,
		groupVirtualPlacement,
		CellPriority(s.Priority),
		preemptionVictims,
		h.cellTypes,
		s.GpuNumber,
		podIndex,
		h.allocatedAffinityGroups[s.AffinityGroup.Name],
		s.AffinityGroup.Name,
		suggestedNodeSet,
		s.VirtualCluster,
		pod)
}

func (h *HivedAlgorithm) AddUnallocatedPod(pod *core.Pod) {
}

func (h *HivedAlgorithm) DeleteUnallocatedPod(pod *core.Pod) {
}

func (h *HivedAlgorithm) AddAllocatedPod(pod *core.Pod) {
	h.algorithmLock.Lock()
	defer h.algorithmLock.Unlock()

	klog.Infof("[%v]: Adding allocated pod...", internal.Key(pod))
	s := internal.ExtractPodSchedulingSpec(pod)
	info := internal.ExtractPodBindInfo(pod)
	klog.Infof("[%v]: Adding to node %v, GPUs %v", internal.Key(pod), info.Node, common.ToJson(info.GpuIsolation))

	if g := h.preemptingAffinityGroups[s.AffinityGroup.Name]; g != nil {
		h.preemptingAffinityGroupToAllocated(g, pod)
	}
	podIndex := int32(0)
	if g := h.allocatedAffinityGroups[s.AffinityGroup.Name]; g == nil {
		h.createAllocatedAffinityGroup(s, info, pod)
	} else {
		if podIndex = getAllocatedPodIndex(info, s.GpuNumber); podIndex == -1 {
			klog.Errorf("[%v]: Pod placement not found in group %v: node %v, GPUs %v",
				internal.Key(pod), s.AffinityGroup.Name, info.Node, info.GpuIsolation)
			return
		}
	}
	h.allocatedAffinityGroups[s.AffinityGroup.Name].allocatedPods[s.GpuNumber][podIndex] = pod
}

func (h *HivedAlgorithm) DeleteAllocatedPod(pod *core.Pod) {
	h.algorithmLock.Lock()
	defer h.algorithmLock.Unlock()

	klog.Infof("[%v]: Deleting allocated pod...", internal.Key(pod))
	s := internal.ExtractPodSchedulingSpec(pod)
	info := internal.ExtractPodBindInfo(pod)
	klog.Infof("[%v]: Deleting from node %v, GPUs %v", internal.Key(pod), info.Node, common.ToJson(info.GpuIsolation))

	if g := h.allocatedAffinityGroups[s.AffinityGroup.Name]; g == nil {
		klog.Errorf("[%v]: Group %v not found when deleting pod", internal.Key(pod), s.AffinityGroup.Name)
		return
	} else {
		if podIndex := getAllocatedPodIndex(info, s.GpuNumber); podIndex == -1 {
			klog.Errorf("[%v]: Pod placement not found in group %v: node %v, GPUs %v",
				internal.Key(pod), s.AffinityGroup.Name, info.Node, info.GpuIsolation)
			return
		} else {
			g.allocatedPods[s.GpuNumber][podIndex] = nil
		}
		if allPodsReleased(g.allocatedPods) {
			h.deleteAllocatedAffinityGroup(g, pod)
		}
	}
}

func (h *HivedAlgorithm) GetAllAffinityGroups() api.AffinityGroupList {
	h.algorithmLock.RLock()
	defer h.algorithmLock.RUnlock()

	ags := api.AffinityGroupList{}
	for _, aag := range h.allocatedAffinityGroups {
		ags.Items = append(ags.Items, aag.ToAffinityGroup())
	}

	return ags
}

func (h *HivedAlgorithm) GetAffinityGroup(name string) api.AffinityGroup {
	h.algorithmLock.RLock()
	defer h.algorithmLock.RUnlock()

	if aag := h.allocatedAffinityGroups[name]; aag != nil {
		return aag.ToAffinityGroup()
	}

	panic(internal.NewBadRequestError(fmt.Sprintf(
		"Affinity group %v does not exist since it is not allocated",
		name)))
}

func (h *HivedAlgorithm) GetClusterStatus() api.ClusterStatus {
	h.algorithmLock.RLock()
	defer h.algorithmLock.RUnlock()

	s := api.ClusterStatus{
		PhysicalCluster: h.apiClusterStatus.PhysicalCluster.DeepCopy(),
		VirtualClusters: map[api.VirtualClusterName]api.VirtualClusterStatus{},
	}
	for vcn, vcs := range h.apiClusterStatus.VirtualClusters {
		s.VirtualClusters[vcn] = vcs.DeepCopy()
	}
	return s
}

func (h *HivedAlgorithm) GetPhysicalClusterStatus() api.PhysicalClusterStatus {
	h.algorithmLock.RLock()
	defer h.algorithmLock.RUnlock()

	return h.apiClusterStatus.PhysicalCluster.DeepCopy()
}

func (h *HivedAlgorithm) GetAllVirtualClustersStatus() map[api.VirtualClusterName]api.VirtualClusterStatus {
	h.algorithmLock.RLock()
	defer h.algorithmLock.RUnlock()

	allVcs := map[api.VirtualClusterName]api.VirtualClusterStatus{}
	for vcn, vcs := range h.apiClusterStatus.VirtualClusters {
		allVcs[vcn] = vcs.DeepCopy()
	}
	return allVcs
}

func (h *HivedAlgorithm) GetVirtualClusterStatus(vcn api.VirtualClusterName) api.VirtualClusterStatus {
	h.algorithmLock.RLock()
	defer h.algorithmLock.RUnlock()

	return h.apiClusterStatus.VirtualClusters[vcn].DeepCopy()
}

// validateInitialAssignment makes sure that the initial cell assignments
// to all VCs can be fit into the configured physical cells.
// This method also initiates the totalLeftCellNum and badFreeCellNum structures.
func (h *HivedAlgorithm) validateInitialAssignment() {

	h.allVCFreeCellNum = map[CellChain]map[CellLevel]int32{}
	for _, vcFreeCellNum := range h.vcFreeCellNum {
		for chain, chainFreeCellNum := range vcFreeCellNum {
			if h.allVCFreeCellNum[chain] == nil {
				h.allVCFreeCellNum[chain] = map[CellLevel]int32{}
			}
			for level, levelFreeCellNum := range chainFreeCellNum {
				h.allVCFreeCellNum[chain][level] += levelFreeCellNum
			}
		}
	}
	for chain, chainFreeCellNum := range h.allVCFreeCellNum {
		if ccl := h.fullCellList[chain]; ccl == nil {
			panic(fmt.Sprintf(
				"Illegal initial VC assignment: Chain %v does not exists in physical cluster", chain))
		} else {
			top := CellLevel(len(ccl))
			available := int32(len(ccl[top]))
			h.totalLeftCellNum[chain] = map[CellLevel]int32{}
			h.badFreeCellNum[chain] = map[CellLevel]int32{}
			h.totalLeftCellNum[chain][top] = available
			h.badFreeCellNum[chain][top] = 0
			for l := top; l >= lowestLevel; l-- {
				left := available - chainFreeCellNum[l]
				if left < 0 {
					panic(fmt.Sprintf(
						"Illegal initial VC assignment: "+
							"Insufficient physical cells at chain %v level %v: %v needed, %v available",
						chain, l, chainFreeCellNum[l], available))
				}
				if l > lowestLevel {
					childNum := int32(len(ccl[l][0].GetChildren()))
					available = left * childNum
					h.totalLeftCellNum[chain][l-1] = h.totalLeftCellNum[chain][l] * childNum
					h.badFreeCellNum[chain][l-1] = 0
				}
			}
		}
	}
}

// initAPIClusterStatus initiates the status of the physical cluster and the VCs that will be exposed to users.
func (h *HivedAlgorithm) initAPIClusterStatus() {

	for _, ccl := range h.fullCellList {
		for _, c := range ccl[CellLevel(len(ccl))] {
			h.apiClusterStatus.PhysicalCluster = append(h.apiClusterStatus.PhysicalCluster, c.(*PhysicalCell).GetAPIStatus())
		}
	}
	for vc, vcs := range h.vcSchedulers {
		h.apiClusterStatus.VirtualClusters[vc] = []*api.VirtualCellStatus{}
		for _, ccl := range vcs.getNonReservedFullCellList() {
			for _, cl := range ccl {
				for _, c := range cl {
					if CellEqual(c, c.(*VirtualCell).GetPreAssignedCell()) {
						h.apiClusterStatus.VirtualClusters[vc] = append(
							h.apiClusterStatus.VirtualClusters[vc], c.(*VirtualCell).GetAPIStatus())
					}
				}
			}
		}
		for _, ccl := range vcs.getReservedCellList() {
			for _, c := range ccl[CellLevel(len(ccl))] {
				h.apiClusterStatus.VirtualClusters[vc] = append(
					h.apiClusterStatus.VirtualClusters[vc], c.(*VirtualCell).GetAPIStatus())
			}
		}
	}
}

// initReservations creates static bindings for the reserved cells, and removes the
// reserved physical cells from the free cell list.
func (h *HivedAlgorithm) initReservations(reservedCells map[api.VirtualClusterName]map[api.ReservationId]*PhysicalCell) {
	for vcn, vcReservation := range reservedCells {
		for rid, physical := range vcReservation {
			h.vcFreeCellNum[vcn][physical.GetChain()][physical.GetLevel()]--
			h.allVCFreeCellNum[physical.GetChain()][physical.GetLevel()]--
			h.removeCellFromFreeList(physical)
			virtualList := h.vcSchedulers[vcn].getReservedCellList()[rid]
			virtual := virtualList[CellLevel(len(virtualList))][0].(*VirtualCell)
			bindCell(physical, virtual)
		}
	}
}

// initBadNodes marks all the physical nodes defined in the config as bad,
// and wait for K8s's AddNode calls to inform the healthy nodes in them.
func (h *HivedAlgorithm) initBadNodes() {
	klog.Info("Init all nodes defined in the config to bad first, " +
		"and wait for K8s to inform the healthy nodes (addNode)")
	for _, ccl := range h.fullCellList {
		for _, c := range ccl[CellLevel(len(ccl))] {
			nodes, _ := c.(*PhysicalCell).GetPhysicalPlacement()
			for _, n := range nodes {
				h.setBadNode(n)
			}
		}
	}
}

// setBadNode marks a node and the cells in it as bad.
func (h *HivedAlgorithm) setBadNode(nodeName string) {
	if h.badNodes.Contains(nodeName) {
		return
	}
	h.badNodes.Add(nodeName)
	for _, ccl := range h.fullCellList {
		for _, gpu := range ccl[1] {
			pGpu := gpu.(*PhysicalCell)
			nodes, _ := pGpu.GetPhysicalPlacement()
			if nodes[0] == nodeName {
				h.setBadCell(pGpu)
			}
		}
	}
}

// setBadNode marks a node and the cells in it as healthy.
func (h *HivedAlgorithm) setHealthyNode(nodeName string) {
	if !h.badNodes.Contains(nodeName) {
		return
	}
	h.badNodes.Delete(nodeName)
	for _, ccl := range h.fullCellList {
		for _, gpu := range ccl[1] {
			pGpu := gpu.(*PhysicalCell)
			nodes, _ := pGpu.GetPhysicalPlacement()
			if nodes[0] == nodeName {
				h.setHealthyCell(pGpu)
			}
		}
	}
}

// setBadCell marks a physical cell as bad and recursively for its parent, guaranteeing that
// a cell is bad if all of its children are bad. The virtual cell that this physical cell
// is bound to will also be marked as bad. If the cell has not been bound to a virtual cell,
// but the scheduler finds that it causes a VC's free cell doomed to be bad,
// one of the cell in that VC will be marked as bad (the first free cell of this type in the VC).
func (h *HivedAlgorithm) setBadCell(c *PhysicalCell) {
	klog.Infof("Cell %v is set to bad", c.GetAddress())
	c.GetAPIStatus().CellHealthiness = api.CellBad
	if c.virtualCell != nil {
		c.GetAPIStatus().VirtualCell.CellHealthiness = api.CellBad
		c.virtualCell.GetAPIStatus().CellHealthiness = api.CellBad
		c.virtualCell.GetAPIStatus().PhysicalCell.CellHealthiness = api.CellBad
	}
	chain, level := c.GetChain(), c.GetLevel()
	if inFreeCellList(c) {
		h.badFreeCellNum[chain][level]++
		if h.allVCFreeCellNum[chain][level] > h.totalLeftCellNum[chain][level]-h.badFreeCellNum[chain][level] {
			klog.Warningf("Cell type %v (chain %v level %v) now has fewer healthy cells (healthy %v, bad %v) "+
				"than the total free cells of all the VCs (%v). Certain VCs' cells may be doomed to be bad.",
				h.cellTypes[chain][level], chain, level, h.totalLeftCellNum[chain][level]-h.badFreeCellNum[chain][level],
				h.badFreeCellNum[chain][level], h.allVCFreeCellNum[chain][level])
			for vcName, vcFreeCellNum := range h.vcFreeCellNum {
				if vcFreeCellNum[chain][level] > h.totalLeftCellNum[chain][level]-h.badFreeCellNum[chain][level] {
					klog.Warningf("Cell type %v (chain %v level %v) now has fewer healthy cells (healthy %v, bad %v) "+
						"than the free cells of the VC %v (%v). Certain cells in the VC are doomed to be bad.",
						h.cellTypes[chain][level], chain, level,
						h.totalLeftCellNum[chain][level]-h.badFreeCellNum[chain][level], h.badFreeCellNum[chain][level],
						vcName, vcFreeCellNum[chain][level])
					for _, vc := range h.vcSchedulers[vcName].getNonReservedFreeCellList()[chain][level] {
						virtualCell := vc.(*VirtualCell)
						if virtualCell.GetPhysicalCell() == nil && virtualCell.GetAPIStatus().CellHealthiness != api.CellBad {
							virtualCell.GetAPIStatus().CellHealthiness = api.CellBad
							break
						}
					}
				}
			}
		}
	}

	if parent := c.GetParent(); parent != nil {
		for _, buddy := range parent.GetChildren() {
			if buddy.(*PhysicalCell).GetAPIStatus().CellHealthiness != api.CellBad {
				return
			}
		}
		h.setBadCell(parent.(*PhysicalCell))
	}
}

// setHealthyCell marks a physical cell as healthy and recursively for its parent, guaranteeing that
// a cell is healthy if any of its children is healthy. The virtual cell that this physical cell
// is bound to will also be marked as healthy.
func (h *HivedAlgorithm) setHealthyCell(c *PhysicalCell) {
	klog.Infof("Cell %v is set to healthy", c.GetAddress())
	c.GetAPIStatus().CellHealthiness = api.CellHealthy
	if c.virtualCell != nil {
		c.GetAPIStatus().VirtualCell.CellHealthiness = api.CellHealthy
		c.virtualCell.GetAPIStatus().CellHealthiness = api.CellHealthy
		c.virtualCell.GetAPIStatus().PhysicalCell.CellHealthiness = api.CellHealthy
	}
	chain, level := c.GetChain(), c.GetLevel()
	if inFreeCellList(c) {
		h.badFreeCellNum[chain][level]--
		if h.allVCFreeCellNum[chain][level] >= h.totalLeftCellNum[chain][level]-h.badFreeCellNum[chain][level] {
			if h.allVCFreeCellNum[chain][level] == h.totalLeftCellNum[chain][level]-h.badFreeCellNum[chain][level] {
				klog.Infof("Cell type %v (chain %v level %v) now has sufficient healthy cells (healthy %v, bad %v) "+
					"for allocating the total free cells of all the VCs (%v).",
					h.cellTypes[chain][level], chain, level, h.totalLeftCellNum[chain][level]-h.badFreeCellNum[chain][level],
					h.badFreeCellNum[chain][level], h.allVCFreeCellNum[chain][level])
			}
			for vcName, vcFreeCellNum := range h.vcFreeCellNum {
				if vcFreeCellNum[chain][level] >= h.totalLeftCellNum[chain][level]-h.badFreeCellNum[chain][level] {
					if vcFreeCellNum[chain][level] == h.totalLeftCellNum[chain][level]-h.badFreeCellNum[chain][level] {
						klog.Infof("Cell type %v (chain %v level %v) now has sufficient healthy cells (healthy %v, bad %v) "+
							"for allocating the free cells of the VC %v (%v).",
							h.cellTypes[chain][level], chain, level,
							h.totalLeftCellNum[chain][level]-h.badFreeCellNum[chain][level], h.badFreeCellNum[chain][level],
							vcName, vcFreeCellNum[chain][level])
					}
					for _, vc := range h.vcSchedulers[vcName].getNonReservedFreeCellList()[chain][level] {
						virtualCell := vc.(*VirtualCell)
						if virtualCell.GetPhysicalCell() == nil && virtualCell.GetAPIStatus().CellHealthiness == api.CellBad {
							virtualCell.GetAPIStatus().CellHealthiness = api.CellHealthy
							break
						}
					}
				}
			}
		}
	}

	if parent := c.GetParent(); parent != nil {
		if pp := parent.(*PhysicalCell); pp.GetAPIStatus().CellHealthiness == api.CellBad {
			h.setHealthyCell(pp)
		}
	}
}

// scheduleNewAffinityGroup schedules each pod of a new affinity group to a set of GPUs
// (in both the physical cluster and the VC). This is the entrance of a new scheduling.
func (h *HivedAlgorithm) scheduleNewAffinityGroup(
	pod *core.Pod,
	s *api.PodSchedulingSpec,
	suggestedNodes common.Set) (physicalPlacement groupPhysicalPlacement, virtualPlacement groupVirtualPlacement) {

	priority := CellPriority(s.Priority)
	sr := schedulingRequest{
		vc:                   s.VirtualCluster,
		reservationId:        s.ReservationId,
		priority:             priority,
		affinityGroupName:    s.AffinityGroup.Name,
		affinityGroupPodNums: map[int32]int32{},
	}
	for _, m := range s.AffinityGroup.Members {
		// we will merge group members with same GPU number
		sr.affinityGroupPodNums[m.GpuNumber] += m.PodNumber
	}
	h.validateSchedulingRequest(sr, pod)
	if sr.reservationId != "" {
		klog.Infof("Use reservation %v", s.ReservationId)
		physicalPlacement, virtualPlacement = h.processSchedulingRequest(sr, suggestedNodes)
	} else {
		physicalPlacement, virtualPlacement = h.scheduleAffinityGroupForGpuType(sr, s.GpuType, pod, suggestedNodes)
	}
	if physicalPlacement != nil {
		klog.Infof("Succeeded in scheduling group %v", s.AffinityGroup.Name)
	} else {
		klog.Infof("Failed to schedule group %v", s.AffinityGroup.Name)
	}
	return physicalPlacement, virtualPlacement
}

// scheduleAffinityGroupForGpuType schedules an affinity group in a certain cell chain.
// If a GPU type is specified, it will be scheduled to a chain that contains this GPU type.
// Otherwise any GPU type will be tried (the first one that succeeds will be picked).
func (h *HivedAlgorithm) scheduleAffinityGroupForGpuType(
	sr schedulingRequest,
	gpuType string,
	pod *core.Pod,
	suggestedNodes common.Set) (physicalPlacement groupPhysicalPlacement, virtualPlacement groupVirtualPlacement) {

	if gpuType != "" {
		if chains := h.chains[gpuType]; chains == nil {
			panic(internal.NewBadRequestError(fmt.Sprintf(
				"[%v]: pod requesting GPU type %v which the whole cluster does not have",
				internal.Key(pod), gpuType)))
		} else {
			vcHasType := false
			for _, chain := range chains {
				if h.vcSchedulers[sr.vc].getNonReservedFullCellList()[chain] != nil {
					vcHasType = true
				}
				sr.chain = chain
				if physicalPlacement, virtualPlacement = h.processSchedulingRequest(sr, suggestedNodes); physicalPlacement != nil {
					return physicalPlacement, virtualPlacement
				}
			}
			if sr.priority >= minGuaranteedPriority && !vcHasType {
				panic(internal.NewBadRequestError(fmt.Sprintf(
					"[%v]: pod requesting GPU type %v which VC %v does not have",
					internal.Key(pod), gpuType, sr.vc)))
			}
		}
	} else {
		for _, chains := range h.chains {
			for _, chain := range chains {
				sr.chain = chain
				if physicalPlacement, virtualPlacement = h.processSchedulingRequest(sr, suggestedNodes); physicalPlacement != nil {
					return physicalPlacement, virtualPlacement
				}
			}
		}
	}
	return nil, nil
}

// validateSchedulingRequest checks the existence of VC and reservation ID, and the legality of priority.
func (h *HivedAlgorithm) validateSchedulingRequest(sr schedulingRequest, pod *core.Pod) {
	var message string
	if h.vcSchedulers[sr.vc] == nil {
		message = fmt.Sprintf("VC %v does not exists!", sr.vc)
	} else if sr.reservationId != "" {
		if h.vcSchedulers[sr.vc].getReservedCellList()[sr.reservationId] == nil {
			message = fmt.Sprintf("VC %v does not have reservation %v", sr.vc, sr.reservationId)
		} else if sr.priority == opportunisticPriority {
			message = fmt.Sprintf("opportunistic pod not supported to use reservation %v", sr.reservationId)
		}
	}
	if message != "" {
		panic(internal.NewBadRequestError(fmt.Sprintf("[%v]: %v", internal.Key(pod), message)))
	}
}

// processSchedulingRequest feeds a request to a VC scheduler
// or the opportunistic scheduler according to its priority.
func (h *HivedAlgorithm) processSchedulingRequest(
	sr schedulingRequest,
	suggestedNodes common.Set) (physicalPlacement groupPhysicalPlacement, virtualPlacement groupVirtualPlacement) {

	if sr.priority >= minGuaranteedPriority {
		return h.scheduleGuaranteedAffinityGroup(sr, suggestedNodes)
	} else {
		return h.scheduleOpportunisticAffinityGroup(sr, suggestedNodes), nil
	}
}

func (h *HivedAlgorithm) mapVirtualPlacementToPhysical(
	virtualPlacement groupVirtualPlacement,
	sr schedulingRequest,
	suggestedNodes common.Set) groupPhysicalPlacement {

	gpuNums := common.Int32MapKeys(sr.affinityGroupPodNums)
	common.SortInt32(gpuNums)
	physicalPlacement := groupPhysicalPlacement{}
	for _, podGpuNum := range gpuNums {
		podPlacements := virtualPlacement[podGpuNum]
		physicalPlacement[podGpuNum] = make([]CellList, len(podPlacements))
		for i, podGpus := range podPlacements {
			physicalPlacement[podGpuNum][i] = make(CellList, len(podGpus))
			for j, gpu := range podGpus {
				vGpu := gpu.(*VirtualCell)
				if pGpu := vGpu.GetPhysicalCell(); pGpu != nil {
					// Two possible cases of finding the virtual cell has been bound to a physical cell:
					// 1. A group of this VC is running on this physical cell (then the cell will be in CellUsed state).
					// We can either lazy-preempt this group and try to use another physical cell, or just preempt the group.
					// 2. A group of this VC is preempting another group on this cell (CellAcquiring or CellAcquired).
					// We will cancel the ongoing preemption and delete the preempting group.
					if pGpu.GetState() == cellUsed {
						if groupToPreempt := vGpu.GetPhysicalCell().GetUsingGroup(); groupToPreempt.lazyPreemptionEnable {
							h.lazyPreemptAffinityGroup(groupToPreempt, sr.affinityGroupName)
						}
					} else { // cellAcquiring or cellAcquired
						h.deletePreemptingAffinityGroup(pGpu.GetAcquiringGroup(), sr.affinityGroupName)
					}
				}
				physicalPlacement[podGpuNum][i][j] = mapVirtualCellToPhysical(vGpu, h.freeCellList[sr.chain], suggestedNodes)
			}
		}
	}
	clearPreBindings(virtualPlacement)
	return physicalPlacement
}

// scheduleGuaranteedAffinityGroup schedules an affinity group in its VC, and
// then maps the placement in VC to the physical cluster.
func (h *HivedAlgorithm) scheduleGuaranteedAffinityGroup(
	sr schedulingRequest,
	suggestedNodes common.Set) (groupPhysicalPlacement, groupVirtualPlacement) {

	// schedule in VC
	virtualPlacement := h.vcSchedulers[sr.vc].schedule(sr)
	if virtualPlacement == nil {
		return nil, nil
	}
	// map the vc placement to the physical cluster
	return h.mapVirtualPlacementToPhysical(virtualPlacement, sr, suggestedNodes), virtualPlacement
}

// scheduleOpportunisticAffinityGroup calls the opportunistic pod scheduler to schedule an affinity group.
func (h *HivedAlgorithm) scheduleOpportunisticAffinityGroup(
	sr schedulingRequest,
	suggestedNodes common.Set) (physicalPlacement groupPhysicalPlacement) {

	physicalPlacement = h.opportunisticSchedulers[sr.chain].Schedule(
		sr.affinityGroupPodNums, opportunisticPriority, suggestedNodes)
	if physicalPlacement == nil {
		klog.Infof("Insufficient capacity in PC for scheduling request: GPU numbers %v, priority %v, chain %v",
			sr.affinityGroupPodNums, sr.priority, sr.chain)
	} else {
		klog.Infof("Succeeded in scheduling in PC for scheduling request: GPU numbers %v, priority %v, chain %v",
			sr.affinityGroupPodNums, sr.priority, sr.chain)
	}
	return physicalPlacement
}

func (h *HivedAlgorithm) createPreemptingAffinityGroup(
	s *api.PodSchedulingSpec,
	physicalPlacement groupPhysicalPlacement,
	virtualPlacement groupVirtualPlacement,
	pod *core.Pod) {

	newGroup := newAlgoAffinityGroup(
		s.AffinityGroup, s.VirtualCluster, s.GangReleaseEnable, s.LazyPreemptionEnable, s.Priority, groupPreempting)
	newGroup.physicalGpuPlacement = physicalPlacement
	newGroup.virtualGpuPlacement = virtualPlacement
	for gpuNum := range physicalPlacement {
		for podIndex := range physicalPlacement[gpuNum] {
			for gpuIndex, gpu := range physicalPlacement[gpuNum][podIndex] {
				pGpu := gpu.(*PhysicalCell)
				vGpu := virtualPlacement[gpuNum][podIndex][gpuIndex].(*VirtualCell)
				if pGpu.GetState() == cellUsed {
					usingGroup := pGpu.GetUsingGroup()
					h.releaseGpu(pGpu, usingGroup.vc)
					usingGroup.state = groupBeingPreempted
				}
				h.allocateGpu(pGpu, vGpu, CellPriority(s.Priority), newGroup.vc)
				pGpu.AddAcquiringGroup(newGroup)
			}
		}
	}
	h.preemptingAffinityGroups[s.AffinityGroup.Name] = newGroup
	klog.Infof("[%v]: New preempting affinity group created: %v", internal.Key(pod), newGroup.name)
}

func (h *HivedAlgorithm) deletePreemptingAffinityGroup(g *AlgoAffinityGroup, newGroupName string) {
	for gpuNum := range g.physicalGpuPlacement {
		for podIndex := range g.physicalGpuPlacement[gpuNum] {
			for _, gpu := range g.physicalGpuPlacement[gpuNum][podIndex] {
				pGpu := gpu.(*PhysicalCell)
				h.releaseGpu(pGpu, g.vc)
				pGpu.DeleteAcquiringGroup(pGpu.GetAcquiringGroup())
				if pGpu.GetState() == cellUsed {
					// return the cell to the group being preempted
					beingPreemptedGroup := pGpu.GetUsingGroup()
					var beingPreemptedVGpu *VirtualCell
					if beingPreemptedGroup.virtualGpuPlacement != nil {
						beingPreemptedVGpu = retrieveVirtualCell(
							beingPreemptedGroup.physicalGpuPlacement,
							beingPreemptedGroup.virtualGpuPlacement, pGpu)
					}
					h.allocateGpu(
						pGpu, beingPreemptedVGpu, CellPriority(beingPreemptedGroup.priority), beingPreemptedGroup.vc)
				}
			}
		}
	}
	delete(h.preemptingAffinityGroups, g.name)
	klog.Infof("Preempting affinity group %v deleted because a higher-priority group %v is acquiring its resource",
		g.name, newGroupName)
}

func (h *HivedAlgorithm) preemptingAffinityGroupToAllocated(g *AlgoAffinityGroup, pod *core.Pod) {
	for gpuNum := range g.physicalGpuPlacement {
		for podIndex := range g.physicalGpuPlacement[gpuNum] {
			for _, gpu := range g.physicalGpuPlacement[gpuNum][podIndex] {
				pGpu := gpu.(*PhysicalCell)
				pGpu.DeleteAcquiringGroup(g)
				pGpu.AddUsingGroup(g)
			}
		}
	}
	g.state = groupAllocated
	h.allocatedAffinityGroups[g.name] = g
	delete(h.preemptingAffinityGroups, g.name)
	klog.Infof("[%v]: Preempting affinity group %v transits to allocated", internal.Key(pod), g.name)
}

// createAllocatedAffinityGroup creates a new affinity group, and confirms the allocated resources.
func (h *HivedAlgorithm) createAllocatedAffinityGroup(s *api.PodSchedulingSpec, info *api.PodBindInfo, pod *core.Pod) {
	newGroup := newAlgoAffinityGroup(
		s.AffinityGroup, s.VirtualCluster, s.GangReleaseEnable, s.LazyPreemptionEnable, s.Priority, groupAllocated)
	shouldLazyPreempt := false
	for _, gms := range info.AffinityGroupBindInfo {
		gpuNumber := int32(len(gms.PodPlacements[0].PhysicalGpuIndices))
		for podIndex := int32(0); podIndex < int32(len(gms.PodPlacements)); podIndex++ {
			node := gms.PodPlacements[podIndex].PhysicalNode
			for gpuIndex := int32(0); gpuIndex < int32(
				len(gms.PodPlacements[podIndex].PhysicalGpuIndices)); gpuIndex++ {
				pGpu, vGpu, lazyPreempt := h.findAllocatedGpu(
					gpuIndex,
					gms.PodPlacements[podIndex].PhysicalGpuIndices,
					gms.PodPlacements[podIndex].PreassignedCellTypes,
					CellChain(info.CellChain), node, shouldLazyPreempt, s, newGroup, pod)
				if pGpu == nil {
					// pGpu not being found means that this GPU address does not exist in the spec.
					// we simply ignore this GPU, and let the job run normally
					// (but we cannot ignore the other GPUs of this pod that are still in the spec,
					// otherwise it may cause resource conflicts)
					continue
				} else {
					newGroup.physicalGpuPlacement[gpuNumber][podIndex][gpuIndex] = pGpu
					if lazyPreempt == nil {
						newGroup.virtualGpuPlacement = nil
					} else if vGpu != nil {
						newGroup.virtualGpuPlacement[gpuNumber][podIndex][gpuIndex] = vGpu
						if vGpu.GetPhysicalCell() != nil {
							groupToPreempt := vGpu.GetPhysicalCell().GetUsingGroup()
							h.lazyPreemptAffinityGroup(groupToPreempt, newGroup.name)
						}
					} else {
						shouldLazyPreempt = shouldLazyPreempt || *lazyPreempt
					}
					// Even if we have successfully found the vGpu and pGpu, there is still one possibility
					// that we should not bind them: allocating the physical cell may lead to broken safety.
					// Such case won't happen by design as buddy alloc guarantees safety; but this could
					// happen due to inconsistency of VC assignments for reasons like reconfiguration.
					// In this case, we will lazy preempt this affinity group.
					success, message := h.allocateGpu(pGpu, vGpu, CellPriority(s.Priority), newGroup.vc)
					pGpu.AddUsingGroup(newGroup)
					if !success {
						shouldLazyPreempt = true
						klog.Warningf("[%v]: %v", internal.Key(pod), message)
					}
				}
			}
		}
	}
	if shouldLazyPreempt {
		h.lazyPreemptAffinityGroup(newGroup, newGroup.name)
	}
	h.allocatedAffinityGroups[s.AffinityGroup.Name] = newGroup
	klog.Infof("[%v]: New allocated affinity group created: %v", internal.Key(pod), s.AffinityGroup.Name)
}

func (h HivedAlgorithm) deleteAllocatedAffinityGroup(g *AlgoAffinityGroup, pod *core.Pod) {
	klog.Infof("[%v]: All pods complete, releasing resource from the affinity group %v",
		internal.Key(pod), g.name)
	for _, podPlacements := range g.physicalGpuPlacement {
		for _, podPlacement := range podPlacements {
			for _, gpu := range podPlacement {
				if gpu == nil {
					continue
				}
				pGpu := gpu.(*PhysicalCell)
				pGpu.DeleteUsingGroup(g)
				// State of pGpu can be either Free or Acquired now. We should call h.releaseGpu
				// only when it is in Free state; otherwise the cell must have been allocated
				// to the acquiring group before, we shouldn't release it.
				if pGpu.GetState() == cellFree {
					h.releaseGpu(pGpu, g.vc)
				}
			}
		}
	}
	delete(h.allocatedAffinityGroups, g.name)
	klog.Infof("[%v]: Allocated affinity group deleted: %v", internal.Key(pod), g.name)
}

// findAllocatedGpu finds the physical and virtual GPUs in the full cell lists for an allocate pod.
// The boolean return value indicates whether the affinity group should be lazy-preempted.
// The bool being nil means the group is OT and has no virtual placement.
func (h *HivedAlgorithm) findAllocatedGpu(
	index int32,
	physicalGpuIndices []int32,
	preassignedCellTypes []api.CellType,
	chain CellChain,
	node string,
	lazyPreempted bool,
	s *api.PodSchedulingSpec,
	group *AlgoAffinityGroup,
	pod *core.Pod) (*PhysicalCell, *VirtualCell, *bool) {

	priority := CellPriority(s.Priority)
	physicalGpuIndex := physicalGpuIndices[index]
	if pGpu := h.findPhysicalGpu(chain, node, physicalGpuIndex); pGpu == nil {
		klog.Warningf(
			"[%v]: cannot find GPU %v on node %v: not found in the spec. pod ignored",
			internal.Key(pod), physicalGpuIndex, node)
		return nil, nil, common.PtrBool(false)
	} else {
		var vGpu *VirtualCell
		if preassignedCellTypes == nil {
			klog.Warningf("[%v]: cannot find virtual cell: preassigned cell not found in pod bind info", internal.Key(pod))
			return pGpu, nil, common.PtrBool(true)
		}
		if group.virtualGpuPlacement != nil && !lazyPreempted {
			preassignedType := preassignedCellTypes[index]
			if preassignedType != "" {
				var preassignedLevel CellLevel
				typeFound := false
				for l, t := range h.cellTypes[pGpu.GetChain()] {
					if t == preassignedType {
						preassignedLevel = l
						typeFound = true
					}
				}
				var message string
				if !typeFound {
					message = fmt.Sprintf("preassigned cell type %v not found in chain %v", preassignedType, pGpu.GetChain())
				} else if vcs := h.vcSchedulers[s.VirtualCluster]; vcs == nil {
					message = fmt.Sprintf("VC %v not found", s.VirtualCluster)
				} else {
					vccl := vcs.getNonReservedFreeCellList()[pGpu.GetChain()]
					str := string(pGpu.GetChain())
					if s.ReservationId != "" {
						vccl = vcs.getReservedCellList()[s.ReservationId]
						str = string(s.ReservationId)
					}
					if vccl == nil {
						message = fmt.Sprintf("VC %v has no cell for %v", s.VirtualCluster, str)
					} else {
						vGpu, message = mapPhysicalCellToVirtual(pGpu, vccl, preassignedLevel, priority)
					}
				}
				if vGpu == nil {
					klog.Warningf("[%v]: cannot find virtual cell: %v", internal.Key(pod), message)
					return pGpu, nil, common.PtrBool(true)
				} else {
					return pGpu, vGpu, common.PtrBool(false)
				}
			} else {
				return pGpu, nil, nil
			}
		} else {
			return pGpu, nil, common.PtrBool(false)
		}
	}
}

func (h *HivedAlgorithm) allocateGpu(
	pGpu *PhysicalCell,
	vGpu *VirtualCell,
	p CellPriority,
	vcn api.VirtualClusterName) (success bool, message string) {

	success = true
	if vGpu != nil {
		setPriority(vGpu, p)
		updateUsedGpuNumAtPriority(vGpu, p, true)
		setPriority(pGpu, p)
		updateUsedGpuNumAtPriority(pGpu, p, true)
		preassignedNewlyBound := vGpu.GetPreAssignedCell().GetPhysicalCell() == nil
		bindCell(pGpu, vGpu)
		if preassignedNewlyBound {
			h.vcFreeCellNum[vcn][vGpu.GetChain()][vGpu.GetPreAssignedCell().GetLevel()]--
			h.allVCFreeCellNum[vGpu.GetChain()][vGpu.GetPreAssignedCell().GetLevel()]--
			// remove the allocated cell from the free list (possibly splitting cells)
			success, message = h.removeCellFromFreeList(vGpu.GetPreAssignedCell().GetPhysicalCell())
		}
	} else {
		setPriority(pGpu, opportunisticPriority)
		updateUsedGpuNumAtPriority(pGpu, opportunisticPriority, true)
		pGpu.GetAPIStatus().VC = vcn
		h.apiClusterStatus.VirtualClusters[vcn] = append(
			h.apiClusterStatus.VirtualClusters[vcn], generateOpporVirtualCell(pGpu.GetAPIStatus()))
	}
	return success, message
}

func (h *HivedAlgorithm) releaseGpu(pGpu *PhysicalCell, vcn api.VirtualClusterName) {
	if vGpu := pGpu.GetVirtualCell(); vGpu != nil {
		preassignedPhysical := vGpu.GetPreAssignedCell().GetPhysicalCell()
		unbindCell(pGpu)
		if vGpu.GetPreAssignedCell().GetPhysicalCell() == nil {
			h.vcFreeCellNum[vcn][vGpu.GetChain()][vGpu.GetPreAssignedCell().GetLevel()]++
			h.allVCFreeCellNum[vGpu.GetChain()][vGpu.GetPreAssignedCell().GetLevel()]++
			// add the released cell back to the free list (possibly merging cells)
			h.addCellToFreeList(preassignedPhysical)
		}
		updateUsedGpuNumAtPriority(vGpu, vGpu.GetPriority(), false)
		setPriority(vGpu, freePriority)
	} else {
		var opporVirtualCellIdx int32
		for i, ovc := range h.apiClusterStatus.VirtualClusters[vcn] {
			if ovc.PhysicalCell != nil && ovc.PhysicalCell.CellAddress == pGpu.GetAddress() {
				opporVirtualCellIdx = int32(i)
				break
			}
		}
		novc := len(h.apiClusterStatus.VirtualClusters[vcn])
		h.apiClusterStatus.VirtualClusters[vcn][opporVirtualCellIdx] = h.apiClusterStatus.VirtualClusters[vcn][novc-1]
		h.apiClusterStatus.VirtualClusters[vcn][novc-1] = nil
		h.apiClusterStatus.VirtualClusters[vcn] = h.apiClusterStatus.VirtualClusters[vcn][:novc-1]
		pGpu.GetAPIStatus().VC = ""
	}
	updateUsedGpuNumAtPriority(pGpu, pGpu.GetPriority(), false)
	setPriority(pGpu, freePriority)
}

// confirmAllocatedGpu creates the cell bindings, removes the physical cell from the free list
// (if necessary), and sets the priority.
func (h *HivedAlgorithm) confirmAllocatedGpu(
	pGpu *PhysicalCell,
	vGpu *VirtualCell,
	p CellPriority,
	g *AlgoAffinityGroup) (success bool, message string) {

	success = true
	if pGpu.GetState() == cellFree {
		h.allocateGpu(pGpu, vGpu, p, g.vc)
	}
	pGpu.AddUsingGroup(g)
	pGpu.SetState(cellUsed)
	return success, message
}

// confirmReleasedGpu destroys the cell bindings, adds the physical cell back to the free list
// (if necessary), and resets the priority.
func (h *HivedAlgorithm) confirmReleasedGpu(pGpu *PhysicalCell, g *AlgoAffinityGroup) {
	h.releaseGpu(pGpu, g.vc)
	pGpu.DeleteUsingGroup(g)
}

// lazyPreemptAffinityGroup removes an affinity group from its VC, clears it virtual placement,
// and exposes this decision.
func (h *HivedAlgorithm) lazyPreemptAffinityGroup(
	victim *AlgoAffinityGroup, preemptor string) {

	for _, podVirtualPlacements := range victim.virtualGpuPlacement {
		for _, podVirtualPlacement := range podVirtualPlacements {
			for _, gpu := range podVirtualPlacement {
				if gpu != nil {
					vGpu := gpu.(*VirtualCell)
					pGpu := vGpu.GetPhysicalCell()
					h.releaseGpu(pGpu, victim.vc)
					h.allocateGpu(pGpu, nil, opportunisticPriority, victim.vc)
				}
			}
		}
	}
	victim.virtualGpuPlacement = nil
	victim.lazyPreemptionStatus = &api.LazyPreemptionStatus{
		Preemptor:      preemptor,
		PreemptionTime: meta.Now(),
	}

	klog.Infof("Affinity group %v is lazy preempted from VC by %v", victim.name, preemptor)
}

// removeCellFromFreeList removes a cell from the free cell list and splits its parent recursively if needed.
func (h *HivedAlgorithm) removeCellFromFreeList(c *PhysicalCell) (success bool, message string) {
	chain := c.GetChain()
	success = true
	// reduce the left cell numbers for the lower levels and do safety check
	numToRemove := int32(len(h.fullCellList[chain][c.GetLevel()][0].GetChildren()))
	isBad := c.GetAPIStatus().CellHealthiness == api.CellBad
	for l := c.GetLevel() - 1; l >= lowestLevel; l-- {
		h.totalLeftCellNum[chain][l] -= numToRemove
		if h.totalLeftCellNum[chain][l] < h.allVCFreeCellNum[chain][l] {
			success = false
			message = fmt.Sprintf("Adding pod would lead to broken safety: cell type %v, %v left, %v free cells in all VCs",
				h.cellTypes[chain][l], h.totalLeftCellNum[chain][l], h.allVCFreeCellNum[chain][l])
		}
		// if the used cell is bad, we should exclude it from the bad free cells (the same below)
		if isBad {
			h.badFreeCellNum[chain][l] -= numToRemove
		}
		numToRemove *= int32(len(h.fullCellList[chain][l][0].GetChildren()))
	}
	for terminate := false; ; {
		l := c.GetLevel()
		parent := c.GetParent()
		if parent != nil {
			pp := parent.(*PhysicalCell)
			if pp.IsSplit() {
				terminate = true
			} else {
				h.freeCellList[chain][l] = append(h.freeCellList[chain][l], pp.GetChildren()...)
				pp.SetSplit(true)
			}
		} else {
			terminate = true
		}
		h.freeCellList[chain].remove(c, l)
		h.totalLeftCellNum[chain][l]--
		if h.totalLeftCellNum[chain][l] < h.allVCFreeCellNum[chain][l] {
			success = false
			message = fmt.Sprintf("Adding pod would lead to broken safety: cell type %v, %v left, %v free cells in all VCs",
				h.cellTypes[chain][l], h.totalLeftCellNum[chain][l], h.allVCFreeCellNum[chain][l])
		}
		if c.GetAPIStatus().CellHealthiness == api.CellBad {
			h.badFreeCellNum[chain][l]--
		}
		if terminate {
			break
		} else {
			c = parent.(*PhysicalCell)
		}
	}
	return success, message
}

// addCellToFreeList adds a cell to the free cell list and merges its buddies recursively if needed.
func (h *HivedAlgorithm) addCellToFreeList(c *PhysicalCell) {
	chain := c.GetChain()
	// increase the left cell numbers for the lower levels
	numToAdd := int32(len(h.fullCellList[chain][c.GetLevel()][0].GetChildren()))
	isBad := c.GetAPIStatus().CellHealthiness == api.CellBad
	for l := c.GetLevel() - 1; l >= lowestLevel; l-- {
		h.totalLeftCellNum[chain][l] += numToAdd
		// if the freed cell is bad, we should add it to the bad free cells (the same below)
		if isBad {
			h.badFreeCellNum[chain][l] += numToAdd
		}
		numToAdd *= int32(len(h.fullCellList[chain][l][0].GetChildren()))
	}
	for terminate := false; ; {
		l := c.GetLevel()
		parent := c.GetParent()
		if parent != nil {
			allBuddyFree := true
			for _, buddy := range parent.GetChildren() {
				if buddy.(*PhysicalCell).GetVirtualCell() != nil {
					allBuddyFree = false
					break
				}
			}
			if !allBuddyFree {
				terminate = true
			} else {
				for _, buddy := range parent.GetChildren() {
					if !CellEqual(buddy, c) {
						h.freeCellList[chain].remove(buddy, l)
					}
				}
				parent.(*PhysicalCell).SetSplit(false)
			}
		} else {
			terminate = true
		}
		h.totalLeftCellNum[chain][l]++
		if c.GetAPIStatus().CellHealthiness == api.CellBad {
			h.badFreeCellNum[chain][l]++
		}
		if terminate {
			h.freeCellList[chain][l] = append(h.freeCellList[chain][l], c)
			break
		} else {
			c = parent.(*PhysicalCell)
		}
	}
}

// findPhysicalGpu finds a physical GPU cell in the full list. If the GPU is not found in the chain specified
// in the PodBindInfo (due to reconfiguration), we will try to search in the other chains.
func (h *HivedAlgorithm) findPhysicalGpu(
	chain CellChain,
	node string,
	gpuIndex int32) *PhysicalCell {

	if g := h.findPhysicalGpuInChain(chain, node, gpuIndex); g == nil {
		for c := range h.fullCellList {
			if c != chain {
				if g = h.findPhysicalGpuInChain(c, node, gpuIndex); g != nil {
					klog.Warningf("GPU %v on node %v has been moved to chain %v", gpuIndex, node, c)
					return g
				}
			}
		}
		return nil
	} else {
		return g
	}
}

// findPhysicalGpuInChain finds a physical GPU cell in the full list of a given chain. This search is based on
// *one* node and *one* GPU index, assuming there is no resource overlapping among cells at the same level.
func (h *HivedAlgorithm) findPhysicalGpuInChain(
	chain CellChain,
	node string,
	gpuIndex int32) *PhysicalCell {

	for _, c := range h.fullCellList[chain][1] {
		success := false
		cc := c.(*PhysicalCell)
		nodes, gpuIndices := cc.GetPhysicalPlacement()
		for _, n := range nodes {
			if n == node {
				success = true
				break
			}
		}
		if success {
			if gpuIndex < 0 {
				return cc
			} else {
				for _, g := range gpuIndices {
					if g == gpuIndex {
						return cc
					}
				}
			}
		}
	}
	return nil
}
