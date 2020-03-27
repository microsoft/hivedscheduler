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
	"math"
	"math/rand"
	"sync"
)

// HivedAlgorithm implements an internal.SchedulerAlgorithm. It schedules pods using the algorithm of HiveD.
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
		vcSchedulers:            map[api.VirtualClusterName]intraVCScheduler{},
		opportunisticSchedulers: map[CellChain]*topologyAwareScheduler{},
		fullCellList:            fullPcl,
		freeCellList:            freePcl,
		vcFreeCellNum:           vcFreeCellNum,
		totalLeftCellNum:        map[CellChain]map[CellLevel]int32{},
		badFreeCellNum:          map[CellChain]map[CellLevel]int32{},
		badNodes:                common.NewSet(),
		chains:                  gpuTypeToChain,
		cellTypes:               cellLevelToType,
		allocatedAffinityGroups: map[string]*AlgoAffinityGroup{},
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

func (h *HivedAlgorithm) Schedule(
	pod *core.Pod,
	suggestedNodes []string,
	phase internal.SchedulingPhase) internal.PodScheduleResult {
	h.algorithmLock.Lock()
	defer h.algorithmLock.Unlock()

	klog.Infof("[%v]: Scheduling pod...", internal.Key(pod))
	s := internal.ExtractPodSchedulingSpec(pod)
	// gpu number -> a set of pods -> a set of GPUs of each pod
	groupPhysicalPlacement := groupPhysicalPlacement{}
	groupVirtualPlacement := groupVirtualPlacement{}
	podIndex := int32(0)
	suggestedNodeSet := common.NewSet()
	for _, n := range suggestedNodes {
		suggestedNodeSet.Add(n)
	}

	group := h.allocatedAffinityGroups[s.AffinityGroup.Name]
	if group == nil {
		klog.Infof("[%v]: Scheduling new affinity group %v", internal.Key(pod), s.AffinityGroup.Name)
		groupPhysicalPlacement, groupVirtualPlacement = h.scheduleNewAffinityGroup(pod, s, suggestedNodeSet)
	} else {
		klog.Infof("[%v]: Pod from existing affinity group: %v", internal.Key(pod), s.AffinityGroup.Name)
		groupPhysicalPlacement = group.physicalGpuPlacement
		groupVirtualPlacement = group.virtualGpuPlacement
		podIndex = -1
		for i, p := range group.allocatedPods[s.GpuNumber] {
			if p == nil {
				podIndex = int32(i)
				break
			}
		}
		if podIndex == -1 {
			panic(internal.NewBadRequestError(fmt.Sprintf(
				"Requesting more pods than the configured number for %v GPUs (%v pods) in affinity group %v",
				s.GpuNumber, group.totalPodNums[s.GpuNumber], s.AffinityGroup.Name)))
		}
	}
	return generatePodScheduleResult(
		groupPhysicalPlacement,
		groupVirtualPlacement,
		CellPriority(s.Priority),
		h.cellTypes,
		s.GpuNumber,
		podIndex,
		group,
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

	klog.Infof("[%v]: adding allocated pod...", internal.Key(pod))
	s := internal.ExtractPodSchedulingSpec(pod)
	info := internal.ExtractPodBindInfo(pod)
	klog.Infof("[%v]: adding to node %v, GPUs %v", internal.Key(pod), info.Node, info.GpuIsolation)

	podIndex := int32(0)
	if group := h.allocatedAffinityGroups[s.AffinityGroup.Name]; group == nil {
		h.createAllocatedAffinityGroup(pod, s, info)
	} else {
		for _, gms := range info.AffinityGroupBindInfo {
			if gpuNumber := int32(len(gms.PodPlacements[0].PhysicalGpuIndices)); gpuNumber == s.GpuNumber {
				podIndex = getPodIndex(gms.PodPlacements, info.Node, info.GpuIsolation[0])
				if podIndex == -1 {
					klog.Errorf("[%v]: pod placement not found in group %v: node %v, GPUs %v",
						internal.Key(pod), s.AffinityGroup.Name, info.Node, info.GpuIsolation)
					return
				}
				// if this pod was previously added, then deleted, and we are now re-adding it,
				// we should return the resources to the pod (i.e., re-execute h.confirmAllocatedGpu)
				for gpuIndex := int32(0); gpuIndex < int32(
					len(gms.PodPlacements[podIndex].PhysicalGpuIndices)); gpuIndex++ {
					pGpu, vGpu, _ := h.findAllocatedGpu(
						gpuIndex,
						gms.PodPlacements[podIndex].PhysicalGpuIndices,
						gms.PodPlacements[podIndex].PreassignedCellTypes,
						CellChain(info.CellChain), info.Node, false, s, group, pod)
					if pGpu == nil {
						continue
					} else if pGpu.GetAffinityGroup() == nil {
						if vGpu != nil && vGpu.GetPhysicalCell() != nil {
							groupToPreempt := vGpu.GetPhysicalCell().GetAffinityGroup()
							h.lazyPreemptAffinityGroup(groupToPreempt, group.name)
						}
						h.confirmAllocatedGpu(pGpu, vGpu, CellPriority(s.Priority), group)
					}
				}
			}
			break
		}
	}
	h.allocatedAffinityGroups[s.AffinityGroup.Name].allocatedPods[s.GpuNumber][podIndex] = pod
}

func (h *HivedAlgorithm) DeleteAllocatedPod(pod *core.Pod) {
	h.algorithmLock.Lock()
	defer h.algorithmLock.Unlock()

	klog.Infof("[%v]: deleting allocated pod...", internal.Key(pod))
	s := internal.ExtractPodSchedulingSpec(pod)
	info := internal.ExtractPodBindInfo(pod)
	klog.Infof("[%v]: deleting from node %v, GPUs %v", internal.Key(pod), info.Node, info.GpuIsolation)

	if group := h.allocatedAffinityGroups[s.AffinityGroup.Name]; group == nil {
		klog.Errorf("[%v]: group %v not found when deleting pod", internal.Key(pod), s.AffinityGroup.Name)
		return
	} else {
		var podIndex int32
		for _, gms := range info.AffinityGroupBindInfo {
			if gpuNumber := int32(len(gms.PodPlacements[0].PhysicalGpuIndices)); gpuNumber == s.GpuNumber {
				podIndex = getPodIndex(gms.PodPlacements, info.Node, info.GpuIsolation[0])
				if podIndex == -1 {
					klog.Errorf("[%v]: pod placement not found in group %v: node %v, GPUs %v",
						internal.Key(pod), s.AffinityGroup.Name, info.Node, info.GpuIsolation)
					return
				}
			}
		}
		group.allocatedPods[s.GpuNumber][podIndex] = nil
		if !group.gangReleaseEnable {
			klog.Infof("[%v]: gang release NOT enabled for group %v, releasing resources for this pod",
				internal.Key(pod), s.AffinityGroup.Name)
			for _, gpu := range group.physicalGpuPlacement[s.GpuNumber][podIndex] {
				if gpu != nil {
					h.confirmReleasedGpu(gpu.(*PhysicalCell), group)
				}
			}
		}

		if allPodsReleased(group.allocatedPods) {
			if group.gangReleaseEnable {
				klog.Infof("[%v]: gang release enabled for group %v, releasing resources for all the pods",
					internal.Key(pod), s.AffinityGroup.Name)
			}
			// Even if the group does not enable gang release, we still need to check the placements of all pods
			// and release them if still in use (this could be because a pod in the group is never added,
			// and hence also never deleted, we need to release the resource explicitly)
			for _, podPlacements := range group.physicalGpuPlacement {
				for _, podPlacement := range podPlacements {
					for _, gpu := range podPlacement {
						if gpu != nil && gpu.GetPriority() != freePriority {
							h.confirmReleasedGpu(gpu.(*PhysicalCell), group)
						}
					}
				}
			}
			delete(h.allocatedAffinityGroups, s.AffinityGroup.Name)
			klog.Infof("[%v]: All pods complete, affinity group deleted: %v", internal.Key(pod), s.AffinityGroup.Name)
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

// scheduleGuaranteedAffinityGroup schedules an affinity group in its VC, and
// then maps the placement in VC to the physical cluster.
func (h *HivedAlgorithm) scheduleGuaranteedAffinityGroup(
	sr schedulingRequest,
	suggestedNodes common.Set) (physicalPlacement groupPhysicalPlacement, virtualPlacement groupVirtualPlacement) {

	// schedule in VC
	virtualPlacement = h.vcSchedulers[sr.vc].schedule(sr)
	if virtualPlacement == nil {
		return nil, nil
	}
	// map the vc placement to the physical cluster
	gpuNums := make([]int32, len(sr.affinityGroupPodNums))
	i := 0
	for n := range sr.affinityGroupPodNums {
		gpuNums[i] = n
		i++
	}
	common.SortInt32(gpuNums)
	physicalPlacement = groupPhysicalPlacement{}
	for _, podGpuNum := range gpuNums {
		podPlacements := virtualPlacement[podGpuNum]
		physicalPlacement[podGpuNum] = make([]CellList, len(podPlacements))
		for i, podGpus := range podPlacements {
			physicalPlacement[podGpuNum][i] = make(CellList, len(podGpus))
			for j, gpu := range podGpus {
				vGpu := gpu.(*VirtualCell)
				if vGpu.GetPhysicalCell() != nil {
					if groupToPreempt := vGpu.GetPhysicalCell().GetAffinityGroup(); groupToPreempt.lazyPreemptionEnable {
						h.lazyPreemptAffinityGroup(groupToPreempt, sr.affinityGroupName)
					}
				}
				pac := vGpu.GetPreAssignedCell()
				// check if the preassigned cell has been (temporarily) bound to a physical cell
				preassignedPhysical := pac.GetPhysicalCell()
				if preassignedPhysical == nil {
					preassignedPhysical = pac.GetPreBoundPhysicalCell()
				}
				if preassignedPhysical == nil {
					// allocate a new physical cell to the preassigned cell. input a copy of the free cell list
					// because during the scheduling we should not make in-place change to the data structures
					// (they can be modified only when adding or deleting pods)
					c := buddyAlloc(h.getTmpFreeCellList(sr.chain), pac.GetLevel(), suggestedNodes)
					if c == nil {
						panic(fmt.Sprintf(
							"VC Safety Broken: Cannot find physical cell for a VC cell: %v", pac.GetAddress()))
					} else {
						preassignedPhysical = c
						// create binding (which is temporary and will be cleared after the scheduling,
						// same reason as above)
						pac.SetPreBoundPhysicalCell(preassignedPhysical)
						preassignedPhysical.SetPreBoundVirtualCell(pac)
					}
				}
				physicalPlacement[podGpuNum][i][j] = mapNonPreassignedCellToPhysical(vGpu, suggestedNodes)
			}
		}
	}
	clearPreBindings(virtualPlacement)
	return physicalPlacement, virtualPlacement
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

// getTmpFreeCellList returns a copy of the free cell list.
func (h *HivedAlgorithm) getTmpFreeCellList(chain CellChain) ChainCellList {
	ccl := ChainCellList{}
	for l := CellLevel(1); l <= CellLevel(len(h.freeCellList[chain])); l++ {
		ccl[l] = make(CellList, len(h.freeCellList[chain][l]))
		copy(ccl[l], h.freeCellList[chain][l])
	}
	return ccl
}

// createAllocatedAffinityGroup creates a new affinity group, and confirms the allocated resources.
func (h *HivedAlgorithm) createAllocatedAffinityGroup(pod *core.Pod, s *api.PodSchedulingSpec, info *api.PodBindInfo) {
	newGroup := newAlgoAffinityGroup(s.AffinityGroup, s.VirtualCluster, s.GangReleaseEnable, s.LazyPreemptionEnable)
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
							groupToPreempt := vGpu.GetPhysicalCell().GetAffinityGroup()
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
					if success, message := h.confirmAllocatedGpu(pGpu, vGpu, CellPriority(s.Priority), newGroup); !success {
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
	klog.Infof("[%v]: New affinity group created: %v", internal.Key(pod), s.AffinityGroup.Name)
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
						vGpu, message = mapNonPreassignedCellToVirtual(pGpu, vccl, preassignedLevel, priority)
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

// confirmAllocatedGpu creates the cell bindings, removes the physical cell from the free list
// (if necessary), and sets the priority.
func (h *HivedAlgorithm) confirmAllocatedGpu(
	pGpu *PhysicalCell,
	vGpu *VirtualCell,
	p CellPriority,
	g *AlgoAffinityGroup) (success bool, message string) {

	success = true
	if vGpu != nil {
		setPriority(vGpu, p)
		updateUsedGpuNumAtPriority(vGpu, p, true)
		setPriority(pGpu, p)
		updateUsedGpuNumAtPriority(pGpu, p, true)
		preassignedNewlyBound := vGpu.GetPreAssignedCell().GetPhysicalCell() == nil
		bindCell(pGpu, vGpu)
		if preassignedNewlyBound {
			h.vcFreeCellNum[g.vc][vGpu.GetChain()][vGpu.GetPreAssignedCell().GetLevel()]--
			h.allVCFreeCellNum[vGpu.GetChain()][vGpu.GetPreAssignedCell().GetLevel()]--
			// remove the allocated cell from the free list (possibly splitting cells)
			success, message = h.removeCellFromFreeList(vGpu.GetPreAssignedCell().GetPhysicalCell())
		}
	} else {
		setPriority(pGpu, opportunisticPriority)
		updateUsedGpuNumAtPriority(pGpu, opportunisticPriority, true)
		pGpu.GetAPIStatus().VC = g.vc
		h.apiClusterStatus.VirtualClusters[g.vc] = append(
			h.apiClusterStatus.VirtualClusters[g.vc], generateOpporVirtualCell(pGpu.GetAPIStatus()))
	}
	pGpu.AddAffinityGroup(g)
	return success, message
}

// confirmReleasedGpu destroys the cell bindings, adds the physical cell back to the free list
// (if necessary), and resets the priority.
func (h *HivedAlgorithm) confirmReleasedGpu(pGpu *PhysicalCell, g *AlgoAffinityGroup) {
	if vGpu := pGpu.GetVirtualCell(); vGpu != nil {
		preassignedPhysical := vGpu.GetPreAssignedCell().GetPhysicalCell()
		unbindCell(pGpu)
		if vGpu.GetPreAssignedCell().GetPhysicalCell() == nil {
			h.vcFreeCellNum[g.vc][vGpu.GetChain()][vGpu.GetPreAssignedCell().GetLevel()]++
			h.allVCFreeCellNum[vGpu.GetChain()][vGpu.GetPreAssignedCell().GetLevel()]++
			// add the released cell back to the free list (possibly merging cells)
			h.addCellToFreeList(preassignedPhysical)
		}
		updateUsedGpuNumAtPriority(vGpu, vGpu.GetPriority(), false)
		setPriority(vGpu, freePriority)
	} else {
		var opporVirtualCellIdx int32
		for i, ovc := range h.apiClusterStatus.VirtualClusters[g.vc] {
			if ovc.PhysicalCell != nil && ovc.PhysicalCell.CellAddress == pGpu.GetAddress() {
				opporVirtualCellIdx = int32(i)
				break
			}
		}
		novc := len(h.apiClusterStatus.VirtualClusters[g.vc])
		h.apiClusterStatus.VirtualClusters[g.vc][opporVirtualCellIdx] = h.apiClusterStatus.VirtualClusters[g.vc][novc-1]
		h.apiClusterStatus.VirtualClusters[g.vc][novc-1] = nil
		h.apiClusterStatus.VirtualClusters[g.vc] = h.apiClusterStatus.VirtualClusters[g.vc][:novc-1]
		pGpu.GetAPIStatus().VC = ""
	}
	setPriority(pGpu, freePriority)
	updateUsedGpuNumAtPriority(pGpu, pGpu.GetPriority(), false)
	pGpu.DeleteAffinityGroup(g)
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
					h.confirmReleasedGpu(pGpu, victim)
					h.confirmAllocatedGpu(pGpu, nil, opportunisticPriority, victim)
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

// generatePodScheduleResult writes the scheduling result into a PodScheduleResult.
func generatePodScheduleResult(
	groupPhysicalPlacement groupPhysicalPlacement,
	groupVirtualPlacement groupVirtualPlacement,
	priority CellPriority,
	cellLevelToType map[CellChain]map[CellLevel]api.CellType,
	currentGpuNum int32,
	currentPodIndex int32,
	group *AlgoAffinityGroup,
	groupName string,
	suggestedNodes common.Set,
	vc api.VirtualClusterName,
	pod *core.Pod) internal.PodScheduleResult {

	klog.V(4).Infof("[%v]: Got K8s suggested nodes: %v", internal.Key(pod), suggestedNodes.ToString())
	klog.Infof("[%v]: Physical placement: %v", internal.Key(pod), groupPhysicalPlacement.toString())
	if groupVirtualPlacement != nil {
		klog.Infof("[%v]: Virtual placement: %v", internal.Key(pod), groupVirtualPlacement.toString())
	}
	preemptionVictims, nodesHaveVictims := collectPreemptionVictims(groupPhysicalPlacement, priority, groupName)
	if len(preemptionVictims) > 0 {
		// We collect victims on a random node, as K8S preempts victims from only one node once.
		// Random is to let different pods preempt victims on different nodes
		// (note that this randomness is not necessary for the eventual completeness of preemption).
		nodeToPreempt := nodesHaveVictims[rand.Int31n(int32(len(nodesHaveVictims)))]
		var victimPods []*core.Pod
		var victimNames []string
		for v := range preemptionVictims[nodeToPreempt].Items() {
			victimPods = append(victimPods, v.(*core.Pod))
			victimNames = append(victimNames, internal.Key(v.(*core.Pod)))
		}
		klog.Infof("[%v]: need to preempt pods %v", internal.Key(pod), common.ToJson(victimNames))
		return internal.PodScheduleResult{
			PodPreemptInfo: &internal.PodPreemptInfo{VictimPods: victimPods},
		}
	} else {
		// we find the selected node after the preemption is done, otherwise the preemption victims
		// may cause the selected node to be excluded from the suggested nodes
		affinityGroupBindInfo, nodesNotInSuggested, selectedNode, selectedGpuIndices, cellChain := generateAffinityGroupBindInfo(
			groupPhysicalPlacement, groupVirtualPlacement, cellLevelToType, currentGpuNum, currentPodIndex, group, groupName, suggestedNodes)
		var waitReason string
		if affinityGroupBindInfo == nil {
			waitReason = "insufficient capacity in physical cluster"
			if priority >= minGuaranteedPriority {
				waitReason = fmt.Sprintf("insufficient capacity in VC %v", vc)
			}
		} else if len(nodesNotInSuggested) > 0 {
			if group == nil {
				// for a new group, we will keep it waiting if not all of its pods are scheduled to suggested nodes
				waitReason = fmt.Sprintf(
					"affinity group is scheduled to some nodes not within K8s suggested nodes: %v",
					common.ToJson(nodesNotInSuggested))
			} else {
				// for an existing group, we always insist the previous scheduling decision
				// even if some pods are now not within suggested nodes
				klog.Warningf("Some nodes used by affinity group %v are no longer within K8s suggested nodes: %v",
					group.name, common.ToJson(nodesNotInSuggested))
			}
		}
		if waitReason != "" {
			klog.Infof("[%v]: need to wait because %v", internal.Key(pod), waitReason)
			return internal.PodScheduleResult{PodWaitInfo: &internal.PodWaitInfo{Reason: waitReason}}
		}
		klog.Infof("[%v]: scheduled to node %v, GPUs %v",
			internal.Key(pod), selectedNode, selectedGpuIndices)
		return internal.PodScheduleResult{
			PodBindInfo: &api.PodBindInfo{
				Node:                  selectedNode,
				GpuIsolation:          selectedGpuIndices,
				CellChain:             cellChain,
				AffinityGroupBindInfo: affinityGroupBindInfo,
			},
		}
	}
}

// generateAffinityGroupBindInfo writes the physical and virtual placements of an affinity group
// into a a series of AffinityGroupMemberBindInfos, and returns the allocated node and GPU addresses
// of the current pod.
func generateAffinityGroupBindInfo(
	groupPhysicalPlacement groupPhysicalPlacement,
	groupVirtualPlacement groupVirtualPlacement,
	cellLevelToType map[CellChain]map[CellLevel]api.CellType,
	currentGpuNum int32,
	currentPodIndex int32,
	group *AlgoAffinityGroup,
	groupName string,
	suggestedNodes common.Set) (
	affinityGroupBindInfo []api.AffinityGroupMemberBindInfo,
	nodesNotInSuggested []string,
	selectedNode string,
	selectedGpuIndices []int32,
	chain string) {

	if groupPhysicalPlacement == nil {
		return
	}
	affinityGroupBindInfo = make([]api.AffinityGroupMemberBindInfo, len(groupPhysicalPlacement))
	groupMemberIndex := 0
	for podGpuNum, podPhysicalPlacements := range groupPhysicalPlacement {
		mbi := api.AffinityGroupMemberBindInfo{
			PodPlacements: make([]api.PodPlacementInfo, len(podPhysicalPlacements)),
		}
		for podIndex := int32(0); podIndex < int32(len(podPhysicalPlacements)); podIndex++ {
			mbi.PodPlacements[podIndex].PhysicalGpuIndices = make([]int32, podGpuNum)
			mbi.PodPlacements[podIndex].PreassignedCellTypes = make([]api.CellType, podGpuNum)
			for gpuIndex := int32(0); gpuIndex < podGpuNum; gpuIndex++ {
				pGpu := podPhysicalPlacements[podIndex][gpuIndex]
				if pGpu == nil {
					if group == nil {
						panic(fmt.Sprintf("The first pod in group %v was allocated invalid resource", groupName))
					}
					// if the physical placement of this pod is not found (e.g., removed due to reconfiguration),
					// we will insist the decision by retrieving it from other pods
					mbi.PodPlacements[podIndex], chain = retrieveMissingPodPlacement(group, podGpuNum, podIndex)
					klog.Warningf(
						"pod placement has been invalid and is retrieved from annotation of other pods: node %v, GPU %v",
						mbi.PodPlacements[podIndex].PhysicalNode, mbi.PodPlacements[podIndex].PhysicalGpuIndices[gpuIndex])
				} else {
					nodes, gpuIndices := pGpu.(*PhysicalCell).GetPhysicalPlacement()
					// here each cell (i.e., pGpu) is only one GPU, hence we takes the first element
					// in its "nodes" and "gpuIndices" as the node and GPU address
					if mbi.PodPlacements[podIndex].PhysicalNode == "" {
						mbi.PodPlacements[podIndex].PhysicalNode = nodes[0]
						if !suggestedNodes.Contains(nodes[0]) {
							nodesNotInSuggested = append(nodesNotInSuggested, nodes[0])
						}
					}
					mbi.PodPlacements[podIndex].PhysicalGpuIndices[gpuIndex] = gpuIndices[0]
					if groupVirtualPlacement != nil {
						vGpu := groupVirtualPlacement[podGpuNum][podIndex][gpuIndex].(*VirtualCell)
						mbi.PodPlacements[podIndex].PreassignedCellTypes[gpuIndex] =
							cellLevelToType[vGpu.GetChain()][vGpu.GetPreAssignedCell().GetLevel()]
					} else {
						mbi.PodPlacements[podIndex].PreassignedCellTypes[gpuIndex] = ""
					}
				}
			}
		}
		if podGpuNum == currentGpuNum {
			selectedNode = mbi.PodPlacements[currentPodIndex].PhysicalNode
			selectedGpuIndices = mbi.PodPlacements[currentPodIndex].PhysicalGpuIndices
			if pGpu := groupPhysicalPlacement[currentGpuNum][currentPodIndex][0]; pGpu != nil {
				chain = string(pGpu.GetChain())
			}
		}
		affinityGroupBindInfo[groupMemberIndex] = mbi
		groupMemberIndex++
	}
	return affinityGroupBindInfo, nodesNotInSuggested, selectedNode, selectedGpuIndices, chain
}

// collectPreemptionVictims collects preemption victims of an affinity group.
// If any of the GPUs allocated for the whole group is still used by a pod,
// we will wait for the preemption, as a group is gang-scheduled.
func collectPreemptionVictims(
	groupPhysicalPlacement groupPhysicalPlacement,
	priority CellPriority,
	groupName string) (map[string]common.Set, []string) {

	preemptionVictims := map[string]common.Set{}
	var nodesHaveVictims []string
	for gpuNum := range groupPhysicalPlacement {
		for podIndex := range groupPhysicalPlacement[gpuNum] {
			for _, gpu := range groupPhysicalPlacement[gpuNum][podIndex] {
				if gpu == nil {
					continue
				}
				pGpu := gpu.(*PhysicalCell)
				if victimGroup := pGpu.GetAffinityGroup(); victimGroup != nil && victimGroup.name != groupName {
					// There are two cases of finding a running pod on the allocated resources:
					// 1. the running pod is a preemption victim.
					// 2. the running pod used resource partially released by the current group,
					// but the group wants to schedule a pod again.
					// Our principle is we allow preemption if the running pod's priority is lower than that
					// of the group to be scheduled (the 2nd case may also satisfy this condition, and we
					// allow such preemption). Otherwise the running pod cannot be preempted, and the pod
					// to be scheduled will wait.
					if pGpu.GetPriority() >= priority {
						panic(fmt.Sprintf(
							"Resources previously allocated (%v) has been allocated to "+
								"another non-preemptible group %v; pod should wait",
							pGpu.GetPhysicalPlacementString(), victimGroup.name))
					}
					// for any victim pod, gang-preempt all the other pods from the same affinity group
					for _, victims := range victimGroup.allocatedPods {
						for _, v := range victims {
							if v != nil {
								if _, ok := preemptionVictims[v.Spec.NodeName]; !ok {
									preemptionVictims[v.Spec.NodeName] = common.NewSet()
									nodesHaveVictims = append(nodesHaveVictims, v.Spec.NodeName)
								}
								preemptionVictims[v.Spec.NodeName].Add(v)
							}
						}
					}
				}
			}
		}
	}
	return preemptionVictims, nodesHaveVictims
}

// retrieveMissingPodPlacement finds the placement of a pod from the annotation of other pods in the same group
// when the pod's placement has been invalid (i.e., not found in the spec).
func retrieveMissingPodPlacement(group *AlgoAffinityGroup, gpuNum int32, podIndex int32) (api.PodPlacementInfo, string) {
	for _, pods := range group.allocatedPods {
		for _, p := range pods {
			if p != nil {
				info := internal.ExtractPodBindInfo(p)
				for _, mbi := range info.AffinityGroupBindInfo {
					if gpuNum == int32(len(mbi.PodPlacements[0].PhysicalGpuIndices)) {
						return mbi.PodPlacements[podIndex], info.CellChain
					}
				}
			}
		}
	}
	panic(fmt.Sprintf(
		"No allocated pod found in an allocated group %v when retrieving placement for pod %v with GPU number %v", group.name, podIndex, gpuNum))
}

// buddyAlloc allocates a free cell at a certain level from a free list.
// It splits a higher-level cell when there is no free cell at the current level.
// As the input cell list is a copy of the real free list and hence is one-off,
// we won't remove a returned cell from it.
func buddyAlloc(freeList ChainCellList, level CellLevel, suggestedNodes common.Set) *PhysicalCell {
	if len(freeList[level]) == 0 && level < CellLevel(len(freeList)) {
		higherCell := buddyAlloc(freeList, level+1, suggestedNodes)
		if higherCell != nil {
			freeList[level] = append(freeList[level], higherCell.GetChildren()...)
		}
	}
	if len(freeList[level]) == 0 {
		return nil
	}
	return getFewestOpporPhysicalCell(freeList[level], suggestedNodes)
}

// getFewestOpporPhysicalCell selects a physical cell with the minimum number of opportunistic pods from a cell list.
// This function will try to avoid using cells whose resources are not within the suggested nodes if possible.
// In case there is no cell within the suggested nodes, it will try to randomly return a healthy node with at least
// one opportunistic pod to try preemption. If that also fails, it just returns the min-opportunistic cell,
// regardless of if it is suggested or healthy.
func getFewestOpporPhysicalCell(cl CellList, suggestedNodes common.Set) *PhysicalCell {
	fewestOpporNum := int32(math.MaxInt32)
	fewestOpporNumSuggested := int32(math.MaxInt32)
	var fewestOpporCell *PhysicalCell
	var fewestOpporCellSuggested *PhysicalCell
	var preemptibleCells []*PhysicalCell
	for _, c := range cl {
		if pc := c.(*PhysicalCell); pc.GetVirtualCell() == nil && pc.GetPreBoundVirtualCell() == nil {
			opporNum := pc.GetUsedGpuNumAtPriorities()[opportunisticPriority]
			if opporNum < fewestOpporNum {
				fewestOpporNum = opporNum
				fewestOpporCell = pc
			}
			allNodesInSuggested := true
			nodes, _ := pc.GetPhysicalPlacement()
			for _, n := range nodes {
				if !suggestedNodes.Contains(n) {
					allNodesInSuggested = false
					break
				}
			}
			if allNodesInSuggested && opporNum < fewestOpporNumSuggested {
				fewestOpporNumSuggested = opporNum
				fewestOpporCellSuggested = pc
			}
			if opporNum > 0 {
				preemptibleCells = append(preemptibleCells, pc)
			}
		}
	}
	var selectedCell *PhysicalCell
	if fewestOpporCellSuggested != nil {
		selectedCell = fewestOpporCellSuggested
		nodes, _ := selectedCell.GetPhysicalPlacement()
		klog.Infof("Selected a cell within suggested nodes: %v, nodes %v", selectedCell.GetAddress(), common.ToJson(nodes))
	} else if len(preemptibleCells) > 0 {
		// If we cannot find a cell within suggested nodes, we will try to preempt some pods instead of
		// directly returning the fewestOpporCell (because this cell could be a bad node, we should not return it).
		// Also, we will choose a random cell, to avoid always returning the same cell (similar to above,
		// if we always return the same cell, it might be a bad node, preempting pods on a bad node won't bring
		// it back to the suggested nodes)
		selectedCell = preemptibleCells[rand.Int31n(int32(len(preemptibleCells)))]
		nodes, _ := selectedCell.GetPhysicalPlacement()
		klog.Infof("Selected a cell not within suggested nodes but preempting opportunistic pods may help: %v, nodes %v",
			selectedCell.GetAddress(), common.ToJson(nodes))
	} else if fewestOpporCell == nil {
		panic("VC Safety Broken: Cannot find any physical cell that has not been bound to a virtual cell")
	} else {
		selectedCell = fewestOpporCell
		nodes, _ := selectedCell.GetPhysicalPlacement()
		klog.Infof("Selected a cell not within suggested nodes and no preemption can help: %v, nodes %v",
			selectedCell.GetAddress(), common.ToJson(nodes))
	}
	return selectedCell
}

// mapNonPreassignedCellToPhysical maps a virtual cell (possibly inside a preassigned one) to
// one of the cell inside the physical cell of the preassigned cell. This operation keeps the
// inner-cell topology equivalent, by recursively binding the cells inside the preassigned one.
func mapNonPreassignedCellToPhysical(c *VirtualCell, suggestedNodes common.Set) *PhysicalCell {
	if c.GetPhysicalCell() != nil {
		return c.GetPhysicalCell()
	} else if c.GetPreBoundPhysicalCell() != nil {
		return c.GetPreBoundPhysicalCell()
	} else {
		parentPhysical := mapNonPreassignedCellToPhysical(c.GetParent().(*VirtualCell), suggestedNodes)
		pc := getFewestOpporPhysicalCell(parentPhysical.GetChildren(), suggestedNodes)
		if pc == nil || pc.GetPriority() > opportunisticPriority {
			panic(fmt.Sprintf("VC Safety Broken: Cannot find physical cell for %v", c.GetAddress()))
		}
		c.SetPreBoundPhysicalCell(pc)
		pc.SetPreBoundVirtualCell(c)
		return pc
	}
}

// clearPreBindings clears the temporary bindings created during scheduling.
func clearPreBindings(virtualPlacement groupVirtualPlacement) {
	for _, podPlacements := range virtualPlacement {
		for _, podGpus := range podPlacements {
			for _, gpu := range podGpus {
				for gpu != nil {
					vGpu := gpu.(*VirtualCell)
					if pGpu := vGpu.GetPreBoundPhysicalCell(); pGpu != nil {
						pGpu.SetPreBoundVirtualCell(nil)
						vGpu.SetPreBoundPhysicalCell(nil)
						gpu = gpu.GetParent()
					} else {
						break
					}
				}
			}
		}
	}
}

// getPodIndex finds the index of a pod in its group according to its placement.
func getPodIndex(podPlacements []api.PodPlacementInfo, node string, gpuIndex int32) int32 {
	for podIndex, placement := range podPlacements {
		if placement.PhysicalNode == node && common.Int32SliceContains(placement.PhysicalGpuIndices, gpuIndex) {
			return int32(podIndex)
		}
	}
	return -1
}

// mapNonPreassignedCellToVirtual maps a physical cell (possibly allocated to a non-preassigned virtual cell)
// to the corresponding virtual cell. This can be viewed as an inverse operation of mapNonPreassignedCellToPhysical,
// used for finding the virtual cell when adding an allocated pod.
func mapNonPreassignedCellToVirtual(
	c *PhysicalCell,
	vccl ChainCellList,
	preassignedLevel CellLevel,
	p CellPriority) (*VirtualCell, string) {

	if c.GetVirtualCell() != nil {
		return c.GetVirtualCell(), ""
	} else if c.GetLevel() == preassignedLevel {
		if preassignedVirtual := getLowestPriorityCell(vccl[preassignedLevel], p); preassignedVirtual == nil {
			return nil, fmt.Sprintf("insufficient free cell in the VC at the preassigned level (%v)", preassignedLevel)
		} else {
			return preassignedVirtual.(*VirtualCell), ""
		}
	} else if c.GetParent() == nil {
		return nil, fmt.Sprintf(
			"physical and virtual cell hierarchies not match (cannot reach the preassigned level %v in physical)",
			preassignedLevel)
	} else {
		parentVirtual, message := mapNonPreassignedCellToVirtual(c.GetParent().(*PhysicalCell), vccl, preassignedLevel, p)
		if parentVirtual == nil {
			return nil, message
		} else {
			return getLowestPriorityCell(parentVirtual.GetChildren(), p).(*VirtualCell), ""
		}
	}
}

// getLowestPriorityCell returns a cell with the lowest priority among the cells
// whose priorities are lower than the given priority (p).
func getLowestPriorityCell(cl CellList, p CellPriority) Cell {
	lowestPriority := maxGuaranteedPriority
	var lowestPriorityCell Cell
	for _, c := range cl {
		pp := c.GetPriority()
		if pp == freePriority {
			return c
		} else if pp < p && pp < lowestPriority {
			lowestPriority = pp
			lowestPriorityCell = c
		}
	}
	return lowestPriorityCell
}

// bindCell binds a virtual cell to a physical cell and its parent recursively.
func bindCell(pc *PhysicalCell, vc *VirtualCell) {
	for vc.GetPhysicalCell() == nil {
		pc.SetVirtualCell(vc)
		vc.SetPhysicalCell(pc)
		message := fmt.Sprintf("Cells bound: %v and %v", vc.GetAddress(), pc.GetAddress())
		if pc.IsReserved() {
			message += " (reservation)"
		}
		klog.Info(message)
		if vc.GetParent() == nil {
			break
		}
		vc = vc.GetParent().(*VirtualCell)
		pc = pc.GetParent().(*PhysicalCell)
	}
}

// unbindCell unbinds a virtual cell with a physical cell and its parent recursively.
func unbindCell(c *PhysicalCell) {
	boundVirtual := c.GetVirtualCell()
	for !boundVirtual.GetPhysicalCell().IsReserved() {
		boundPhysical := boundVirtual.GetPhysicalCell()
		klog.Infof("Cells unbound: %v and %v", boundVirtual.GetAddress(), boundPhysical.GetAddress())
		boundVirtual.SetPhysicalCell(nil)
		boundPhysical.SetVirtualCell(nil)
		if boundVirtual.GetParent() == nil {
			break
		} else {
			unbindParent := true
			for _, cc := range boundVirtual.GetParent().GetChildren() {
				if child := cc.(*VirtualCell); child.GetPhysicalCell() != nil {
					unbindParent = false
					break
				}
			}
			if !unbindParent {
				break
			}
			boundVirtual = boundVirtual.GetParent().(*VirtualCell)
		}
	}
}

// setPriority sets priority and state for a cell and its parent recursively, guaranteeing that
// (i) the priority of a cell is the max of those of its children.
// (ii) a cell is in "Used" state if any of its children is "Used", otherwise "Free".
func setPriority(c Cell, p CellPriority) {
	originalPriority := c.GetPriority()
	c.SetPriority(p)
	if parent := c.GetParent(); parent != nil {
		if p > parent.GetPriority() {
			setPriority(parent, p)
		} else if originalPriority == parent.GetPriority() && p < originalPriority {
			maxBuddyPriority := freePriority
			for _, buddy := range parent.GetChildren() {
				if buddy.GetPriority() > maxBuddyPriority {
					maxBuddyPriority = buddy.GetPriority()
				}
			}
			setPriority(parent, maxBuddyPriority)
		}
	}
}

// updateUsedGpuNumAtPriority updates the number of used GPUs at a priority for a cell
// and its parent recursively.
func updateUsedGpuNumAtPriority(c Cell, p CellPriority, increase bool) {
	for c != nil {
		delta := int32(-1)
		if increase {
			delta = 1
		}
		c.IncreaseUsedGpuNumAtPriority(p, delta)
		c = c.GetParent()
	}
}

// allPodsReleased checks if all the pods of an affinity group were released.
func allPodsReleased(allocatedPods map[int32][]*core.Pod) bool {
	for _, pods := range allocatedPods {
		for _, p := range pods {
			if p != nil {
				return false
			}
		}
	}
	return true
}

// inFreeCellList checks if a physical cell (or its ancestor) is in the global free cell list.
func inFreeCellList(c *PhysicalCell) bool {
	for {
		if c.GetVirtualCell() != nil || c.IsSplit() {
			return false
		}
		if c.GetParent() == nil || c.GetParent().(*PhysicalCell).IsSplit() {
			return true
		}
		c = c.GetParent().(*PhysicalCell)
	}
}

// generateOpporVirtualCell generates a fake virtual cell in a VC's API status
// for an opportunistic cell used by the VC.
func generateOpporVirtualCell(pc *api.PhysicalCellStatus) *api.VirtualCellStatus {
	vc := &api.VirtualCellStatus{
		CellStatus: api.CellStatus{
			GpuType:         pc.GpuType,
			CellType:        pc.CellType,
			CellAddress:     pc.CellAddress + "-opp",
			CellState:       api.CellUsed,
			CellHealthiness: pc.CellHealthiness,
			CellPriority:    api.OpportunisticPriority,
		},
		PhysicalCell: pc,
	}
	return vc
}
