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
	// all affinity groups that have been allocated or are preempting other groups
	affinityGroups map[string]*AlgoAffinityGroup

	// vcFreeCellNum, allVCFreeCellNum, and totalLeftCellNum are used to track cell usage of the VCs.
	// Note that these numbers count both healthy and bad cells.

	// number of free preassigned cells of each VC of each cell type
	vcFreeCellNum map[api.VirtualClusterName]map[CellChain]map[CellLevel]int32
	// total number of preassigned free cells in all the VCs of each cell type
	allVCFreeCellNum map[CellChain]map[CellLevel]int32
	// Number of cells left in the physical cluster of each cell type
	// (the cell is either in the free list or could be obtained by splitting an ancestor that is in the free list).
	// For example, we have a level-2 cell in the free list (which can be split into 2 level-1 cells).
	// Then the left cell numbers will be level-2: 1, level-1: 2.
	totalLeftCellNum map[CellChain]map[CellLevel]int32

	// badFreeCells, vcDoomedBadCells, and allVCDoomedBadCellNum are used to track bad cells.
	// Note that a cell is bad if ANY of its children is bad; so a cell may also contain healthy children.
	// A preassigned cell in a VC is "doomed to be bad" when the healthy free cells in the physical cluster
	// is fewer than the VC's free cells (thus certain free cells in the VC will be inevitably bound
	// to bad physical cells at this moment despite the dynamic cell binding).
	// Marking doomed bad cells can help the intra-VC scheduler avoid finding placements that will
	// be inevitably mapped to bad physical nodes. Status of doomed bad cells can also be exposed
	// to users, so that users can know if their VCs have bad nodes currently.

	// bad free cells in the physical cluster of each cell type (same definition of "free" as above)
	badFreeCells map[CellChain]ChainCellList
	// free cells in each VC that are doomed to be bad
	vcDoomedBadCells map[api.VirtualClusterName]map[CellChain]ChainCellList
	// number of doomed bad cells in all the VCs of each cell type
	allVCDoomedBadCellNum map[CellChain]map[CellLevel]int32
	// Besides bad nodes, we also avoid using nodes not suggested by K8s (configured in
	// ignoreK8sSuggestedNodes of an affinity group).
	// But note that we do NOT mark virtual cells as "doomed to be bound to non-suggested nodes"
	// (like what we do for "doomed to be bound to bad cells"),
	// because suggested nodes are not stable: they are provided by K8s during scheduling *each pod*,
	// and may change across different pods. The consequence is that, even if ignoreK8sSuggestedNodes is false
	// for an affinity group, the intra-VC scheduler may choose some placements that
	// cannot be mapped to a physical placement fully within the suggested nodes.
	// TODO: introduce randomization in intra-VC scheduling to avoid always choosing the same placement
	// that cannot be mapped to suggested nodes

	// bad nodes in the physical cluster
	badNodes common.Set
	// map each GPU type to all chains that contain this type
	cellChains map[string][]CellChain
	// map each level in a chain to the specific cell type name
	cellTypes map[CellChain]map[CellLevel]api.CellType
	// cluster status exposed to external
	apiClusterStatus api.ClusterStatus
	// lock
	algorithmLock sync.RWMutex
}

// NewHivedAlgorithm initializes a HivedAlgorithm from the config file.
func NewHivedAlgorithm(sConfig *api.Config) *HivedAlgorithm {
	fullPcl, freePcl, vcFreeCellNum, nonPinnedFullVcl, nonPinnedFreeVcl, pinnedVcl, pinnedPcl,
		gpuNums, chains, cellTypes := ParseConfig(sConfig)

	h := &HivedAlgorithm{
		vcSchedulers:            map[api.VirtualClusterName]intraVCScheduler{},
		opportunisticSchedulers: map[CellChain]*topologyAwareScheduler{},
		fullCellList:            fullPcl,
		freeCellList:            freePcl,
		vcFreeCellNum:           vcFreeCellNum,
		allVCFreeCellNum:        map[CellChain]map[CellLevel]int32{},
		totalLeftCellNum:        map[CellChain]map[CellLevel]int32{},
		badFreeCells:            map[CellChain]ChainCellList{},
		vcDoomedBadCells:        map[api.VirtualClusterName]map[CellChain]ChainCellList{},
		allVCDoomedBadCellNum:   map[CellChain]map[CellLevel]int32{},
		badNodes:                common.NewSet(),
		cellChains:              chains,
		cellTypes:               cellTypes,
		affinityGroups:          map[string]*AlgoAffinityGroup{},
		apiClusterStatus: api.ClusterStatus{
			PhysicalCluster: api.PhysicalClusterStatus{},
			VirtualClusters: map[api.VirtualClusterName]api.VirtualClusterStatus{},
		},
	}
	for vcName := range nonPinnedFullVcl {
		// TODO: Support per-VC configurable intra VC scheduling algo.
		h.vcSchedulers[vcName] = newDefaultIntraVCScheduler(
			nonPinnedFullVcl[vcName], nonPinnedFreeVcl[vcName], pinnedVcl[vcName], gpuNums)
	}
	for chain, ccl := range h.fullCellList {
		h.opportunisticSchedulers[chain] = NewTopologyAwareScheduler(ccl, gpuNums[chain], false)
	}
	h.initCellNums()
	h.initAPIClusterStatus()
	h.initPinnedCells(pinnedPcl)
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

	klog.Infof("[%v]: Scheduling pod in %v phase...", internal.Key(pod), phase)
	s := internal.ExtractPodSchedulingSpec(pod)
	suggestedNodeSet := common.NewSet()
	for _, n := range suggestedNodes {
		suggestedNodeSet.Add(n)
	}
	var (
		groupPhysicalPlacement groupPhysicalPlacement // GPU number -> a set of pods -> a set of GPUs of each pod
		groupVirtualPlacement  groupVirtualPlacement  // GPU number -> a set of pods -> a set of GPUs of each pod
		preemptionVictims      map[string]common.Set  // node -> pods
		waitReason             string
		podIndex               int32 // index of current pod among those of the same GPU number in the group, 0 by default
	)

	if g := h.affinityGroups[s.AffinityGroup.Name]; g != nil {
		groupPhysicalPlacement, groupVirtualPlacement, preemptionVictims, podIndex =
			h.schedulePodFromExistingGroup(g, s, suggestedNodeSet, phase, pod)
	}
	// we need to re-evaluate the existence of the group here (instead of an "else") because it is
	// possible that the group was a preempting group and deleted in h.schedulePodFromExistingGroup
	if h.affinityGroups[s.AffinityGroup.Name] == nil {
		groupPhysicalPlacement, groupVirtualPlacement, preemptionVictims, waitReason =
			h.schedulePodFromNewGroup(s, suggestedNodeSet, phase, pod)
	}
	return generatePodScheduleResult(
		groupPhysicalPlacement,
		groupVirtualPlacement,
		preemptionVictims,
		waitReason,
		h.cellTypes,
		s.GpuNumber,
		podIndex,
		h.affinityGroups[s.AffinityGroup.Name],
		s.AffinityGroup.Name,
		suggestedNodeSet,
		pod)
}

func (h *HivedAlgorithm) AddUnallocatedPod(*core.Pod) {
}

func (h *HivedAlgorithm) DeleteUnallocatedPod(pod *core.Pod) {
	h.algorithmLock.Lock()
	defer h.algorithmLock.Unlock()

	s := internal.ExtractPodSchedulingSpec(pod)
	if g := h.affinityGroups[s.AffinityGroup.Name]; g != nil && g.state == groupPreempting {
		if g.preemptingPods[pod.UID] != nil {
			klog.Infof("[%v]: Deleting preempting pod from affinity group %v...", internal.Key(pod), g.name)
			delete(g.preemptingPods, pod.UID)
		}
		if len(g.preemptingPods) == 0 {
			klog.Infof("[%v]: Canceling affinity group %v's preemption because its pods are all deleted",
				internal.Key(pod), g.name)
			h.deletePreemptingAffinityGroup(g, pod)
		}
	}
}

func (h *HivedAlgorithm) AddAllocatedPod(pod *core.Pod) {
	h.algorithmLock.Lock()
	defer h.algorithmLock.Unlock()

	s := internal.ExtractPodSchedulingSpec(pod)
	info := internal.ExtractPodBindInfo(pod)
	klog.Infof("[%v]: Adding allocated pod to affinity group %v...", internal.Key(pod), s.AffinityGroup.Name)
	klog.Infof("[%v]: Adding to node %v, GPUs %v", internal.Key(pod), info.Node, common.ToJson(info.GpuIsolation))

	podIndex := int32(0)
	if g := h.affinityGroups[s.AffinityGroup.Name]; g != nil {
		if g.state == groupPreempting {
			h.allocatePreemptingAffinityGroup(g, pod)
		}
		if podIndex = getAllocatedPodIndex(info, s.GpuNumber); podIndex == -1 {
			klog.Errorf("[%v]: Pod placement not found in group %v: node %v, GPUs %v",
				internal.Key(pod), s.AffinityGroup.Name, info.Node, info.GpuIsolation)
			return
		}
	} else {
		h.createAllocatedAffinityGroup(s, info, pod)
	}
	h.affinityGroups[s.AffinityGroup.Name].allocatedPods[s.GpuNumber][podIndex] = pod
}

func (h *HivedAlgorithm) DeleteAllocatedPod(pod *core.Pod) {
	h.algorithmLock.Lock()
	defer h.algorithmLock.Unlock()

	s := internal.ExtractPodSchedulingSpec(pod)
	info := internal.ExtractPodBindInfo(pod)
	klog.Infof("[%v]: Deleting allocated pod from affinity group %v...", internal.Key(pod), s.AffinityGroup.Name)
	klog.Infof("[%v]: Deleting from node %v, GPUs %v", internal.Key(pod), info.Node, common.ToJson(info.GpuIsolation))

	if g := h.affinityGroups[s.AffinityGroup.Name]; g == nil {
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
	for _, aag := range h.affinityGroups {
		ags.Items = append(ags.Items, aag.ToAffinityGroup())
	}

	return ags
}

func (h *HivedAlgorithm) GetAffinityGroup(name string) api.AffinityGroup {
	h.algorithmLock.RLock()
	defer h.algorithmLock.RUnlock()

	if aag := h.affinityGroups[name]; aag != nil {
		return aag.ToAffinityGroup()
	}

	panic(internal.NewBadRequestError(fmt.Sprintf(
		"Affinity group %v does not exist since it is not allocated or preempting",
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

	if vcs, ok := h.apiClusterStatus.VirtualClusters[vcn]; ok {
		return vcs.DeepCopy()
	}
	panic(internal.NewBadRequestError(fmt.Sprintf("VC %v not found", vcn)))
}

// initCellNums initiates the data structures for tracking cell usages and healthiness,
// i.e., h.allVCFreeCellNum, h.totalLeftCellNum, h.badFreeCells, h.vcDoomedBadCells, and h.allVCDoomedBadCellNum.
// This method also validates the initial cell assignment to the VCs to make sure that
// all the assigned cells can be fit into the configured physical cells.
func (h *HivedAlgorithm) initCellNums() {
	for vc, vcFreeCellNum := range h.vcFreeCellNum {
		h.vcDoomedBadCells[vc] = map[CellChain]ChainCellList{}
		for chain, chainFreeCellNum := range vcFreeCellNum {
			h.vcDoomedBadCells[vc][chain] = ChainCellList{}
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
			h.badFreeCells[chain] = ChainCellList{}
			h.allVCDoomedBadCellNum[chain] = map[CellLevel]int32{}
			h.totalLeftCellNum[chain][top] = available
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
		for _, ccl := range vcs.getNonPinnedPreassignedCells() {
			for _, cl := range ccl {
				for _, c := range cl {
					h.apiClusterStatus.VirtualClusters[vc] = append(
						h.apiClusterStatus.VirtualClusters[vc], c.(*VirtualCell).GetAPIStatus())
				}
			}
		}
		for _, ccl := range vcs.getPinnedCells() {
			for _, c := range ccl[CellLevel(len(ccl))] {
				h.apiClusterStatus.VirtualClusters[vc] = append(
					h.apiClusterStatus.VirtualClusters[vc], c.(*VirtualCell).GetAPIStatus())
			}
		}
	}
}

// initPinnedCells creates static bindings for the pinned cells, and removes the
// pinned physical cells from the free cell list.
func (h *HivedAlgorithm) initPinnedCells(pinnedCells map[api.VirtualClusterName]map[api.PinnedCellId]*PhysicalCell) {
	klog.Info("Init pinned cells")
	for vcn, vcPinnedCells := range pinnedCells {
		for pid, pinnedPhysical := range vcPinnedCells {
			h.allocatePreassignedCell(pinnedPhysical, vcn)
			virtualList := h.vcSchedulers[vcn].getPinnedCells()[pid]
			pinnedVirtual := virtualList[CellLevel(len(virtualList))][0].(*VirtualCell)
			bindCell(pinnedPhysical, pinnedVirtual)
		}
	}
}

// initBadNodes marks all the physical nodes defined in the config as bad,
// and waits for K8s's AddNode calls to inform the healthy nodes in them.
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

// setBadCell marks a physical cell (and also the virtual cell it is bound to) as bad,
// and recursively for its parent, guaranteeing that a cell is bad if any of its children is bad.
// setBadCell always starts from the lowest level, i.e., GPU-level cells.
func (h *HivedAlgorithm) setBadCell(c *PhysicalCell) {
	if !c.IsHealthy() {
		return
	}
	c.SetHealthiness(api.CellBad)
	if c.GetParent() != nil {
		h.setBadCell(c.GetParent().(*PhysicalCell))
	}
	if inFreeCellList(c) {
		h.addBadFreeCell(c)
	} else if c.GetVirtualCell() == nil && !c.IsSplit() {
		// if c's ancestor has been bound to a virtual cell, we should also bind c
		// so that the VC scheduler can see this cell failure
		vc := getUnboundVirtualCell(c.GetParent().(*PhysicalCell).GetVirtualCell().GetChildren())
		c.SetVirtualCell(vc)
		vc.SetPhysicalCell(c)
		klog.Infof("Virtual cell %v is bound to physical cell %v", vc.GetAddress(), c.GetAddress())
	}
}

// setHealthyCell marks a physical cell (and also the virtual cell it is bound to) as healthy,
// and recursively for its parent, guaranteeing that a cell is healthy if all of its children are healthy.
// setHealthy always starts from the lowest level, i.e., GPU-level cells.
func (h *HivedAlgorithm) setHealthyCell(c *PhysicalCell) {
	if c.IsHealthy() {
		return
	}
	c.SetHealthiness(api.CellHealthy)
	if inFreeCellList(c) {
		h.removeBadFreeCell(c)
	} else if vc := c.GetVirtualCell(); vc != nil {
		if !c.IsPinned() && c.GetPriority() < minGuaranteedPriority {
			// The cell is not in real use, the binding was created only because the cell was bad.
			// We should unbind this cell as it is healthy now.
			c.SetVirtualCell(nil)
			vc.SetPhysicalCell(nil)
			klog.Infof("Virtual cell %v is unbound from physical cell %v",
				vc.GetAddress(), c.GetAddress())
			if vc.GetParent() == nil {
				// this is a preassigned cell, it must be a doomed bad cell
				// (otherwise it must have been released before)
				h.vcDoomedBadCells[vc.GetVirtualCluster()][c.GetChain()].remove(c, c.GetLevel())
				h.allVCDoomedBadCellNum[c.GetChain()][c.GetLevel()]--
				h.releasePreassignedCell(c, vc.GetVirtualCluster())
			}
		}
	}
	if c.GetParent() == nil {
		return
	} else {
		for _, buddy := range c.GetParent().GetChildren() {
			if !buddy.(*PhysicalCell).IsHealthy() {
				return
			}
		}
	}
	h.setHealthyCell(c.GetParent().(*PhysicalCell))
}

// addBadFreeCell adds a physical cell to the bad free cells, and checks if
// the healthy cells in the physical cluster can satisfy the free cells in the VCs.
func (h *HivedAlgorithm) addBadFreeCell(c *PhysicalCell) {
	chain := c.GetChain()
	level := c.GetLevel()
	h.badFreeCells[chain][level] = append(h.badFreeCells[chain][level], c)
	// compare the free cells in the VCs and the healthy free cells in the physical cluster
	if h.allVCFreeCellNum[chain][level] >
		h.totalLeftCellNum[chain][level]-int32(len(h.badFreeCells[chain][level])) {
		// in the log we also count the cells marked as doomed bad in the free cells
		// (they have been excluded from allVCFreeCellNum)
		klog.Warningf("Cell type %v (chain %v level %v) now has fewer healthy cells (%v) "+
			"than the total free cells of all the VCs (%v). Certain VCs' cells may be doomed to be bad.",
			h.cellTypes[chain][level], chain, level,
			h.totalLeftCellNum[chain][level]-int32(len(h.badFreeCells[chain][level])),
			h.allVCFreeCellNum[chain][level]+h.allVCDoomedBadCellNum[chain][level])
		h.tryBindDoomedBadCell(chain, level)
	}
}

// removeBadFreeCell removes a physical cell from the bad free cells, and checks if
// the healthy cells in the physical cluster can satisfy the free cells in the VCs.
func (h *HivedAlgorithm) removeBadFreeCell(c *PhysicalCell) {
	chain := c.GetChain()
	level := c.GetLevel()
	h.badFreeCells[chain].remove(c, level)
	// when checking if all doomed bad cells can be released, we should add the number of
	// the doomed bad cells to the L.H.S. of the comparison below (otherwise,
	// h.allVCFreeCellNum[chain][level] <= R.H.S. always holds after we bind the doomed bad cells)
	if h.allVCFreeCellNum[chain][level]+h.allVCDoomedBadCellNum[chain][level] <=
		h.totalLeftCellNum[chain][level]-int32(len(h.badFreeCells[chain][level])) {
		klog.Infof("Cell type %v (chain %v level %v) now has sufficient healthy cells (%v) for "+
			"allocating the total free cells of all the VCs (%v). No VC cell will be doomed to be bad.",
			h.cellTypes[chain][level], chain, level,
			h.totalLeftCellNum[chain][level]-int32(len(h.badFreeCells[chain][level])),
			h.allVCFreeCellNum[chain][level]+h.allVCDoomedBadCellNum[chain][level])
	}
	h.tryUnbindDoomedBadCell(chain, level)
}

// tryBindDoomedBadCell checks if the healthy cells in the physical cluster can satisfy the free cells
// in each VC. If not, we will mark some of the virtual cells in the VC as bad (i.e., doomed to be bad).
func (h *HivedAlgorithm) tryBindDoomedBadCell(c CellChain, l CellLevel) {
	for vcName, vcFreeNum := range h.vcFreeCellNum {
		if _, ok := vcFreeNum[c]; !ok {
			continue
		}
		for vcFreeNum[c][l] >
			h.totalLeftCellNum[c][l]-int32(len(h.badFreeCells[c][l])) {
			pc := h.badFreeCells[c][l][0].(*PhysicalCell)
			vc := getUnboundVirtualCell(h.vcSchedulers[vcName].getNonPinnedPreassignedCells()[c][l])
			pc.SetVirtualCell(vc)
			vc.SetPhysicalCell(pc)
			klog.Infof("Virtual cell %v is bound to physical cell %v", vc.GetAddress(), pc.GetAddress())
			klog.Warningf("Cell type %v (chain %v level %v) now has fewer healthy cells (%v) "+
				"than the free cells of the VC %v (%v). "+
				"Cell %v is doomed to be bad and bound to %v.",
				h.cellTypes[c][l], c, l,
				h.totalLeftCellNum[c][l]-int32(len(h.badFreeCells[c][l])),
				vcName, vcFreeNum[c][l]+int32(len(h.vcDoomedBadCells[vcName][c][l])),
				vc.GetAddress(), pc.GetAddress())
			h.vcDoomedBadCells[vcName][c][l] = append(h.vcDoomedBadCells[vcName][c][l], pc)
			h.allVCDoomedBadCellNum[c][l]++
			h.allocatePreassignedCell(pc, vcName)
		}
	}
}

// tryUnbindDoomedBadCell checks if some VCs' doomed bad cells can now be satisfied by healthy cells.
// If so, we will release these cells.
func (h *HivedAlgorithm) tryUnbindDoomedBadCell(c CellChain, l CellLevel) {
	for vcName, vcFreeNum := range h.vcFreeCellNum {
		if _, ok := vcFreeNum[c]; !ok {
			continue
		}
		for len(h.vcDoomedBadCells[vcName][c][l]) != 0 &&
			vcFreeNum[c][l] < h.totalLeftCellNum[c][l]-int32(len(h.badFreeCells[c][l])) {
			pc := h.vcDoomedBadCells[vcName][c][l][0].(*PhysicalCell)
			klog.Infof("Cell type %v (chain %v level %v) now has more healthy free cells (%v) "+
				"than the free cells of the VC %v (%v excluding the doomed bad cells). "+
				"Cell %v is no longer doomed to be bad and is unbound from %v.",
				h.cellTypes[c][l], c, l,
				h.totalLeftCellNum[c][l]-int32(len(h.badFreeCells[c][l])),
				vcName, vcFreeNum[c][l], pc.GetVirtualCell().GetAddress(), pc.GetAddress())
			pc.GetVirtualCell().SetPhysicalCell(nil)
			pc.SetVirtualCell(nil)
			h.vcDoomedBadCells[vcName][c].remove(pc, l)
			h.allVCDoomedBadCellNum[c][l]--
			h.releasePreassignedCell(pc, vcName)
		}
	}
}

// schedulePodFromExistingGroup schedules a pod from an allocated or preempting affinity group.
// If it is from an allocated group, we will schedule the pod to the corresponding placement.
// If it is from a preempting group, we will continue its preemption, or schedule it when the preemption is done.
func (h *HivedAlgorithm) schedulePodFromExistingGroup(
	g *AlgoAffinityGroup,
	s *api.PodSchedulingSpec,
	suggestedNodes common.Set,
	phase internal.SchedulingPhase,
	pod *core.Pod) (
	groupPhysicalPlacement groupPhysicalPlacement,
	groupVirtualPlacement groupVirtualPlacement,
	preemptionVictims map[string]common.Set,
	podIndex int32) {

	badOrNonSuggestedNodes := collectBadOrNonSuggestedNodes(
		g.physicalGpuPlacement, suggestedNodes, g.ignoreK8sSuggestedNodes)
	// state of an existing group can be either Allocated or Preempting
	if g.state == groupAllocated {
		klog.Infof("[%v]: Pod is from an affinity group that is already allocated: %v",
			internal.Key(pod), s.AffinityGroup.Name)
		groupPhysicalPlacement = g.physicalGpuPlacement
		groupVirtualPlacement = g.virtualGpuPlacement
		if !badOrNonSuggestedNodes.IsEmpty() {
			// for an allocated group, we always insist the previous scheduling decision
			// even if some pods are now bad or not within suggested nodes
			klog.Warningf("[%v]: Some nodes allocated to affinity group %v are no longer "+
				"healthy and within K8s suggested nodes: %v", internal.Key(pod), g.name, badOrNonSuggestedNodes)
		}
		if podIndex = getNewPodIndex(g.allocatedPods[s.GpuNumber]); podIndex == -1 {
			panic(internal.NewBadRequestError(fmt.Sprintf(
				"Requesting more pods than the configured number for %v GPUs (%v pods) in affinity group %v",
				s.GpuNumber, g.totalPodNums[s.GpuNumber], s.AffinityGroup.Name)))
		}
	} else { // groupPreempting
		klog.Infof("[%v]: Pod is from an affinity group that is preempting others: %v",
			internal.Key(pod), s.AffinityGroup.Name)
		if phase == internal.PreemptingPhase && !badOrNonSuggestedNodes.IsEmpty() {
			// If we find a preempting group's placement is not fully healthy and within suggested nodes,
			// we should cancel the preemption so as to reschedule it to other places.
			// We should do this only in Preempting phase
			// because only suggested nodes of this phase consider preemption.
			klog.Infof("[%v]: Canceling affinity group %v's preemption because its placement is "+
				"no longer fully healthy and within Preempting-phase suggested nodes: %v",
				internal.Key(pod), g.name, badOrNonSuggestedNodes)
			h.deletePreemptingAffinityGroup(g, pod)
		} else {
			groupPhysicalPlacement = g.physicalGpuPlacement
			groupVirtualPlacement = g.virtualGpuPlacement
			preemptionVictims, _ = collectPreemptionVictims(groupPhysicalPlacement)
			if len(preemptionVictims) == 0 {
				klog.Infof(
					"Preemption victims have been cleaned up for the preemptor affinity group %v", g.name)
			}
			g.preemptingPods[pod.UID] = pod
		}
	}
	return groupPhysicalPlacement, groupVirtualPlacement, preemptionVictims, podIndex
}

// schedulePodFromNewGroup schedules a pod from a new affinity group, find placement for the group,
// and checks if the group needs preemption.
func (h *HivedAlgorithm) schedulePodFromNewGroup(
	s *api.PodSchedulingSpec,
	suggestedNodes common.Set,
	phase internal.SchedulingPhase,
	pod *core.Pod) (
	groupPhysicalPlacement groupPhysicalPlacement,
	groupVirtualPlacement groupVirtualPlacement,
	preemptionVictims map[string]common.Set,
	waitReason string) {

	groupPhysicalPlacement, groupVirtualPlacement, waitReason = h.scheduleNewAffinityGroup(
		pod, s, suggestedNodes)
	if groupPhysicalPlacement == nil {
		return nil, nil, nil, waitReason
	}
	preemptionVictims, overlappingPreemptors := collectPreemptionVictims(groupPhysicalPlacement)
	// we allow a new preemption only when in Preempting phase
	// and the placement is fully within suggested nodes
	if phase == internal.PreemptingPhase {
		// first cancel preemption of other groups whose resources overlap with the current group
		for preemptor := range overlappingPreemptors.Items() {
			klog.Infof("[%v]: Canceling affinity group %v's preemption because it is "+
				"further preempted by a higher-priority affinity group %v",
				internal.Key(pod), preemptor.(*AlgoAffinityGroup).name, s.AffinityGroup.Name)
			h.deletePreemptingAffinityGroup(preemptor.(*AlgoAffinityGroup), pod)
		}
		if len(preemptionVictims) != 0 {
			// create preemption state to avoid resource contention among multiple preemptors
			h.createPreemptingAffinityGroup(s, groupPhysicalPlacement, groupVirtualPlacement, pod)
		}
	} else if len(preemptionVictims) != 0 {
		// here we won't create preemption state since we call preempt only in Preempting phase
		klog.Infof("[%v]: Found preemption victims %v in non-Preempting phase, skipping it",
			internal.Key(pod), victimsToString(preemptionVictims))
	}
	return groupPhysicalPlacement, groupVirtualPlacement, preemptionVictims, waitReason
}

// scheduleNewAffinityGroup schedules each pod of a new affinity group to a set of GPUs
// (in both the physical cluster and the VC). This is the entrance of a new scheduling attempt.
func (h *HivedAlgorithm) scheduleNewAffinityGroup(
	pod *core.Pod,
	s *api.PodSchedulingSpec,
	suggestedNodes common.Set) (
	physicalPlacement groupPhysicalPlacement,
	virtualPlacement groupVirtualPlacement,
	failedReason string) {

	klog.Infof("[%v]: Scheduling new affinity group %v", internal.Key(pod), s.AffinityGroup.Name)
	priority := CellPriority(s.Priority)
	sr := schedulingRequest{
		vc:                   s.VirtualCluster,
		pinnedCellId:         s.PinnedCellId,
		priority:             priority,
		affinityGroupName:    s.AffinityGroup.Name,
		affinityGroupPodNums: map[int32]int32{},
		suggestedNodes:       suggestedNodes,
		ignoreSuggestedNodes: s.IgnoreK8sSuggestedNodes,
	}
	for _, m := range s.AffinityGroup.Members {
		// we will merge group members with same GPU number
		sr.affinityGroupPodNums[m.GpuNumber] += m.PodNumber
	}
	h.validateSchedulingRequest(sr, pod)
	if sr.pinnedCellId != "" {
		klog.Infof("Using pinned cell %v", s.PinnedCellId)
		physicalPlacement, virtualPlacement, failedReason = h.handleSchedulingRequest(sr)
	} else if s.GpuType != "" {
		if _, ok := h.cellChains[s.GpuType]; !ok {
			panic(internal.NewBadRequestError(fmt.Sprintf(
				"[%v]: Pod requesting GPU type %v which the whole cluster does not have",
				internal.Key(pod), s.GpuType)))
		}
		klog.Infof("Using specified GPU type %v", s.GpuType)
		physicalPlacement, virtualPlacement, failedReason = h.scheduleAffinityGroupForGpuType(
			sr, s.GpuType, pod, true)
	} else {
		physicalPlacement, virtualPlacement, failedReason = h.scheduleAffinityGroupForAnyGpuType(sr, pod)
	}
	return physicalPlacement, virtualPlacement, failedReason
}

// scheduleAffinityGroupForGpuType schedules an affinity group in a certain cell chain
// that matches the given GPU type.
func (h *HivedAlgorithm) scheduleAffinityGroupForGpuType(
	sr schedulingRequest,
	gpuType string,
	pod *core.Pod,
	typeSpecified bool) (
	physicalPlacement groupPhysicalPlacement,
	virtualPlacement groupVirtualPlacement,
	failedReason string) {

	vcHasType := false
	for _, chain := range h.cellChains[gpuType] {
		if sr.priority < minGuaranteedPriority ||
			h.vcSchedulers[sr.vc].getNonPinnedPreassignedCells()[chain] != nil {
			vcHasType = true
			klog.Infof("Searching chain %v", chain)
			sr.chain = chain
			physicalPlacement, virtualPlacement, failedReason =
				h.handleSchedulingRequest(sr)
			if physicalPlacement != nil {
				return physicalPlacement, virtualPlacement, ""
			}
		}
	}
	if typeSpecified && sr.priority >= minGuaranteedPriority && !vcHasType {
		panic(internal.NewBadRequestError(fmt.Sprintf(
			"[%v]: Pod requesting GPU type %v which VC %v does not have",
			internal.Key(pod), gpuType, sr.vc)))
	}
	return nil, nil, failedReason
}

// scheduleAffinityGroupForAnyGpuType schedules an affinity group in every possible GPU type
// (when the user does not specify a GPU type).
func (h *HivedAlgorithm) scheduleAffinityGroupForAnyGpuType(
	sr schedulingRequest,
	pod *core.Pod) (
	groupPhysicalPlacement,
	groupVirtualPlacement,
	string) {

	var failedReason string
	for gpuType := range h.cellChains {
		klog.Infof("Searching GPU type %v", gpuType)
		typePhysicalPlacement, typeVirtualPlacement, typeFailedReason :=
			h.scheduleAffinityGroupForGpuType(sr, gpuType, pod, false)
		if typePhysicalPlacement != nil {
			return typePhysicalPlacement, typeVirtualPlacement, ""
		}
		if typeFailedReason != "" {
			failedReason = typeFailedReason
		}
	}
	return nil, nil, failedReason
}

// validateSchedulingRequest checks the existence of VC and pinned cell, and the legality of priority.
func (h *HivedAlgorithm) validateSchedulingRequest(sr schedulingRequest, pod *core.Pod) {
	var message string
	if h.vcSchedulers[sr.vc] == nil {
		message = fmt.Sprintf("VC %v does not exists!", sr.vc)
	} else if sr.pinnedCellId != "" {
		if h.vcSchedulers[sr.vc].getPinnedCells()[sr.pinnedCellId] == nil {
			message = fmt.Sprintf("VC %v does not have pinned cell %v", sr.vc, sr.pinnedCellId)
		} else if sr.priority == opportunisticPriority {
			message = fmt.Sprintf("opportunistic pod not supported to use pinned cell %v", sr.pinnedCellId)
		}
	}
	if message != "" {
		panic(internal.NewBadRequestError(fmt.Sprintf("[%v]: %v", internal.Key(pod), message)))
	}
}

// handleSchedulingRequest feeds a request to a VC scheduler or the opportunistic scheduler depending on its priority.
func (h *HivedAlgorithm) handleSchedulingRequest(
	sr schedulingRequest) (
	physicalPlacement groupPhysicalPlacement,
	virtualPlacement groupVirtualPlacement,
	failedReason string) {

	str := fmt.Sprintf("chain %v", sr.chain)
	if sr.pinnedCellId != "" {
		str = fmt.Sprintf("pinned cell %v", sr.pinnedCellId)
	}
	klog.Infof("Processing scheduling request: %v, GPU numbers %v, priority %v",
		str, common.ToJson(sr.affinityGroupPodNums), sr.priority)
	if sr.priority >= minGuaranteedPriority {
		physicalPlacement, virtualPlacement, failedReason = h.scheduleGuaranteedAffinityGroup(sr)
	} else {
		physicalPlacement, failedReason = h.scheduleOpportunisticAffinityGroup(sr)
	}
	if physicalPlacement == nil {
		klog.Infof("Cannot find placement in %v: %v", str, failedReason)
		return nil, nil, failedReason
	}
	klog.Infof("Found placement in %v: %v", str, physicalPlacement)
	return physicalPlacement, virtualPlacement, ""
}

// scheduleGuaranteedAffinityGroup schedules an affinity group in its VC,
// and then maps the placement in VC to the physical cluster.
func (h *HivedAlgorithm) scheduleGuaranteedAffinityGroup(
	sr schedulingRequest) (
	physicalPlacement groupPhysicalPlacement,
	virtualPlacement groupVirtualPlacement,
	failedReason string) {

	// schedule in VC
	virtualPlacement, failedReason = h.vcSchedulers[sr.vc].schedule(sr)
	if virtualPlacement == nil {
		return nil, nil, failedReason
	}
	// map the vc placement to the physical cluster
	bindings := map[api.CellAddress]*PhysicalCell{}
	gpuNums := common.Int32MapKeys(sr.affinityGroupPodNums)
	common.SortInt32(gpuNums)
	lazyPreemptedGroups := h.tryLazyPreempt(virtualPlacement, gpuNums, sr.affinityGroupName)
	preassignedCells, nonPreassignedCells := virtualPlacement.toBindingPaths(gpuNums, bindings)
	if ok := mapVirtualPlacementToPhysical(
		preassignedCells,
		nonPreassignedCells,
		h.freeCellList[sr.chain].shallowCopy(),
		sr.suggestedNodes,
		sr.ignoreSuggestedNodes,
		bindings); ok {
		return virtualPlacement.toPhysicalPlacement(bindings, gpuNums), virtualPlacement, ""
	}
	for groupName, placement := range lazyPreemptedGroups {
		h.revertLazyPreempt(h.affinityGroups[groupName], placement)
	}
	failedNodeType := "bad or non-suggested"
	if sr.ignoreSuggestedNodes {
		failedNodeType = "bad"
	}
	return nil, nil, fmt.Sprintf(
		"Mapping the virtual placement would need to use at least one %v node "+
			"(virtual placement : %v)", failedNodeType, virtualPlacement)
}

// tryLazyPreempt tries to lazy preempt the affinity groups found on a placement.
func (h *HivedAlgorithm) tryLazyPreempt(
	p groupVirtualPlacement,
	gpuNums []int32,
	groupName string) map[string]groupVirtualPlacement {

	preemptedGroups := map[string]groupVirtualPlacement{}
	for _, podGpuNum := range gpuNums {
		podPlacements := p[podGpuNum]
		for _, pod := range podPlacements {
			for _, gpu := range pod {
				if pGpu := gpu.(*VirtualCell).GetPhysicalCell(); pGpu != nil {
					if pGpu.GetState() == cellUsed && pGpu.GetUsingGroup().lazyPreemptionEnable {
						preemptedGroups[pGpu.GetUsingGroup().name] = h.lazyPreemptAffinityGroup(
							pGpu.GetUsingGroup(), groupName)
					}
				}
			}
		}
	}
	return preemptedGroups
}

// scheduleOpportunisticAffinityGroup calls the opportunistic pod scheduler to schedule an affinity group.
func (h *HivedAlgorithm) scheduleOpportunisticAffinityGroup(
	sr schedulingRequest) (
	placement groupPhysicalPlacement,
	failedReason string) {

	placement, failedReason = h.opportunisticSchedulers[sr.chain].Schedule(
		sr.affinityGroupPodNums, opportunisticPriority, sr.suggestedNodes, sr.ignoreSuggestedNodes)
	if placement == nil {
		return nil, fmt.Sprintf("%v when scheduling in physical cluster", failedReason)
	}
	return placement, ""
}

// createAllocatedAffinityGroup creates a new affinity group and allocate the resources.
func (h *HivedAlgorithm) createAllocatedAffinityGroup(s *api.PodSchedulingSpec, info *api.PodBindInfo, pod *core.Pod) {
	klog.Infof("[%v]: Creating new allocated affinity group: %v", internal.Key(pod), s.AffinityGroup.Name)
	newGroup := newAlgoAffinityGroup(
		s.AffinityGroup, s.VirtualCluster, s.LazyPreemptionEnable, s.Priority, groupAllocated)
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
						if inFreeCellList(pGpu) && vGpu.GetPreassignedCell().GetPriority() > freePriority {
							// This means we decide to bind this cell to a virtual cell whose preassigned cell
							// has been bound (in cases like reconfiguration and the VC's cells are fewer than before).
							// We need to destroy the previous binding, by lazy preempting all the groups
							// in the preassigned cell
							h.lazyPreemptCell(vGpu.GetPreassignedCell(), newGroup.name)
						}
					} else {
						shouldLazyPreempt = shouldLazyPreempt || *lazyPreempt
					}
					// Even if we have successfully found the vGpu and pGpu, there is still one possibility
					// that we should not bind them: allocating the physical cell may lead to broken safety.
					// Such case won't happen by design as buddy alloc guarantees safety; but this could
					// happen due to inconsistency of VC assignments for reasons like reconfiguration.
					// In this case, we will lazy preempt this affinity group.
					safetyOk, reason := h.allocateGpu(pGpu, vGpu, CellPriority(s.Priority), newGroup.vc)
					pGpu.AddUsingGroup(newGroup)
					setCellState(pGpu, cellUsed)
					if !safetyOk {
						shouldLazyPreempt = true
						klog.Warningf("[%v]: %v", internal.Key(pod), reason)
					}
				}
			}
		}
	}
	if shouldLazyPreempt {
		h.lazyPreemptAffinityGroup(newGroup, newGroup.name)
	}
	h.affinityGroups[s.AffinityGroup.Name] = newGroup
	klog.Infof("[%v]: New allocated affinity group created: %v", internal.Key(pod), s.AffinityGroup.Name)
}

// deleteAllocatedAffinityGroup deletes a new affinity group and release the resources (that are not
// allocated to a preempting group).
func (h *HivedAlgorithm) deleteAllocatedAffinityGroup(g *AlgoAffinityGroup, pod *core.Pod) {
	klog.Infof("[%v]: All pods complete, deleting allocated affinity group: %v",
		internal.Key(pod), g.name)
	for _, podPlacements := range g.physicalGpuPlacement {
		for _, podPlacement := range podPlacements {
			for _, gpu := range podPlacement {
				if gpu == nil {
					continue
				}
				pGpu := gpu.(*PhysicalCell)
				pGpu.DeleteUsingGroup(g)
				// state of pGpu can be either Used or Reserving
				if pGpu.GetState() == cellUsed {
					h.releaseGpu(pGpu, g.vc)
					setCellState(pGpu, cellFree)
				} else { // cellReserving
					// When pGpu is in Reserving state, we shouldn't call h.releaseGpu
					// because it must have been allocated to the reserving group before
					setCellState(pGpu, cellReserved)
				}
			}
		}
	}
	delete(h.affinityGroups, g.name)
	klog.Infof("[%v]: Allocated affinity group deleted: %v", internal.Key(pod), g.name)
}

// createPreemptingAffinityGroup creates a new affinity group that is preempting some other groups.
// Its resources are immediately allocated to the group (even if the preemption victims have not yet been deleted),
// so that other groups will not be scheduled to the same placement (unless they have higher priorities).
// This avoids the case where multiple groups preempt the same victims simultaneously, which may cause resource deadlock.
func (h *HivedAlgorithm) createPreemptingAffinityGroup(
	s *api.PodSchedulingSpec,
	physicalPlacement groupPhysicalPlacement,
	virtualPlacement groupVirtualPlacement,
	pod *core.Pod) {

	klog.Infof("[%v]: Creating new preempting affinity group: %v", internal.Key(pod), s.AffinityGroup.Name)
	newGroup := newAlgoAffinityGroup(
		s.AffinityGroup, s.VirtualCluster, s.LazyPreemptionEnable, s.Priority, groupPreempting)
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
				pGpu.AddReservingOrReservedGroup(newGroup)
				// state of pGpu can be either Used or Free (if it was Reserving or Reserved,
				// we must have canceled the ongoing preemption before, in h.Schedule)
				if pGpu.GetState() == cellUsed {
					setCellState(pGpu, cellReserving)
				} else { // cellFree
					setCellState(pGpu, cellReserved)
				}
			}
		}
	}
	newGroup.preemptingPods[pod.UID] = pod
	h.affinityGroups[s.AffinityGroup.Name] = newGroup
	klog.Infof("[%v]: New preempting affinity group created: %v", internal.Key(pod), newGroup.name)
}

// deletePreemptingAffinityGroup revokes a preemption and deletes the affinity group that is
// still waiting for the completion of the preemption.
func (h *HivedAlgorithm) deletePreemptingAffinityGroup(g *AlgoAffinityGroup, pod *core.Pod) {
	for gpuNum := range g.physicalGpuPlacement {
		for podIndex := range g.physicalGpuPlacement[gpuNum] {
			for _, gpu := range g.physicalGpuPlacement[gpuNum][podIndex] {
				pGpu := gpu.(*PhysicalCell)
				h.releaseGpu(pGpu, g.vc)
				pGpu.DeleteReservingOrReservedGroup(pGpu.GetReservingOrReservedGroup())
				// state of pGpu can be either Reserving or Reserved
				if pGpu.GetState() == cellReserving {
					setCellState(pGpu, cellUsed)
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
				} else { // cellReserved
					setCellState(pGpu, cellFree)
				}
			}
		}
	}
	delete(h.affinityGroups, g.name)
	klog.Infof("[%v]: Preempting affinity group %v deleted", internal.Key(pod), g.name)
}

// allocatePreemptingAffinityGroup lets a preemptor affinity group whose preemption has completed
// transition to allocated state.
func (h *HivedAlgorithm) allocatePreemptingAffinityGroup(g *AlgoAffinityGroup, pod *core.Pod) {
	for gpuNum := range g.physicalGpuPlacement {
		for podIndex := range g.physicalGpuPlacement[gpuNum] {
			for _, gpu := range g.physicalGpuPlacement[gpuNum][podIndex] {
				pGpu := gpu.(*PhysicalCell)
				pGpu.DeleteReservingOrReservedGroup(g)
				pGpu.AddUsingGroup(g)
				setCellState(pGpu, cellUsed)
			}
		}
	}
	g.state = groupAllocated
	g.preemptingPods = nil
	klog.Infof("[%v]: Preempting affinity group %v transitioned to allocated", internal.Key(pod), g.name)
}

// lazyPreemptAffinityGroup removes an affinity group from its VC, clears it virtual placement,
// and exposes this decision.
func (h *HivedAlgorithm) lazyPreemptAffinityGroup(
	victim *AlgoAffinityGroup,
	preemptor string) (originalVirtualPlacement groupVirtualPlacement) {
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
	originalVirtualPlacement = victim.virtualGpuPlacement
	victim.virtualGpuPlacement = nil
	victim.lazyPreemptionStatus = &api.LazyPreemptionStatus{
		Preemptor:      preemptor,
		PreemptionTime: meta.Now(),
	}
	klog.Infof("Affinity group %v is lazy preempted from VC by %v", victim.name, preemptor)
	return originalVirtualPlacement
}

// lazyPreemptCell lazy preempts all the affinity groups inside a virtual cell (and its children).
func (h *HivedAlgorithm) lazyPreemptCell(c *VirtualCell, preemptor string) {
	if c.GetLevel() == lowestLevel && c.GetState() == cellUsed {
		h.lazyPreemptAffinityGroup(c.GetPhysicalCell().GetUsingGroup(), preemptor)
	}
	for _, child := range c.GetChildren() {
		h.lazyPreemptCell(child.(*VirtualCell), preemptor)
	}
}

// revertLazyPreempt reverts the lazy preemption of an affinity group.
func (h *HivedAlgorithm) revertLazyPreempt(g *AlgoAffinityGroup, virtualPlacement groupVirtualPlacement) {
	for gpuNum := range g.physicalGpuPlacement {
		for podIndex := range g.physicalGpuPlacement[gpuNum] {
			for gpuIndex, gpu := range g.physicalGpuPlacement[gpuNum][podIndex] {
				if gpu == nil {
					continue
				}
				pGpu := gpu.(*PhysicalCell)
				vGpu := virtualPlacement[gpuNum][podIndex][gpuIndex].(*VirtualCell)
				h.releaseGpu(pGpu, g.vc)
				h.allocateGpu(pGpu, vGpu, CellPriority(g.priority), g.vc)
			}
		}
	}
	g.virtualGpuPlacement = virtualPlacement
	g.lazyPreemptionStatus = nil
	klog.Infof("Lazy preemption of affinity group %v is reverted", g.name)
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
	if pGpu := findPhysicalGpu(h.fullCellList, chain, node, physicalGpuIndex); pGpu == nil {
		klog.Warningf(
			"[%v]: Cannot find GPU %v on node %v: not found in the spec. Pod ignored",
			internal.Key(pod), physicalGpuIndex, node)
		return nil, nil, common.PtrBool(false)
	} else {
		var vGpu *VirtualCell
		if preassignedCellTypes == nil {
			klog.Warningf("[%v]: Cannot find virtual cell: preassigned cell not found in pod bind info", internal.Key(pod))
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
					message = fmt.Sprintf("Preassigned cell type %v not found in chain %v", preassignedType, pGpu.GetChain())
				} else if vcs := h.vcSchedulers[s.VirtualCluster]; vcs == nil {
					message = fmt.Sprintf("VC %v not found", s.VirtualCluster)
				} else {
					vccl := vcs.getNonPinnedPreassignedCells()[pGpu.GetChain()]
					str := string(pGpu.GetChain())
					if s.PinnedCellId != "" {
						vccl = vcs.getPinnedCells()[s.PinnedCellId]
						str = string(s.PinnedCellId)
					}
					if vccl == nil {
						message = fmt.Sprintf("VC %v has no cell for %v", s.VirtualCluster, str)
					} else {
						vGpu, message = mapPhysicalCellToVirtual(pGpu, vccl, preassignedLevel, priority)
					}
				}
				if vGpu == nil {
					klog.Warningf("[%v]: Cannot find virtual cell: %v", internal.Key(pod), message)
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

// allocateGpu creates the cell bindings, allocates the preassigned cell (if necessary),
// and sets the priority.
func (h *HivedAlgorithm) allocateGpu(
	pGpu *PhysicalCell,
	vGpu *VirtualCell,
	p CellPriority,
	vcn api.VirtualClusterName) (safetyOk bool, reason string) {

	safetyOk = true
	if vGpu != nil {
		setCellPriority(vGpu, p)
		updateUsedGpuNumAtPriority(vGpu, p, true)
		setCellPriority(pGpu, p)
		updateUsedGpuNumAtPriority(pGpu, p, true)
		pac := vGpu.GetPreassignedCell()
		preassignedNewlyBound := pac.GetPhysicalCell() == nil
		if pGpu.GetVirtualCell() == nil {
			// the binding could have been created before (when the cell is bad)
			bindCell(pGpu, vGpu)
		}
		if preassignedNewlyBound {
			safetyOk, reason = h.allocatePreassignedCell(pac.GetPhysicalCell(), vcn)
		}
	} else {
		setCellPriority(pGpu, opportunisticPriority)
		updateUsedGpuNumAtPriority(pGpu, opportunisticPriority, true)
		pGpu.GetAPIStatus().VC = vcn
		h.apiClusterStatus.VirtualClusters[vcn] = append(
			h.apiClusterStatus.VirtualClusters[vcn], generateOTVirtualCell(pGpu.GetAPIStatus()))
	}
	return safetyOk, reason
}

// releaseGpu destroys the cell bindings, release the preassigned cell (if necessary),
// and resets the priority.
func (h *HivedAlgorithm) releaseGpu(pGpu *PhysicalCell, vcn api.VirtualClusterName) {
	if vGpu := pGpu.GetVirtualCell(); vGpu != nil {
		updateUsedGpuNumAtPriority(vGpu, vGpu.GetPriority(), false)
		setCellPriority(vGpu, freePriority)
		preassignedPhysical := vGpu.GetPreassignedCell().GetPhysicalCell()
		if pGpu.IsHealthy() {
			// we won't unbind the cell if it is bad
			unbindCell(pGpu)
		}
		// To check if we should release the preassigned cell, we cannot simply check if the
		// virtual cell is already unbound. It's possible that the cell is bad, then the binding
		// won't be destroyed automatically (the cell is still bound only because it is bad).
		// If the below condition is true, then the preassigned cell is not in real use and we can hence release it.
		if !preassignedPhysical.IsPinned() && vGpu.GetPreassignedCell().GetPriority() < minGuaranteedPriority &&
			!h.vcDoomedBadCells[vcn][preassignedPhysical.GetChain()].contains(
				preassignedPhysical, preassignedPhysical.GetLevel()) {
			h.releasePreassignedCell(preassignedPhysical, vcn)
		}
	} else {
		pGpu.GetAPIStatus().VC = ""
		h.apiClusterStatus.VirtualClusters[vcn] = deleteOTVirtualCell(
			h.apiClusterStatus.VirtualClusters[vcn], pGpu.GetAddress())
	}
	updateUsedGpuNumAtPriority(pGpu, pGpu.GetPriority(), false)
	setCellPriority(pGpu, freePriority)
}

// allocatePreassignedCell allocates a physical cell to a preassigned virtual cell, removes the physical cell
// from the free cell list, and track the cell usages for safety and doomed bad cell check.
func (h *HivedAlgorithm) allocatePreassignedCell(
	c *PhysicalCell,
	vcn api.VirtualClusterName) (safetyOk bool, reason string) {

	safetyOk = true
	chain := c.GetChain()
	level := c.GetLevel()
	h.vcFreeCellNum[vcn][chain][level]--
	h.allVCFreeCellNum[chain][level]--
	h.totalLeftCellNum[chain][level]--
	splitLevelUpTo := h.removeCellFromFreeList(c)

	parent := c.GetParent()
	for l := level + 1; l <= splitLevelUpTo; l++ {
		h.totalLeftCellNum[chain][l]--
		if h.totalLeftCellNum[chain][l] < h.allVCFreeCellNum[chain][l] {
			safetyOk = false
			reason = fmt.Sprintf("Adding pod would lead to broken safety: "+
				"cell type %v, %v left, %v free cells in all VCs",
				h.cellTypes[chain][l], h.totalLeftCellNum[chain][l], h.allVCFreeCellNum[chain][l])
		}
		if !parent.(*PhysicalCell).IsHealthy() {
			h.badFreeCells[chain].remove(parent, l)
		} else {
			h.tryBindDoomedBadCell(chain, l)
		}
		parent = parent.GetParent()
	}
	if !c.IsHealthy() {
		h.allocateBadCell(c)
	} else {
		h.tryBindDoomedBadCell(chain, level)
	}
	numToReduce := int32(len(c.GetChildren()))
	for l := level - 1; l >= lowestLevel; l-- {
		h.totalLeftCellNum[chain][l] -= numToReduce
		if h.totalLeftCellNum[chain][l] < h.allVCFreeCellNum[chain][l] {
			safetyOk = false
			reason = fmt.Sprintf("Adding pod would lead to broken safety: "+
				"cell type %v, %v left, %v free cells in all VCs",
				h.cellTypes[chain][l], h.totalLeftCellNum[chain][l], h.allVCFreeCellNum[chain][l])
		}
		h.tryBindDoomedBadCell(chain, l)
		numToReduce *= int32(len(h.fullCellList[chain][l][0].GetChildren()))
	}
	return safetyOk, reason
}

// allocateBadCell allocates a bad free physical cell, and then binds each of its bad children
// to a virtual cell recursively (so that the VC scheduler can see the bad cell).
func (h *HivedAlgorithm) allocateBadCell(c *PhysicalCell) {
	if h.badFreeCells[c.GetChain()].contains(c, c.GetLevel()) {
		h.badFreeCells[c.GetChain()].remove(c, c.GetLevel())
	}
	if c.GetVirtualCell() == nil {
		// parent of c must have been bound to a virtual cell
		vc := getUnboundVirtualCell(c.GetParent().(*PhysicalCell).GetVirtualCell().GetChildren())
		c.SetVirtualCell(vc)
		vc.SetPhysicalCell(c)
		klog.Infof("Virtual cell %v is bound to physical cell %v", vc.GetAddress(), c.GetAddress())
	}
	for _, child := range c.GetChildren() {
		if pc := child.(*PhysicalCell); !pc.IsHealthy() {
			h.allocateBadCell(pc)
		}
	}
}

// releasePreassignedCell releases a physical cell allocated to a preassigned virtual cell, adds the physical cell
// back to the free cell list, and track the cell usages for safety and doomed bad cell check.
func (h *HivedAlgorithm) releasePreassignedCell(c *PhysicalCell, vcn api.VirtualClusterName) {
	chain := c.GetChain()
	level := c.GetLevel()
	h.vcFreeCellNum[vcn][chain][level]++
	h.allVCFreeCellNum[chain][level]++
	h.totalLeftCellNum[chain][level]++
	mergeLevelUpTo := h.addCellToFreeList(c)

	parent := c.GetParent()
	for l := level + 1; l <= mergeLevelUpTo; l++ {
		h.totalLeftCellNum[chain][l]++
		if !parent.(*PhysicalCell).IsHealthy() {
			h.badFreeCells[chain][l] = append(h.badFreeCells[chain][l], parent)
		} else {
			h.tryUnbindDoomedBadCell(chain, l)
		}
		parent = parent.GetParent()
	}
	if !c.IsHealthy() {
		h.releaseBadCell(c)
	} else {
		h.tryUnbindDoomedBadCell(chain, level)
	}
	numToAdd := int32(len(c.GetChildren()))
	for l := level - 1; l >= lowestLevel; l-- {
		h.totalLeftCellNum[chain][l] += numToAdd
		h.tryUnbindDoomedBadCell(chain, l)
		numToAdd *= int32(len(h.fullCellList[chain][l][0].GetChildren()))
	}
}

// releaseBadCell releases a bad free physical cell, and then unbinds its bad children recursively.
func (h *HivedAlgorithm) releaseBadCell(c *PhysicalCell) {
	h.badFreeCells[c.GetChain()][c.GetLevel()] = append(h.badFreeCells[c.GetChain()][c.GetLevel()], c)
	if vc := c.GetVirtualCell(); vc != nil {
		c.SetVirtualCell(nil)
		vc.SetPhysicalCell(nil)
		klog.Infof("Virtual cell %v is unbound from physical cell %v", vc.GetAddress(), c.GetAddress())
	}
	for _, child := range c.GetChildren() {
		if pc := child.(*PhysicalCell); !pc.IsHealthy() {
			h.releaseBadCell(pc)
		}
	}
}

// removeCellFromFreeList removes a cell from the free cell list and splits its parent recursively if needed.
func (h *HivedAlgorithm) removeCellFromFreeList(c *PhysicalCell) (splitLevelUpTo CellLevel) {
	chain := c.GetChain()
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
		klog.Infof("Cell %v (type %v) removed from free cell list", c.GetAddress(), h.cellTypes[chain][l])
		if terminate {
			return l
		} else {
			c = parent.(*PhysicalCell)
		}
	}
}

// addCellToFreeList adds a cell to the free cell list and merges its buddies recursively if needed.
func (h *HivedAlgorithm) addCellToFreeList(c *PhysicalCell) (mergeLevelUpTo CellLevel) {
	chain := c.GetChain()
	for terminate := false; ; {
		l := c.GetLevel()
		parent := c.GetParent()
		if parent != nil {
			allBuddyFree := true
			for _, buddy := range parent.GetChildren() {
				if !CellEqual(buddy, c) && !h.freeCellList[chain].contains(buddy, l) {
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
						klog.Infof("Cell %v (type %v) removed from free cell list",
							buddy.GetAddress(), h.cellTypes[chain][l])
					}
				}
				parent.(*PhysicalCell).SetSplit(false)
			}
		} else {
			terminate = true
		}
		if terminate {
			h.freeCellList[chain][l] = append(h.freeCellList[chain][l], c)
			return l
		} else {
			c = parent.(*PhysicalCell)
		}
	}
}
