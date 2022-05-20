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

package api

import (
	"fmt"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////////////
// General Types
///////////////////////////////////////////////////////////////////////////////////////
type (
	CellType     string
	CellAddress  string
	PinnedCellId string
)

// Physical cluster definition
type PhysicalClusterSpec struct {
	CellTypes     map[CellType]CellTypeSpec `yaml:"cellTypes"`
	PhysicalCells []PhysicalCellSpec        `yaml:"physicalCells"`
}

type CellTypeSpec struct {
	ChildCellType   CellType `yaml:"childCellType"`
	ChildCellNumber int32    `yaml:"childCellNumber"`
	IsNodeLevel     bool     `yaml:"isNodeLevel"`
}

// Specify physical Cell instances.
type PhysicalCellSpec struct {
	CellType     CellType           `yaml:"cellType"`
	CellAddress  CellAddress        `yaml:"cellAddress"`
	PinnedCellId PinnedCellId       `yaml:"pinnedCellId"`
	CellChildren []PhysicalCellSpec `yaml:"cellChildren,omitempty"`
}

// Virtual cluster definition
type VirtualClusterName string

type VirtualClusterSpec struct {
	VirtualCells []VirtualCellSpec `yaml:"virtualCells"`
	PinnedCells  []PinnedCellSpec  `yaml:"pinnedCells,omitempty"`
}

type VirtualCellSpec struct {
	CellNumber int32    `yaml:"cellNumber"`
	CellType   CellType `yaml:"cellType"`
}

type PinnedCellSpec struct {
	PinnedCellId PinnedCellId `yaml:"pinnedCellId"`
}

type Quota struct {
	// Tag for quota
	Tag string `yaml:"tag,omitempty"`
	// Count for quota
	Count int32 `yaml:"count,omitempty"`
}

type PodSchedulingSpec struct {
	VirtualCluster          VirtualClusterName `yaml:"virtualCluster"`
	Priority                int32              `yaml:"priority"`
	PinnedCellId            PinnedCellId       `yaml:"pinnedCellId"`
	LeafCellType            string             `yaml:"leafCellType"`
	LeafCellNumber          int32              `yaml:"leafCellNumber"`
	GangReleaseEnable       bool               `yaml:"gangReleaseEnable"`
	LazyPreemptionEnable    bool               `yaml:"lazyPreemptionEnable"`
	IgnoreK8sSuggestedNodes bool               `yaml:"ignoreK8sSuggestedNodes" default:"true"`
	AffinityGroup           *AffinityGroupSpec `yaml:"affinityGroup"`
	Quota                   Quota              `yaml:"quota,omitempty"`
}

type AffinityGroupSpec struct {
	Name    string                    `yaml:"name"`
	Members []AffinityGroupMemberSpec `yaml:"members"`
}

type AffinityGroupMemberSpec struct {
	PodNumber      int32 `yaml:"podNumber"`
	LeafCellNumber int32 `yaml:"leafCellNumber"`
}

// Used to recover scheduler allocated resource
type PodBindInfo struct {
	Node                  string                        `yaml:"node"`              // node to bind
	LeafCellIsolation     []int32                       `yaml:"leafCellIsolation"` // leaf cells to bind
	CellChain             string                        `yaml:"cellChain"`         // cell chain selected
	AffinityGroupBindInfo []AffinityGroupMemberBindInfo `yaml:"affinityGroupBindInfo"`
}

type AffinityGroupMemberBindInfo struct {
	PodPlacements []PodPlacementInfo `yaml:"podPlacements"`
}

type PodPlacementInfo struct {
	PhysicalNode            string  `yaml:"physicalNode"`
	PhysicalLeafCellIndices []int32 `yaml:"physicalLeafCellIndices"`
	// preassigned cell types used by the pods. used to locate the virtual cells
	// when adding an allocated pod
	PreassignedCellTypes []CellType `yaml:"preassignedCellTypes"`
}

type WebServerPaths struct {
	Paths []string `json:"paths"`
}

type WebServerError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func NewWebServerError(code int, message string) *WebServerError {
	return &WebServerError{
		Code:    code,
		Message: message,
	}
}

func (err *WebServerError) Error() string {
	return fmt.Sprintf("Code: %v, Message: %v", err.Code, err.Message)
}

// WebServer Exposed Objects: Align with K8S Objects
type ObjectMeta struct {
	Name string `json:"name"`
}

type AffinityGroupList struct {
	Items []AffinityGroup `json:"items"`
}

type AffinityGroup struct {
	ObjectMeta `json:"metadata"`
	Status     AffinityGroupStatus `json:"status"`
}

type AffinityGroupState string

type AffinityGroupStatus struct {
	VC                   VirtualClusterName            `json:"vc"`
	Priority             int32                         `json:"priority"`
	State                AffinityGroupState            `json:"state"`
	PhysicalPlacement    map[string][]int32            `json:"physicalPlacement,omitempty"` // node -> leaf cell indices
	VirtualPlacement     map[CellAddress][]CellAddress `json:"virtualPlacement,omitempty"`  // preassigned cell -> leaf cells
	AllocatedPods        []types.UID                   `json:"allocatedPods,omitempty"`
	PreemptingPods       []types.UID                   `json:"preemptingPods,omitempty"`
	LazyPreemptionStatus *LazyPreemptionStatus         `json:"lazyPreemptionStatus,omitempty"`
}

type LazyPreemptionStatus struct {
	// The AffinityGroup who has lazy preempted it.
	Preemptor string `json:"preemptor"`
	// It was lazy preempted at PreemptionTime.
	PreemptionTime meta.Time `json:"preemptionTime"`
}

type (
	CellState       string
	CellHealthiness string
)

const (
	CellHealthy CellHealthiness = "Healthy"
	CellBad     CellHealthiness = "Bad"
)

type CellStatus struct {
	LeafCellType string   `json:"leafCellType,omitempty"`
	CellType     CellType `json:"cellType"`
	IsNodeLevel  bool     `json:"isNodeLevel,omitempty"`
	// Address of a physical cell consists of its address (or index) in each level
	// (e.g., node0/0/0/0 may represent node0, CPU socket 0, PCIe switch 0, GPU 0.
	// Address of a virtual cell consists of its VC name, index of the preassigned cell,
	// and the relative index in each level inside the preassigned cell
	// (e.g., VC1/0/0 may represent VC1, preassigned cell 0, index 0 among its children)
	CellAddress CellAddress `json:"cellAddress"`
	// CellState and CellHealthiness are two orthogonal fields.
	// CellState represents whether the cell is used (or reserved) by an affinity group.
	// CellHealthiness represents whether the physical hardware is working normally.
	CellState       CellState       `json:"cellState"`
	CellHealthiness CellHealthiness `json:"cellHealthiness"`
	CellPriority    int32           `json:"cellPriority"`
}

type PhysicalCellStatus struct {
	CellStatus
	CellChildren []*PhysicalCellStatus `json:"cellChildren,omitempty"`
	VC           VirtualClusterName    `json:"vc,omitempty"`
	VirtualCell  *VirtualCellStatus    `json:"virtualCell,omitempty"`
}

type VirtualCellStatus struct {
	CellStatus
	CellChildren []*VirtualCellStatus `json:"cellChildren,omitempty"`
	PhysicalCell *PhysicalCellStatus  `json:"physicalCell,omitempty"`
}

type PhysicalClusterStatus []*PhysicalCellStatus

type VirtualClusterStatus []*VirtualCellStatus

type ClusterStatus struct {
	// Status of cells in the physical cluster
	PhysicalCluster PhysicalClusterStatus `json:"physicalCluster"`
	// Status of cells in each VC
	VirtualClusters map[VirtualClusterName]VirtualClusterStatus `json:"virtualClusters"`
}

func (pcs *PhysicalCellStatus) deepCopy() *PhysicalCellStatus {
	copied := &PhysicalCellStatus{
		CellStatus: pcs.CellStatus,
		VC:         pcs.VC,
	}
	if pcs.CellChildren != nil {
		copied.CellChildren = make([]*PhysicalCellStatus, len(pcs.CellChildren))
		for i, child := range pcs.CellChildren {
			copied.CellChildren[i] = child.deepCopy()
		}
	}
	if pcs.VirtualCell != nil {
		copied.VirtualCell = pcs.VirtualCell.deepCopy()
	}
	return copied
}

func (pcs PhysicalClusterStatus) DeepCopy() PhysicalClusterStatus {
	copied := make(PhysicalClusterStatus, len(pcs))
	for i, c := range pcs {
		copied[i] = c.deepCopy()
	}
	return copied
}

func (vcs *VirtualCellStatus) deepCopy() *VirtualCellStatus {
	copied := &VirtualCellStatus{
		CellStatus: vcs.CellStatus,
	}
	if vcs.CellChildren != nil {
		copied.CellChildren = make([]*VirtualCellStatus, len(vcs.CellChildren))
		for i, child := range vcs.CellChildren {
			copied.CellChildren[i] = child.deepCopy()
		}
	}
	if vcs.PhysicalCell != nil {
		copied.PhysicalCell = vcs.PhysicalCell.deepCopy()
	}
	return copied
}

func (vcs VirtualClusterStatus) DeepCopy() VirtualClusterStatus {
	copied := make(VirtualClusterStatus, len(vcs))
	for i, c := range vcs {
		copied[i] = c.deepCopy()
	}
	return copied
}
