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

package v2

import (
	"fmt"

	"github.com/microsoft/hivedscheduler/pkg/api"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// GeneralSpec represents a generic key-value yaml object interface.
type GeneralSpec map[string]interface{}

// PodSchedulingSpec represents HiveD scheudling spec in k8s pod request.
type PodSchedulingSpec struct {
	Version              string                 `yaml:"version"`
	VirtualCluster       api.VirtualClusterName `yaml:"virtualCluster"`
	Priority             int32                  `yaml:"priority"`
	PinnedCellId         api.PinnedCellId       `yaml:"pinnedCellId"`
	CellType             string                 `yaml:"cellType"`
	CellNumber           int32                  `yaml:"cellNumber"`
	GangReleaseEnable    bool                   `yaml:"gangReleaseEnable"`
	LazyPreemptionEnable bool                   `yaml:"lazyPreemptionEnable"`
	PodRootGroup         *PodGroupSpec          `yaml:"podRootGroup"`
}

// PodGroupSpec represents a tree stucture of pod group spec.
type PodGroupSpec struct {
	Name          string               `yaml:"name"`
	WithinOneCell api.CellType         `yaml:"withinOneCell"`
	Pods          []PodGroupMemberSpec `yaml:"pods"`
	ChildGroups   []*PodGroupSpec      `yaml:"childGroups"`
}

// PodGroupMemberSpec represents content of each node in tree stucture pod group.
// It contains pod number and cell spec for the pod.
type PodGroupMemberSpec struct {
	PodMinNumber       int32                  `yaml:"podMinNumber"`
	PodMaxNumber       int32                  `yaml:"podMaxNumber"`
	CellsPerPod        PodGroupMemberCellSpec `yaml:"cellsPerPod"`
	ContainsCurrentPod bool                   `yaml:"containsCurrentPod"`
}

// PodGroupMemberCellSpec represents cell spec for each pod in pod group.
type PodGroupMemberCellSpec struct {
	CellType   api.CellType `yaml:"cellType"`
	CellNumber int32        `yaml:"cellNumber"`
}

type PodGroupStatus struct {
	VC                   api.VirtualClusterName                `json:"vc"`
	Priority             int32                                 `json:"priority"`
	State                string                                `json:"state"`
	PhysicalPlacement    map[string][]int32                    `json:"physicalPlacement,omitempty"` // node -> leaf cell indices
	VirtualPlacement     map[api.CellAddress][]api.CellAddress `json:"virtualPlacement,omitempty"`  // preassigned cell -> leaf cells
	AllocatedPods        []types.UID                           `json:"allocatedPods,omitempty"`
	PreemptingPods       []types.UID                           `json:"preemptingPods,omitempty"`
	LazyPreemptionStatus *api.LazyPreemptionStatus             `json:"lazyPreemptionStatus,omitempty"`
}

type podGroupSpecIterator struct {
	pods   []*PodGroupMemberSpec
	index  int
	length int
}

// Next returns the next item in iteration.
func (i *podGroupSpecIterator) Next() *PodGroupMemberSpec {
	i.index++
	return i.pods[i.index-1]
}

// HasNext return true if iteration not finishes.
func (i *podGroupSpecIterator) HasNext() bool {
	return i.index < i.length
}

// Iterator returns a stateful iterator for PodGroupSpec
func (podRootGroup *PodGroupSpec) Iterator() *podGroupSpecIterator {
	pods := []*PodGroupMemberSpec{}
	queue := []*PodGroupSpec{podRootGroup}
	for len(queue) > 0 {
		newQueue := []*PodGroupSpec{}
		for _, podGroup := range queue {
			for podIndex := range podGroup.Pods {
				pods = append(pods, &podGroup.Pods[podIndex])
			}
			newQueue = append(newQueue, podGroup.ChildGroups...)
		}
		queue = newQueue
	}
	return &podGroupSpecIterator{pods, 0, len(pods)}
}

// SetCellType sets cell type for all pods in pod group.
func (podRootGroup *PodGroupSpec) SetCellType(cellType string) {
	for iter := podRootGroup.Iterator(); iter.HasNext(); {
		iter.Next().CellsPerPod.CellType = api.CellType(cellType)
	}
}

// GetCurrentPod returns level traverse index and current pod in pod group.
func (obj *PodSchedulingSpec) GetCurrentPod() (int32, PodGroupMemberSpec) {
	index := int32(0)
	queue := []*PodGroupSpec{obj.PodRootGroup}
	for len(queue) > 0 {
		newQueue := []*PodGroupSpec{}
		for _, podGroup := range queue {
			for _, pod := range podGroup.Pods {
				if pod.ContainsCurrentPod == true {
					return index, pod
				}
			}
			index++
			newQueue = append(newQueue, podGroup.ChildGroups...)
		}
		queue = newQueue
	}
	return int32(-1), PodGroupMemberSpec{}
}

// ConvertFromV1 converts a v1 pod scheduling request to v2 spec.
func (obj *PodSchedulingSpec) ConvertFromV1(objV1 *api.PodSchedulingSpec) {
	obj.Version = "v2"
	obj.VirtualCluster = objV1.VirtualCluster
	obj.Priority = objV1.Priority
	obj.PinnedCellId = objV1.PinnedCellId
	obj.CellType = objV1.LeafCellType
	obj.CellNumber = objV1.LeafCellNumber
	obj.GangReleaseEnable = objV1.GangReleaseEnable
	obj.LazyPreemptionEnable = objV1.LazyPreemptionEnable
	if objV1.AffinityGroup != nil {
		var pods []PodGroupMemberSpec
		for _, memberV1 := range objV1.AffinityGroup.Members {
			member := PodGroupMemberSpec{
				PodMinNumber: memberV1.PodNumber,
				PodMaxNumber: memberV1.PodNumber,
				CellsPerPod: PodGroupMemberCellSpec{
					CellType:   api.CellType(obj.CellType),
					CellNumber: memberV1.LeafCellNumber,
				},
				ContainsCurrentPod: bool(obj.CellNumber == memberV1.LeafCellNumber),
			}
			pods = append(pods, member)
		}
		obj.PodRootGroup = &PodGroupSpec{
			Name: objV1.AffinityGroup.Name,
			Pods: pods,
		}
	}
}

// SetDefaults sets default values for PodSchedulingSpec.
func (obj *PodSchedulingSpec) SetDefaults(pod *core.Pod) {
	if obj.PodRootGroup == nil {
		obj.PodRootGroup = &PodGroupSpec{
			Name: fmt.Sprintf("%v/%v", pod.Namespace, pod.Name),
			Pods: []PodGroupMemberSpec{{
				PodMinNumber: 1,
				PodMaxNumber: 1,
				CellsPerPod: PodGroupMemberCellSpec{
					CellType:   api.CellType(obj.CellType),
					CellNumber: obj.CellNumber,
				},
				ContainsCurrentPod: true,
			}},
		}
	}
}

// Validate checks whether PodSchedulingSpec is ok.
func (obj *PodSchedulingSpec) Validate() (msg string, ok bool) {
	if obj.VirtualCluster == "" {
		return "VirtualCluster is empty", false
	}
	if obj.Priority < api.OpportunisticPriority {
		return fmt.Sprintf("Priority is less than %v", api.OpportunisticPriority), false
	}
	if obj.Priority > api.MaxGuaranteedPriority {
		return fmt.Sprintf("Priority is greater than %v", api.MaxGuaranteedPriority), false
	}
	if obj.CellNumber <= 0 {
		return "CellNumber is non-positive", false
	}
	if obj.PodRootGroup.Name == "" {
		return "PodRootGroup.Name is empty", false
	}

	isPodInGroup := false
	queue := []*PodGroupSpec{obj.PodRootGroup}
	for len(queue) > 0 {
		newQueue := []*PodGroupSpec{}
		for _, podGroup := range queue {
			for _, pod := range podGroup.Pods {
				if pod.PodMinNumber <= 0 {
					return "PodGroup.Pods have non-positive PodMinNumber", false
				}
				if pod.PodMaxNumber <= 0 {
					return "PodGroup.Pods have non-positive PodMaxNumber", false
				}
				if pod.CellsPerPod.CellNumber <= 0 {
					return "PodGroup.Pods have non-positive CellsPerPod.CellNumber", false
				}
				if pod.ContainsCurrentPod == true {
					if isPodInGroup == false {
						isPodInGroup = true
					} else {
						return "PodGroup.Pods have multiple ContainsCurrentPod", false
					}
				}
			}
			newQueue = append(newQueue, podGroup.ChildGroups...)
		}
		queue = newQueue
	}
	if !isPodInGroup {
		return "PodGroup.Pods does not contain current Pod", false
	}
	return "", true
}

type PodBindingInfo struct {
	Node                    string               `yaml:"node"`              // node to bind
	LeafCellIsolation       []int32              `yaml:"leafCellIsolation"` // leaf cells to bind
	CellChain               string               `yaml:"cellChain"`         // cell chain selected
	PodRootGroupBindingInfo *PodGroupBindingInfo `yaml:"PodRootGroupBindingInfo"`
}

type PodGroupBindingInfo struct {
	PodPlacements         []PodPlacementsInfo    `yaml:"podPlacements"`
	ChildGroupBindingInfo []*PodGroupBindingInfo `yaml:"childGroupBindingInfo"`
}

type PodPlacementsInfo struct {
	PhysicalNode            string  `yaml:"physicalNode"`
	PhysicalLeafCellIndices []int32 `yaml:"physicalLeafCellIndices"`
	// preassigned cell types used by the pods. used to locate the virtual cells
	// when adding an allocated pod
	PreassignedCellTypes []api.CellType `yaml:"preassignedCellTypes"`
}

type podGroupBindingInfoIterator struct {
	podPlacementsInfoList []*PodPlacementsInfo
	index                 int
	length                int
}

// Next returns the next item in iteration.
func (i *podGroupBindingInfoIterator) Next() *PodPlacementsInfo {
	i.index++
	return i.podPlacementsInfoList[i.index-1]
}

// HasNext return true if iteration not finishes.
func (i *podGroupBindingInfoIterator) HasNext() bool {
	return i.index < i.length
}

// Iterator returns a stateful iterator for PodGroupBindingInfo
func (podRootGroupBindingInfo *PodGroupBindingInfo) Iterator(args ...int32) *podGroupBindingInfoIterator {
	index := int32(0)
	podPlacementsInfoList := []*PodPlacementsInfo{}
	queue := []*PodGroupBindingInfo{podRootGroupBindingInfo}
	for len(queue) > 0 {
		newQueue := []*PodGroupBindingInfo{}
		for _, podGroupBindingInfo := range queue {
			if len(args) == 1 && args[0] == index {
				podPlacementsInfoList = []*PodPlacementsInfo{}
			}
			for podIndex := range podGroupBindingInfo.PodPlacements {
				podPlacementsInfoList = append(podPlacementsInfoList, &podGroupBindingInfo.PodPlacements[podIndex])
			}
			if len(args) == 1 && args[0] == index {
				return &podGroupBindingInfoIterator{podPlacementsInfoList, 0, len(podPlacementsInfoList)}
			}
			index++
			newQueue = append(newQueue, podGroupBindingInfo.ChildGroupBindingInfo...)
		}
		queue = newQueue
	}
	return &podGroupBindingInfoIterator{podPlacementsInfoList, 0, len(podPlacementsInfoList)}
}

type PodGroupList struct {
	Items []PodGroupItem `json:"items"`
}

type PodGroupItem struct {
	api.ObjectMeta `json:"metadata"`
	Status         PodGroupStatus `json:"status"`
}
