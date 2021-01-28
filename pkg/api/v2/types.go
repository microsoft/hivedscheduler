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
	"github.com/microsoft/hivedscheduler/pkg/api"
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
	WithinOneCell string               `yaml:"withinOneCell"`
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
	CellType   string `yaml:"cellType"`
	CellNumber int32  `yaml:"cellNumber"`
}
