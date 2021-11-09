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
	"os"

	"github.com/microsoft/hivedscheduler/pkg/common"
)

///////////////////////////////////////////////////////////////////////////////////////
// General Constants
///////////////////////////////////////////////////////////////////////////////////////
const (
	ComponentName = "hivedscheduler"
	GroupName     = "hivedscheduler.microsoft.com"

	UnlimitedValue = -1

	// To leverage this scheduler, at least one container in the Pod should contain
	// below resource limit with any positive int16 value.
	ResourceNamePodSchedulingEnable = GroupName + "/pod-scheduling-enable"

	// To leverage this scheduler, the Pod should contain below annotation in
	// PodSchedulingSpec YAML format.
	AnnotationKeyPodSchedulingSpec = GroupName + "/pod-scheduling-spec"

	// To leverage this scheduler, the Pod could reference below annotation to
	// use the allocated leaf cells for the whole Pod.
	AnnotationKeyPodLeafCellIsolation      = GroupName + "/pod-leaf-cell-isolation"
	DeprecatedAnnotationKeyPodGpuIsolation = GroupName + "/pod-gpu-isolation"

	// Populated by this scheduler, used to track and recover allocated placement.
	// It is in PodBindInfo YAML format.
	AnnotationKeyPodBindInfo = GroupName + "/pod-bind-info"

	// Priority Range of Guaranteed Pod.
	MaxGuaranteedPriority = int32(1000)
	MinGuaranteedPriority = int32(0)

	// Priority of Opportunistic Pod.
	OpportunisticPriority = int32(-1)
)

var EnvValueConfigFilePath = common.GetEnv("CONFIG", "./hivedscheduler.yaml")
var EnvValueKubeApiServerAddress = common.GetEnv("KUBE_APISERVER_ADDRESS", "")
var EnvValueKubeConfigFilePath = common.GetEnv("KUBECONFIG", os.Getenv("HOME")+"/.kube/config")

///////////////////////////////////////////////////////////////////////////////////////
// WebServer Constants
///////////////////////////////////////////////////////////////////////////////////////
const (
	RootPath    = "/"
	VersionPath = RootPath + "v1"

	// Scheduler Extender API: API with K8S Default Scheduler
	ExtenderPath = VersionPath + "/extender"
	FilterPath   = ExtenderPath + "/filter"
	BindPath     = ExtenderPath + "/bind"
	PreemptPath  = ExtenderPath + "/preempt"

	// Scheduler Inspect API: API to inspect current scheduling status
	// Notes:
	// 1. Both Binding and Bound PodGroups/Pods are considered as Allocated.
	InspectPath = VersionPath + "/inspect"
	// Inspect current allocated PodGroup(s)
	PodGroupsPath = InspectPath + "/podgroups/"
	// Inspect current cluster status
	ClusterStatusPath = InspectPath + "/clusterstatus"
	// Inspect current physical cluster status
	PhysicalClusterPath = ClusterStatusPath + "/physicalcluster"
	// Inspect current virtual cluster(s)' status
	VirtualClustersPath = ClusterStatusPath + "/virtualclusters/"
)
