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
	"io/ioutil"
	"os"

	"github.com/microsoft/hivedscheduler/pkg/common"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type Config struct {
	// KubeApiServerAddress is default to ${KUBE_APISERVER_ADDRESS}.
	// KubeConfigFilePath is default to ${KUBECONFIG} then falls back to ${HOME}/.kube/config.
	//
	// If both KubeApiServerAddress and KubeConfigFilePath after defaulting are still empty, falls back to the
	// [k8s inClusterConfig](https://kubernetes.io/docs/tasks/access-application-cluster/access-cluster/#accessing-the-api-from-a-pod).
	//
	// If both KubeApiServerAddress and KubeConfigFilePath after defaulting are not empty,
	// KubeApiServerAddress overrides the server address specified in the file referred by KubeConfigFilePath.
	//
	// If only KubeApiServerAddress after defaulting is not empty, it should be an insecure ApiServer address (can be got from
	// [Insecure ApiServer](https://kubernetes.io/docs/reference/access-authn-authz/controlling-access/#api-server-ports-and-ips) or
	// [kubectl proxy](https://kubernetes.io/docs/tasks/access-application-cluster/access-cluster/#using-kubectl-proxy))
	// which does not enforce authentication.
	//
	// If only KubeConfigFilePath after defaulting is not empty, it should be an valid
	// [KubeConfig File](https://kubernetes.io/docs/tasks/access-application-cluster/configure-access-multiple-clusters/#explore-the-home-kube-directory)
	// which inlines or refers the valid
	// [ApiServer Credential Files](https://kubernetes.io/docs/reference/access-authn-authz/controlling-access/#transport-security).
	//
	// Address should be in format http[s]://host:port
	KubeApiServerAddress *string `yaml:"kubeApiServerAddress"`
	KubeConfigFilePath   *string `yaml:"kubeConfigFilePath"`

	// WebServer
	// Default to :9096
	WebServerAddress *string `yaml:"webServerAddress"`

	// Specify a threshold for PodBindAttempts, that after it is exceeded, an extra
	// Pod binding will be executed forcefully.
	ForcePodBindThreshold *int32 `yaml:"forcePodBindThreshold"`

	// If a Pod is decided to be PodWaiting, it will block the whole scheduling by
	// WaitingPodSchedulingBlockMilliSec.
	// Large value can be used to achieve stronger FIFO scheduling by sacrificing
	// the scheduling throughput.
	// This is a workaround until PodMaxBackoffSeconds can be configured for
	// K8S Default Scheduler.
	WaitingPodSchedulingBlockMilliSec *int64 `yaml:"waitingPodSchedulingBlockMilliSec"`

	// Specify the whole physical cluster
	// TODO: Automatically construct it based on node info from Device Plugins
	PhysicalCluster *PhysicalClusterSpec `yaml:"physicalCluster"`

	// Specify all the virtual clusters belongs to the physical cluster
	VirtualClusters *map[VirtualClusterName]VirtualClusterSpec `yaml:"virtualClusters"`
}

func NewConfig(rawConfig *Config) *Config {
	c := rawConfig

	// Defaulting
	if c.KubeApiServerAddress == nil {
		c.KubeApiServerAddress = common.PtrString(EnvValueKubeApiServerAddress)
	}
	if c.KubeConfigFilePath == nil {
		c.KubeConfigFilePath = defaultKubeConfigFilePath()
	}
	if c.WebServerAddress == nil {
		c.WebServerAddress = common.PtrString(":9096")
	}
	if c.ForcePodBindThreshold == nil {
		c.ForcePodBindThreshold = common.PtrInt32(3)
	}
	if c.WaitingPodSchedulingBlockMilliSec == nil {
		c.WaitingPodSchedulingBlockMilliSec = common.PtrInt64(0)
	}
	if c.PhysicalCluster == nil {
		c.PhysicalCluster = defaultPhysicalCluster()
	}
	if c.VirtualClusters == nil {
		c.VirtualClusters = defaultVirtualClusters()
	}
	// Append default value for empty items in physical cell
	defaultingPhysicalCells(c.PhysicalCluster)
	// Validation
	// TODO: Validate VirtualClusters against PhysicalCluster

	return c
}

func defaultingPhysicalCells(pc *PhysicalClusterSpec) {
	cts := pc.CellTypes
	pcs := pc.PhysicalCells
	for idx, pc := range pcs {
		_, ok := cts[pc.CellType]
		if !ok {
			// unknown cell type
			panic(fmt.Sprintf("physicalCells contains unknown cellType: %v", pc.CellType))
		}
		inferPhysicalCellSpec(&pcs[idx], cts, pc.CellType, int32(idx), "")
	}
	return
}

func inferPhysicalCellSpec(
	spec *PhysicalCellSpec,
	cts map[CellType]CellTypeSpec,
	cellType CellType,
	defaultAddress int32,
	addressPrefix CellAddress) {

	if spec.CellType == "" {
		spec.CellType = cellType
	}
	if spec.CellAddress == "" {
		spec.CellAddress = addressPrefix + CellAddress(common.Int32ToString(defaultAddress))
	} else {
		spec.CellAddress = addressPrefix + spec.CellAddress
	}

	ct, ok := cts[cellType]
	if !ok {
		// not found in cts, it's a leaf cell type
		return
	}
	if ct.IsNodeLevel {
		// reset default address to 0 when found a node level cell, leaf cell will use it as indices
		defaultAddress = 0
	}
	if ct.ChildCellNumber > 0 && len(spec.CellChildren) == 0 {
		spec.CellChildren = make([]PhysicalCellSpec, ct.ChildCellNumber)
	}
	for i := range spec.CellChildren {
		inferPhysicalCellSpec(&spec.CellChildren[i], cts, ct.ChildCellType,
			defaultAddress*ct.ChildCellNumber+int32(i), spec.CellAddress+"/")
	}
	return
}

func defaultKubeConfigFilePath() *string {
	configPath := EnvValueKubeConfigFilePath
	_, err := os.Stat(configPath)
	if err == nil {
		return &configPath
	}

	configPath = DefaultKubeConfigFilePath
	_, err = os.Stat(configPath)
	if err == nil {
		return &configPath
	}

	configPath = ""
	return &configPath
}

func defaultPhysicalCluster() *PhysicalClusterSpec {
	return &PhysicalClusterSpec{}
}

func defaultVirtualClusters() *map[VirtualClusterName]VirtualClusterSpec {
	return &map[VirtualClusterName]VirtualClusterSpec{}
}

func InitRawConfig(configPath *string) *Config {
	c := Config{}
	var configFilePath string

	if configPath == nil {
		configFilePath = DefaultConfigFilePath
	} else {
		configFilePath = *configPath
	}
	yamlBytes, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		panic(fmt.Errorf(
			"Failed to read config file: %v, %v", configFilePath, err))
	}

	common.FromYaml(string(yamlBytes), &c)
	return &c
}

func BuildKubeConfig(sConfig *Config) *rest.Config {
	kConfig, err := clientcmd.BuildConfigFromFlags(
		*sConfig.KubeApiServerAddress, *sConfig.KubeConfigFilePath)
	if err != nil {
		panic(fmt.Errorf("Failed to build KubeConfig, please ensure "+
			"config kubeApiServerAddress or config kubeConfigFilePath or "+
			"${KUBE_APISERVER_ADDRESS} or ${KUBECONFIG} or ${HOME}/.kube/config or "+
			"${KUBERNETES_SERVICE_HOST}:${KUBERNETES_SERVICE_PORT} is valid: "+
			"Error: %v", err))
	}
	return kConfig
}
