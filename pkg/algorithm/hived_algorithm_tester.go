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
	"io/ioutil"
	"reflect"
	"sort"
	"testing"

	"github.com/microsoft/hivedscheduler/pkg/api"
	apiv2 "github.com/microsoft/hivedscheduler/pkg/api/v2"
	"github.com/microsoft/hivedscheduler/pkg/common"
	"github.com/microsoft/hivedscheduler/pkg/internal"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type bindResult struct {
	Node              string  `yaml:"node"`
	LeafCellIsolation []int32 `yaml:"leafCellIsolation"`
}

type preemptResult struct {
	VictimPods []string `yaml:"victimPods"`
}

type waitResult struct {
	Reason string `yaml:"reason"`
}

type step struct {
	Method     string                      `yaml:"method"`
	Paramaters map[interface{}]interface{} `yaml:"parameters"`
}

type stepList []step

type HivedAlgorithmTester interface {
	SetAllNodesToHealthy()
	SetAllNodesToBad()
	SetNodeToBad(nodeName string)
	SetNodeToHealthy(nodeName string)

	SchedulePod(podName string, podGroupSchedulingRequest apiv2.PodSchedulingSpec, phase internal.SchedulingPhase)
	DeallocatePod(podName string)

	AssertPodBindResult(podName string, expectedResult bindResult)
	AssertPodPreemptResult(podName string, expectedResult preemptResult)
	AssertPodWait(podName string)
	AssertPodPanic(podName string)
	ExecuteCaseFromYamlFile(filePath string)
}

type GenericHivedAlgorithmTester struct {
	h *HivedAlgorithm
	t *testing.T
	// allNodes records all node names in the configuration
	allNodes []string
	// podScheduleResult map podName to internal.PodScheduleResult.
	// pods which have valid scheduling result will be kept in this map.
	podScheduleResult map[string]internal.PodScheduleResult
	// panicPodNames record the pods which panic during scheduling,
	// not including those with preempt info or wait info.
	panicPodNames map[string]bool
	// pods record pod definition.
	pods map[string]*core.Pod
}

func NewHivedAlgorithmTester(t *testing.T, configFilePath string) *GenericHivedAlgorithmTester {
	sConfig := api.NewConfig(api.InitRawConfig(&configFilePath))
	h := NewHivedAlgorithm(sConfig)
	var allNodes []string
	for _, ccl := range h.fullCellList {
		for _, c := range ccl[CellLevel(len(ccl))] {
			allNodes = append(allNodes, c.(*PhysicalCell).nodes...)
		}
	}
	// sort chains of each leaf cell type for stability of the test
	for _, chains := range h.cellChains {
		var chainsTemp []string
		for _, c := range chains {
			chainsTemp = append(chainsTemp, string(c))
		}
		sort.Strings(chainsTemp)
		for i := range chains {
			chains[i] = CellChain(chainsTemp[len(chainsTemp)-i-1])
		}
	}
	tester := GenericHivedAlgorithmTester{h: h, t: t, allNodes: allNodes}
	tester.SetAllNodesToHealthy()
	tester.pods = make(map[string]*core.Pod)
	tester.podScheduleResult = make(map[string]internal.PodScheduleResult)
	tester.panicPodNames = make(map[string]bool)
	return &tester
}

func (tester *GenericHivedAlgorithmTester) SetAllNodesToHealthy() {
	h := tester.h
	for _, nodeName := range tester.allNodes {
		h.setHealthyNode(nodeName)
	}
}

func (tester *GenericHivedAlgorithmTester) SetNodeToBad(nodeName string) {
	h := tester.h
	h.setBadNode(nodeName)
}

func (tester *GenericHivedAlgorithmTester) SetNodeToHealthy(nodeName string) {
	h := tester.h
	h.setHealthyNode(nodeName)
}

func (tester *GenericHivedAlgorithmTester) SchedulePod(podName string, podGroupSchedulingRequest apiv2.PodSchedulingSpec, phase internal.SchedulingPhase) {
	h := tester.h
	t := tester.t
	defer func() {
		if err := recover(); err != nil {
			// record the panic
			t.Logf("Panic detected for pod %v. Details: %v", podName, err)
			tester.panicPodNames[podName] = true
		}
	}()
	pod := &core.Pod{
		ObjectMeta: meta.ObjectMeta{
			Name:        podName,
			Namespace:   "test",
			UID:         types.UID(podName),
			Annotations: map[string]string{},
		},
	}
	pod.Annotations[api.AnnotationKeyPodSchedulingSpec] = common.ToYaml(podGroupSchedulingRequest)
	psr := h.Schedule(pod, tester.allNodes, phase)
	if psr.PodBindInfo != nil {
		allocatedPod := internal.NewBindingPod(pod, psr.PodBindInfo)
		h.AddAllocatedPod(allocatedPod)
		tester.podScheduleResult[podName] = psr
		tester.pods[podName] = allocatedPod
	} else if psr.PodPreemptInfo != nil {
		for _, victimPod := range psr.PodPreemptInfo.VictimPods {
			h.DeleteAllocatedPod(victimPod)
		}
		tester.pods[podName] = pod
		tester.podScheduleResult[podName] = psr
	} else {
		tester.pods[podName] = pod
		tester.podScheduleResult[podName] = psr
	}
}

func (tester *GenericHivedAlgorithmTester) DeallocatePod(podName string) {
	h := tester.h
	pod, ok := tester.pods[podName]
	if ok {
		h.DeleteAllocatedPod(pod)
		delete(tester.pods, podName)
	} else {
		panic("Cannot find pod " + podName)
	}
}

func (tester *GenericHivedAlgorithmTester) AssertPodBindResult(podName string, expectedResult bindResult) {
	t := tester.t
	psr, ok := tester.podScheduleResult[podName]
	if ok {
		if psr.PodBindInfo == nil {
			t.Errorf("AssertPodBindResult failed for pod %v: Cannot find valid PodBindInfo!", podName)
		} else {
			podBindInfo := *psr.PodBindInfo
			if expectedResult.Node != podBindInfo.Node {
				t.Errorf("AssertPodBindResult failed for pod %v: Expected node is %v but got node %v", podName, expectedResult.Node, podBindInfo.Node)
			} else {
				if !reflect.DeepEqual(expectedResult.LeafCellIsolation, podBindInfo.LeafCellIsolation) {
					t.Errorf("AssertPodBindResult failed for pod %v: Expected LeafCellIsolation is %v but got %v", podName,
						expectedResult.LeafCellIsolation, podBindInfo.LeafCellIsolation)
				} else {
					t.Logf("AssertPodBindResult ok for pod %v.", podName)
				}
			}
		}
	} else {
		t.Errorf("AssertPodBindResult failed for pod %v: Cannot find valid schedule result!", podName)
	}
}

func (tester *GenericHivedAlgorithmTester) AssertPodPreemptResult(podName string, expectedResult preemptResult) {
	t := tester.t
	psr, ok := tester.podScheduleResult[podName]
	if ok {
		if psr.PodPreemptInfo == nil {
			t.Errorf("AssertPodPreemptResult failed for pod %v: Cannot find valid PodPreemptInfo!", podName)
		} else {
			victimPodNames := []string{}
			for _, pod := range psr.PodPreemptInfo.VictimPods {
				victimPodNames = append(victimPodNames, pod.Name)
			}
			expectedVictimPodNames := []string{}
			for _, podName := range expectedResult.VictimPods {
				expectedVictimPodNames = append(expectedVictimPodNames, podName)
			}
			sort.Strings(expectedVictimPodNames)
			sort.Strings(victimPodNames)
			if !reflect.DeepEqual(expectedVictimPodNames, victimPodNames) {
				t.Errorf("AssertPodPreemptResult failed for pod %v: Expected victim pods are %v but got %v", podName,
					expectedVictimPodNames, victimPodNames)
			} else {
				t.Logf("AssertPodPreemptResult ok for pod %v.", podName)
			}
		}
	} else {
		t.Errorf("AssertPodPreemptResult failed for pod %v: Cannot find valid schedule result!", podName)
	}
}

func (tester *GenericHivedAlgorithmTester) AssertPodWait(podName string) {
	t := tester.t
	psr, ok := tester.podScheduleResult[podName]
	if ok {
		if psr.PodWaitInfo == nil {
			t.Errorf("AssertPodWait failed for pod %v: Cannot find valid PodWaitInfo!", podName)
		} else {
			t.Logf("AssertPodWait ok for pod %v.", podName)
		}
	} else {
		t.Errorf("AssertPodWait failed for pod %v: Cannot find valid schedule result!", podName)
	}
}

func (tester *GenericHivedAlgorithmTester) AssertPodPanic(podName string) {
	t := tester.t
	_, ok := tester.panicPodNames[podName]
	if !ok {
		t.Errorf("AssertPodPanic failed for pod %v .", podName)
	} else {
		t.Logf("AssertPodPanic ok for pod %v.", podName)
	}
}

func (tester *GenericHivedAlgorithmTester) ExecuteCaseFromYamlFile(filePath string) {
	yamlBytes, err := ioutil.ReadFile(filePath)
	if err != nil {
		panic(fmt.Errorf("Failed to read test case file: %v, %v", filePath, err))
	}
	steps := stepList{}
	common.FromYaml(string(yamlBytes), &steps)
	for _, step := range steps {
		if step.Method == "SchedulePod" {
			var podName = step.Paramaters["podName"].(string)
			var phase = internal.SchedulingPhase(step.Paramaters["phase"].(string))
			podGroupSchedulingRequest := apiv2.PodSchedulingSpec{}
			common.FromYaml(common.ToYaml(step.Paramaters["podGroupSchedulingRequest"]), &podGroupSchedulingRequest)
			tester.SchedulePod(podName, podGroupSchedulingRequest, phase)
		} else if step.Method == "DeallocatePod" {
			var podName = step.Paramaters["podName"].(string)
			tester.DeallocatePod(podName)
		} else if step.Method == "AssertPodBindResult" {
			var podName = step.Paramaters["podName"].(string)
			expectedResult := bindResult{}
			common.FromYaml(common.ToYaml(step.Paramaters["expectedResult"]), &expectedResult)
			tester.AssertPodBindResult(podName, expectedResult)
		} else if step.Method == "AssertPodPreemptResult" {
			var podName = step.Paramaters["podName"].(string)
			expectedResult := preemptResult{}
			common.FromYaml(common.ToYaml(step.Paramaters["expectedResult"]), &expectedResult)
			tester.AssertPodPreemptResult(podName, expectedResult)
		} else if step.Method == "AssertPodWait" {
			var podName = step.Paramaters["podName"].(string)
			tester.AssertPodWait(podName)
		} else if step.Method == "AssertPodPanic" {
			var podName = step.Paramaters["podName"].(string)
			tester.AssertPodPanic(podName)
		} else if step.Method == "SetNodeToBad" {
			var nodeName = step.Paramaters["nodeName"].(string)
			tester.SetNodeToBad(nodeName)
		} else {
			panic(fmt.Errorf("The method %v is not implemented!", step.Method))
		}
	}
}
