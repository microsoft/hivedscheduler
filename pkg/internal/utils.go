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

package internal

import (
	"fmt"
	"net/http"
	"strings"

	si "github.com/microsoft/hivedscheduler/pkg/api"
	apiv2 "github.com/microsoft/hivedscheduler/pkg/api/v2"
	"github.com/microsoft/hivedscheduler/pkg/common"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeClient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
)

func CreateClient(kConfig *rest.Config) kubeClient.Interface {
	kClient, err := kubeClient.NewForConfig(kConfig)
	if err != nil {
		panic(fmt.Errorf("Failed to create KubeClient: %v", err))
	}

	return kClient
}

func GetKey(obj interface{}) (string, error) {
	return cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
}

func SplitKey(key string) (namespace, name string, err error) {
	return cache.SplitMetaNamespaceKey(key)
}

// obj should come from Pod SharedIndexInformer, otherwise may panic.
func ToPod(obj interface{}) *core.Pod {
	pod, ok := obj.(*core.Pod)

	if !ok {
		deletedFinalStateUnknown, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			panic(fmt.Errorf(
				"Failed to convert obj to Pod or DeletedFinalStateUnknown: %#v",
				obj))
		}

		pod, ok = deletedFinalStateUnknown.Obj.(*core.Pod)
		if !ok {
			panic(fmt.Errorf(
				"Failed to convert DeletedFinalStateUnknown.Obj to Pod: %#v",
				deletedFinalStateUnknown))
		}
	}

	return pod
}

// obj should come from Node SharedIndexInformer, otherwise may panic.
func ToNode(obj interface{}) *core.Node {
	node, ok := obj.(*core.Node)

	if !ok {
		deletedFinalStateUnknown, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			panic(fmt.Errorf(
				"Failed to convert obj to Node or DeletedFinalStateUnknown: %#v",
				obj))
		}

		node, ok = deletedFinalStateUnknown.Obj.(*core.Node)
		if !ok {
			panic(fmt.Errorf(
				"Failed to convert DeletedFinalStateUnknown.Obj to Node: %#v",
				deletedFinalStateUnknown))
		}
	}

	return node
}

func Key(p *core.Pod) string {
	return fmt.Sprintf("%v(%v/%v)", p.UID, p.Namespace, p.Name)
}

func IsCompleted(pod *core.Pod) bool {
	return pod.Status.Phase == core.PodSucceeded ||
		pod.Status.Phase == core.PodFailed
}

func IsLive(pod *core.Pod) bool {
	return !IsCompleted(pod)
}

func isHivedEnabledForContainers(containers []core.Container) bool {
	for _, container := range containers {
		// No need to check Requests, since extended resource must set Limits.
		for resourceName, resourceQuantity := range container.Resources.Limits {
			if resourceName == si.ResourceNamePodSchedulingEnable &&
				resourceQuantity.Sign() > 0 {
				return true
			}
		}
	}

	return false
}

func IsHivedEnabled(pod *core.Pod) bool {
	if isHivedEnabledForContainers(pod.Spec.InitContainers) {
		return true
	}
	if isHivedEnabledForContainers(pod.Spec.Containers) {
		return true
	}

	return false
}

// Aligned with K8S HTTPExtender, so that Informer is also aligned with WebServer.
func IsInterested(pod *core.Pod) bool {
	// A completed Pod is the same as a deleted Pod from the scheduler view, so only
	// track live Pod.
	return IsLive(pod) && IsHivedEnabled(pod)
}

// Scheduler only can see live Pod, so the IsLive() check is usually redundant.
// A bound Pod means it is currently holding its requested resources.
func IsBound(pod *core.Pod) bool {
	return pod.Spec.NodeName != "" && IsLive(pod)
}

// An unbound Pod means it is currently waiting on its requested resources.
func IsUnbound(pod *core.Pod) bool {
	return pod.Spec.NodeName == "" && IsLive(pod)
}

// A node is considered healthy if it is not unschedulable and in ready condition.
func IsNodeHealthy(node *core.Node) bool {
	if node.Spec.Unschedulable {
		return false
	}
	for _, c := range node.Status.Conditions {
		if c.Type == core.NodeReady && c.Status == core.ConditionTrue {
			return true
		}
	}
	return false
}

func NewBindingPod(pod *core.Pod, podBindInfo *apiv2.PodBindInfo) *core.Pod {
	bindingPod := pod.DeepCopy()

	bindingPod.Spec.NodeName = podBindInfo.Node

	if bindingPod.Annotations == nil {
		bindingPod.Annotations = map[string]string{}
	}
	bindingPod.Annotations[si.AnnotationKeyPodLeafCellIsolation] =
		common.ToIndicesString(podBindInfo.LeafCellIsolation)
	bindingPod.Annotations[si.AnnotationKeyPodBindInfo] =
		common.ToYaml(podBindInfo)

	return bindingPod
}

// converts old spec annotations for backward compatibility
func convertOldAnnotation(annotation string) string {
	r := strings.NewReplacer(
		"gpuType", "leafCellType",
		"gpuNumber", "leafCellNumber",
		"gpuIsolation", "leafCellIsolation",
		"physicalGpuIndices", "physicalLeafCellIndices",
	)
	return r.Replace(annotation)
}

// PodBindInfo comes from internal, so just need to assert when deserialization.
func ExtractPodBindInfo(allocatedPod *core.Pod) *apiv2.PodBindInfo {
	podBindInfo := apiv2.PodBindInfo{}

	annotation := convertOldAnnotation(allocatedPod.Annotations[si.AnnotationKeyPodBindInfo])
	// TODO: backward compatibility
	if annotation == "" {
		panic(fmt.Errorf(
			"Pod does not contain or contains empty annotation: %v",
			si.AnnotationKeyPodBindInfo))
	}

	common.FromYaml(annotation, &podBindInfo)
	return &podBindInfo
}

func ExtractPodBindAnnotations(allocatedPod *core.Pod) map[string]string {
	if _, ok := allocatedPod.Annotations[si.AnnotationKeyPodLeafCellIsolation]; ok {
		return map[string]string{
			si.AnnotationKeyPodLeafCellIsolation: allocatedPod.Annotations[si.AnnotationKeyPodLeafCellIsolation],
			si.AnnotationKeyPodBindInfo:          allocatedPod.Annotations[si.AnnotationKeyPodBindInfo],
		}
	} else {
		return map[string]string{
			si.AnnotationKeyPodLeafCellIsolation: allocatedPod.Annotations[si.DeprecatedAnnotationKeyPodGpuIsolation],
			si.AnnotationKeyPodBindInfo:          convertOldAnnotation(allocatedPod.Annotations[si.AnnotationKeyPodBindInfo]),
		}
	}
}

// ExtractPodSchedulingSpec extracts pod scheduling request from k8s pod request.
// TODO: Need more defaulting and validation when deserialization, for example,
// check cell type hierarchies, cell type no higher than node level, cell number limit, etc.
func ExtractPodSchedulingSpec(pod *core.Pod) *apiv2.PodSchedulingSpec {
	// Consider all panics are BadRequestPanic.
	defer AsBadRequestPanic()
	errPfx := fmt.Sprintf("Pod annotation %v: ", si.AnnotationKeyPodSchedulingSpec)

	annotation := convertOldAnnotation(pod.Annotations[si.AnnotationKeyPodSchedulingSpec])
	if annotation == "" {
		panic(fmt.Errorf(errPfx + "Annotation does not exist or is empty"))
	}

	podSchedulingSpec := apiv2.PodSchedulingSpec{Version: "v2"}

	generalSpec := si.GeneralSpec{"version": "v1"}
	common.FromYaml(annotation, &generalSpec)
	switch generalSpec["version"] {
	case "v1":
		podSchedulingSpecV1 := si.PodSchedulingSpec{IgnoreK8sSuggestedNodes: true}
		common.FromYaml(annotation, &podSchedulingSpecV1)
		// v1 Defaulting
		if podSchedulingSpecV1.AffinityGroup == nil {
			podSchedulingSpecV1.AffinityGroup = &si.AffinityGroupSpec{
				Name: fmt.Sprintf("%v/%v", pod.Namespace, pod.Name),
				Members: []si.AffinityGroupMemberSpec{{
					PodNumber:      1,
					LeafCellNumber: podSchedulingSpecV1.LeafCellNumber},
				},
			}
		}
		// v1 Validation
		if podSchedulingSpecV1.VirtualCluster == "" {
			panic(fmt.Errorf(errPfx + "VirtualCluster is empty"))
		}
		if podSchedulingSpecV1.Priority < si.OpportunisticPriority {
			panic(fmt.Errorf(errPfx+"Priority is less than %v", si.OpportunisticPriority))
		}
		if podSchedulingSpecV1.Priority > si.MaxGuaranteedPriority {
			panic(fmt.Errorf(errPfx+"Priority is greater than %v", si.MaxGuaranteedPriority))
		}
		if podSchedulingSpecV1.LeafCellNumber <= 0 {
			panic(fmt.Errorf(errPfx + "LeafCellNumber is non-positive"))
		}
		if podSchedulingSpecV1.AffinityGroup.Name == "" {
			panic(fmt.Errorf(errPfx + "AffinityGroup.Name is empty"))
		}
		isPodInGroup := false
		for _, member := range podSchedulingSpecV1.AffinityGroup.Members {
			if member.PodNumber <= 0 {
				panic(fmt.Errorf(errPfx + "AffinityGroup.Members has non-positive PodNumber"))
			}
			if member.LeafCellNumber <= 0 {
				panic(fmt.Errorf(errPfx + "AffinityGroup.Members has non-positive LeafCellNumber"))
			}
			if member.LeafCellNumber == podSchedulingSpecV1.LeafCellNumber {
				isPodInGroup = true
			}
		}
		if !isPodInGroup {
			panic(fmt.Errorf(errPfx + "AffinityGroup.Members does not contains current Pod"))
		}
		// convert to v2
		podSchedulingSpec.ConvertFromV1(&podSchedulingSpecV1)
	case "v2":
		common.FromYaml(annotation, &podSchedulingSpec)
	default:
		panic(fmt.Errorf(errPfx+"Unknown version %v", generalSpec["version"]))
	}

	// Defaulting
	podSchedulingSpec.SetDefaults(pod)

	// Validation
	if msg, ok := podSchedulingSpec.Validate(); !ok {
		panic(fmt.Errorf(errPfx + msg))
	}

	return &podSchedulingSpec
}

func BindPod(kClient kubeClient.Interface, bindingPod *core.Pod) {
	// The K8S Bind is atomic and can only succeed at most once.
	err := kClient.CoreV1().Pods(bindingPod.Namespace).Bind(&core.Binding{
		ObjectMeta: meta.ObjectMeta{
			Namespace:   bindingPod.Namespace,
			Name:        bindingPod.Name,
			UID:         bindingPod.UID,
			Annotations: ExtractPodBindAnnotations(bindingPod),
		},
		Target: core.ObjectReference{
			Kind: "Node",
			Name: bindingPod.Spec.NodeName,
		},
	})

	if err != nil {
		panic(fmt.Errorf("Failed to bind Pod: %v", err))
	}

	klog.Infof("[%v]: Succeeded to bind Pod on node %v, leaf cells %v",
		Key(bindingPod),
		bindingPod.Spec.NodeName,
		bindingPod.Annotations[si.AnnotationKeyPodLeafCellIsolation])
}

func NewBadRequestError(message string) *si.WebServerError {
	return si.NewWebServerError(http.StatusBadRequest, message)
}

// Wrap and Rethrow Panic as BadRequestError Panic
func AsBadRequestPanic() {
	if r := recover(); r != nil {
		panic(NewBadRequestError(fmt.Sprintf("%v", r)))
	}
}

// Recover User Error Panic
// Rethrow Platform Error Panic
func HandleInformerPanic(logPfx string, logOnSucceeded bool) {
	if r := recover(); r != nil {
		if err, ok := r.(*si.WebServerError); ok {
			if err.Code >= http.StatusBadRequest &&
				err.Code < http.StatusInternalServerError {
				klog.Warningf(logPfx+"Skipped due to User Error: %v", err.Error())
				return
			}
		}

		panic(fmt.Errorf(logPfx+"Failed: %v", r))
	} else if logOnSucceeded {
		klog.Infof(logPfx + "Succeeded")
	}
}

// Wrap and Rethrow Panic
func HandleRoutinePanic(logPfx string) {
	if r := recover(); r != nil {
		if err, ok := r.(*si.WebServerError); ok {
			panic(si.NewWebServerError(
				err.Code,
				fmt.Sprintf(logPfx+"Failed: %v", err.Message)))
		} else {
			panic(fmt.Errorf(logPfx+"Failed: %v", r))
		}
	} else {
		klog.Infof(logPfx + "Succeeded")
	}
}

// Log and Recover Panic
func HandleWebServerPanic(handler func(*si.WebServerError)) {
	if r := recover(); r != nil {
		err, ok := r.(*si.WebServerError)
		if !ok {
			err = si.NewWebServerError(
				http.StatusInternalServerError,
				fmt.Sprintf("%v", r))
		}

		if err.Code >= http.StatusInternalServerError {
			klog.Warningf("%v%v", err.Message, common.GetPanicDetails(r))
			err.Message = fmt.Sprintf(si.ComponentName+": Platform Error: %v", err.Message)
		} else if err.Code >= http.StatusBadRequest {
			klog.Infof("%v", err.Message)
			err.Message = fmt.Sprintf(si.ComponentName+": User Error: %v", err.Message)
		}

		if handler != nil {
			handler(err)
		}
	}
}
