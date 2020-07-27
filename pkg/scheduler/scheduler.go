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

package scheduler

import (
	"fmt"
	"sync"
	"time"

	"github.com/microsoft/hivedscheduler/pkg/algorithm"
	si "github.com/microsoft/hivedscheduler/pkg/api"
	"github.com/microsoft/hivedscheduler/pkg/common"
	"github.com/microsoft/hivedscheduler/pkg/internal"
	"github.com/microsoft/hivedscheduler/pkg/webserver"
	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/runtime"
	kubeInformer "k8s.io/client-go/informers"
	kubeClient "k8s.io/client-go/kubernetes"
	coreLister "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
	ei "k8s.io/kubernetes/pkg/scheduler/api"
)

// HivedScheduler is the scheduling framework which serves as the bridge between
// the scheduling algorithm and K8S.
// It provides the whole cluster scheduling view and the interested pod scheduling
// request from K8S to the scheduling algorithm, then it drives the scheduling
// algorithm to make the pod schedule decision, finally it will fully obey the
// decision and try its best to execute the decision to K8S.
type HivedScheduler struct {
	kConfig *rest.Config
	sConfig *si.Config

	// Client is used to write remote objects in ApiServer.
	// Remote objects are up-to-date and is writable.
	//
	// To read objects, it is better to use Lister instead of Client, since the
	// Lister is cached.
	//
	// Client already has retry policy to retry for most transient failures.
	// Client write failure does not mean the write does not succeed on remote, the
	// failure may be due to the success response is just failed to deliver to the
	// Client.
	kClient kubeClient.Interface

	// Informer is used to sync remote objects to local cached objects, and then
	// deliver corresponding events of the object changes.
	//
	// The event delivery for an object is level driven instead of edge driven,
	// and the object is identified by its name instead of its UID.
	// For example:
	// 1. Informer may not deliver any event if a create is immediately followed
	//    by a delete.
	// 2. Informer may deliver an Update event with UID changed if a delete is
	//    immediately followed by a create.
	//
	// Platform Error Panic in Informer Callbacks will not be recovered, i.e. it will
	// crash the whole process, since generally it is a ground truth failure that
	// cannot be ignored and will impact the whole scheduling.
	nodeInformer cache.SharedIndexInformer
	podInformer  cache.SharedIndexInformer

	// Lister is used to read local cached objects in Informer.
	// Local cached objects may be outdated and is not writable.
	//
	// Outdated means current local cached objects may not reflect previous Client
	// remote writes.
	//
	// Node object provides the capacity and schedulable resource of a Node.
	nodeLister coreLister.NodeLister
	// Pod object provides the bound Pods and bound resource of a Node.
	podLister coreLister.PodLister

	// WebServer is used to interact with K8S Default Scheduler and others.
	//
	// Platform Error Panic in WebServer Callbacks will be recovered, since generally
	// it is just one request failure that can be ignored and will not impact the
	// whole scheduling.
	webServer *webserver.WebServer

	// SchedulerLock is used to protect the PodScheduleStatuses and its derived
	// scheduling view inside the SchedulerAlgorithm.
	// It also ensures the SchedulerAlgorithm.Schedule() will never be executed
	// concurrently.
	schedulerLock *sync.RWMutex

	// PodScheduleStatuses serves as the ground truth of the scheduling view.
	// It tracks and caches the PodScheduleStatus for all live (not completed)
	// hived Pods, which merges the information from both SchedulerAlgorithm and
	// PodInformer, so that SchedulerAlgorithm and K8S Default Scheduler can pick
	// it up later.
	podScheduleStatuses internal.PodScheduleStatuses

	// SchedulerAlgorithm is used to make the pod schedule decision based on the
	// scheduling view.
	schedulerAlgorithm internal.SchedulerAlgorithm
}

func NewHivedScheduler() *HivedScheduler {
	klog.Infof("Initializing " + si.ComponentName)

	sConfig := si.NewConfig(si.InitRawConfig(nil))
	klog.Infof("With Config: \n%v", common.ToYaml(sConfig))
	kConfig := si.BuildKubeConfig(sConfig)

	kClient := internal.CreateClient(kConfig)

	nodeListerInformer := kubeInformer.NewSharedInformerFactory(kClient, 0).Core().V1().Nodes()
	podListerInformer := kubeInformer.NewSharedInformerFactory(kClient, 0).Core().V1().Pods()
	nodeInformer := nodeListerInformer.Informer()
	podInformer := podListerInformer.Informer()
	nodeLister := nodeListerInformer.Lister()
	podLister := podListerInformer.Lister()

	s := &HivedScheduler{
		kConfig:             kConfig,
		sConfig:             sConfig,
		kClient:             kClient,
		nodeInformer:        nodeInformer,
		podInformer:         podInformer,
		nodeLister:          nodeLister,
		podLister:           podLister,
		schedulerLock:       &sync.RWMutex{},
		podScheduleStatuses: internal.PodScheduleStatuses{},
		schedulerAlgorithm:  algorithm.NewHivedAlgorithm(sConfig),
	}

	// Setup Informer Callbacks
	s.nodeInformer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    s.addNode,
			UpdateFunc: s.updateNode,
			DeleteFunc: s.deleteNode,
		},
	)

	s.podInformer.AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				pod := internal.ToPod(obj)
				return internal.IsInterested(pod)
			},
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    s.addPod,
				UpdateFunc: s.updatePod,
				DeleteFunc: s.deletePod,
			},
		},
	)

	// Setup WebServer Callbacks
	s.webServer = webserver.NewWebServer(
		sConfig,
		internal.ExtenderHandlers{
			FilterHandler:  s.filterRoutine,
			BindHandler:    s.bindRoutine,
			PreemptHandler: s.preemptRoutine,
		},
		internal.InspectHandlers{
			GetAllAffinityGroupsHandler:        s.getAllAffinityGroups,
			GetAffinityGroupHandler:            s.getAffinityGroup,
			GetClusterStatusHandler:            s.getClusterStatus,
			GetPhysicalClusterStatusHandler:    s.getPhysicalClusterStatus,
			GetAllVirtualClustersStatusHandler: s.getAllVirtualClustersStatus,
			GetVirtualClusterStatusHandler:     s.getVirtualClusterStatus,
		},
	)

	return s
}

func (s *HivedScheduler) Run(stopCh <-chan struct{}) {
	defer klog.Errorf("Stopping " + si.ComponentName)
	defer runtime.HandleCrash()

	klog.Infof("Recovering " + si.ComponentName)

	go s.nodeInformer.Run(stopCh)
	go s.podInformer.Run(stopCh)
	if !cache.WaitForCacheSync(
		stopCh,
		s.nodeInformer.HasSynced,
		s.podInformer.HasSynced) {
		panic(fmt.Errorf("Failed to WaitForCacheSync"))
	}

	// Previous bound pods recovery completed, start to accept scheduling request.
	s.webServer.AsyncRun(stopCh)
	klog.Infof("Running " + si.ComponentName)

	<-stopCh
}

func (s *HivedScheduler) addNode(obj interface{}) {
	node := internal.ToNode(obj)
	logPfx := fmt.Sprintf("[%v]: addNode: ", node.Name)
	klog.Infof(logPfx + "Started")
	defer internal.HandleInformerPanic(logPfx, true)

	s.schedulerAlgorithm.AddNode(node)
}

func (s *HivedScheduler) updateNode(oldObj, newObj interface{}) {
	oldNode := internal.ToNode(oldObj)
	newNode := internal.ToNode(newObj)
	// Informer may deliver an Update event with UID changed if a delete is
	// immediately followed by a create, so manually decompose it.
	if oldNode.UID != newNode.UID {
		s.deleteNode(oldObj)
		s.addNode(newObj)
		return
	}

	logPfx := fmt.Sprintf("[%v]: updateNode: ", newNode.Name)
	defer internal.HandleInformerPanic(logPfx, false)

	s.schedulerAlgorithm.UpdateNode(oldNode, newNode)
}

func (s *HivedScheduler) deleteNode(obj interface{}) {
	node := internal.ToNode(obj)
	logPfx := fmt.Sprintf("[%v]: deleteNode: ", node.Name)
	klog.Infof(logPfx + "Started")
	defer internal.HandleInformerPanic(logPfx, true)

	s.schedulerAlgorithm.DeleteNode(node)
}

func (s *HivedScheduler) addPod(obj interface{}) {
	pod := internal.ToPod(obj)
	if internal.IsBound(pod) {
		s.addBoundPod(pod)
	} else {
		s.addUnboundPod(pod)
	}
}

func (s *HivedScheduler) updatePod(oldObj, newObj interface{}) {
	oldPod := internal.ToPod(oldObj)
	newPod := internal.ToPod(newObj)
	// Informer may deliver an Update event with UID changed if a delete is
	// immediately followed by a create, so manually decompose it.
	if oldPod.UID != newPod.UID {
		s.deletePod(oldObj)
		s.addPod(newObj)
		return
	}

	oldBound := internal.IsBound(oldPod)
	newBound := internal.IsBound(newPod)
	if !oldBound && newBound {
		s.addBoundPod(newPod)
	} else if oldBound && !newBound {
		// Unreachable
		panic(fmt.Errorf(
			"[%v]: Pod updated from bound to unbound: previous bound node: %v",
			internal.Key(newPod), oldPod.Spec.NodeName))
	}
}

func (s *HivedScheduler) deletePod(obj interface{}) {
	pod := internal.ToPod(obj)
	s.schedulerLock.Lock()
	defer s.schedulerLock.Unlock()

	logPfx := fmt.Sprintf("[%v]: deletePod: ", internal.Key(pod))
	klog.Infof(logPfx + "Started")
	defer internal.HandleInformerPanic(logPfx, true)

	podStatus := s.podScheduleStatuses[pod.UID]
	if podStatus != nil {
		if internal.IsAllocated(podStatus.PodState) {
			s.schedulerAlgorithm.DeleteAllocatedPod(podStatus.Pod)
		} else {
			s.schedulerAlgorithm.DeleteUnallocatedPod(podStatus.Pod)
		}

		delete(s.podScheduleStatuses, pod.UID)
	}
}

func (s *HivedScheduler) addBoundPod(pod *core.Pod) {
	s.schedulerLock.Lock()
	defer s.schedulerLock.Unlock()

	logPfx := fmt.Sprintf("[%v]: addBoundPod: ", internal.Key(pod))
	klog.Infof(logPfx + "Started")
	defer internal.HandleInformerPanic(logPfx, true)

	podStatus := s.podScheduleStatuses[pod.UID]
	if podStatus != nil {
		if internal.IsAllocated(podStatus.PodState) {
			// Already allocated, so the placement should never be changed again.
			// So, not need to update the allocated pod.
			if podStatus.PodState != internal.PodBound {
				s.podScheduleStatuses[pod.UID] = &internal.PodScheduleStatus{
					Pod:               podStatus.Pod,
					PodState:          internal.PodBound,
					PodScheduleResult: nil,
				}
			}
			return
		}
	}

	// Recover bound pod.
	s.schedulerAlgorithm.AddAllocatedPod(pod)
	s.podScheduleStatuses[pod.UID] = &internal.PodScheduleStatus{
		Pod:               pod,
		PodState:          internal.PodBound,
		PodScheduleResult: nil,
	}
}

func (s *HivedScheduler) addUnboundPod(pod *core.Pod) {
	s.schedulerLock.Lock()
	defer s.schedulerLock.Unlock()

	logPfx := fmt.Sprintf("[%v]: addUnboundPod: ", internal.Key(pod))
	klog.Infof(logPfx + "Started")
	defer internal.HandleInformerPanic(logPfx, true)

	podStatus := s.podScheduleStatuses[pod.UID]
	if podStatus != nil {
		// Keep the existing one.
		return
	}

	// Receive newly unbound pod, so it must be PodWaiting.
	s.schedulerAlgorithm.AddUnallocatedPod(pod)
	s.podScheduleStatuses[pod.UID] = &internal.PodScheduleStatus{
		Pod:               pod,
		PodState:          internal.PodWaiting,
		PodScheduleResult: nil,
	}
}

// Only live (not completed) unbound hived Pods are accepted to be scheduled.
// Return unbound PodScheduleStatus.
func (s *HivedScheduler) generalScheduleAdmissionCheck(
	podStatus *internal.PodScheduleStatus) *internal.PodScheduleStatus {
	if podStatus == nil {
		// If the pod does not exist or completed:
		// The inconsistency should can be reconciled by K8S Default Scheduler.
		// If the pod has not been informed to the scheduler:
		// The inconsistency should can be reconciled by the scheduler PodInformer.
		panic(internal.NewBadRequestError(fmt.Sprintf(
			"Pod does not exist, completed or has not been informed to the scheduler")))
	} else {
		if podStatus.PodState == internal.PodBound {
			// The inconsistency should can be reconciled by K8S Default Scheduler.
			panic(internal.NewBadRequestError(fmt.Sprintf(
				"Pod has already been bound to node %v",
				podStatus.Pod.Spec.NodeName)))
		}
	}

	return podStatus
}

func (s *HivedScheduler) validatePodBindInfo(
	podBindInfo *si.PodBindInfo, suggestedNodes []string) error {
	node := podBindInfo.Node

	// Check against existing nodes
	_, getErr := s.nodeLister.Get(node)
	if getErr != nil {
		if apiErrors.IsNotFound(getErr) {
			// If the node does not exist:
			// The inconsistency cannot be reconciled, the Pod should be still bound
			// and then retried with another one, as normal pod deleted on node
			// deleted.
			// If the node has not been informed to the scheduler:
			// The inconsistency should can be reconciled by the scheduler NodeInformer.
			return fmt.Errorf(
				"The SchedulerAlgorithm decided to bind on node %v, but the node "+
					"does not exist or has not been informed to the scheduler", node)
		} else {
			// The inconsistency should can be reconciled by the scheduler NodeInformer.
			return fmt.Errorf(
				"Failed to check whether the SchedulerAlgorithm decided to bind on "+
					"not existing node %v. Node cannot be got from local cache: %v",
				node, getErr)
		}
	}

	// Check against suggested nodes
	if !common.StringsContains(suggestedNodes, node) {
		return fmt.Errorf(
			"The SchedulerAlgorithm decided to bind on node %v but the node "+
				"is not within the selected nodes from K8S Default Scheduler. "+
				"So, the binding is incompatible with K8S Default Scheduler.",
			node)
	}

	return nil
}

func (s *HivedScheduler) shouldForceBind(
	podStatus *internal.PodScheduleStatus, suggestedNodes []string) bool {
	pod := podStatus.Pod
	podBindAttempts := podStatus.PodBindAttempts
	podBindInfo := podStatus.PodScheduleResult.PodBindInfo

	logPfx := fmt.Sprintf("[%v]: Will force bind Pod: ", internal.Key(pod))

	// Ensure the Pod can be bound according to the pod schedule decision eventually.
	//
	// Therefore, if the decision is really problematic based on current status,
	// the Pod can fail itself or can be deleted by the GarbageCollectionController,
	// then can be retried with another one, instead of potential forever binding
	// or wrongly rolled back to PodWaiting.
	//
	// Given that the SchedulerAlgorithm should avoid make problematic decision
	// based on current status, the retried Pod should can be scheduled on suitable
	// placement decision eventually.
	// Thus, the problematic decision can only be stale decision, i.e. only newly
	// bad devices or newly deleted Nodes will lead Pod retried.
	// For newly bad devices, it is like the normal behaviour that a pod will fail
	// after the devices it runs on become unhealthy.
	// For newly deleted Nodes, it is like the normal behaviour that a pod will
	// be deleted by the GarbageCollectionController after the node it runs on is
	// deleted.
	//
	// So overall keeps on binding, regardless of potential problematic decision,
	// is acceptable.
	if podBindAttempts >= *s.sConfig.ForcePodBindThreshold {
		klog.Warningf(logPfx+
			"The Pod binding has already been tried %v times which reaches the "+
			"ForcePodBindThreshold %v",
			podBindAttempts, *s.sConfig.ForcePodBindThreshold)
		return true
	} else if err := s.validatePodBindInfo(podBindInfo, suggestedNodes); err != nil {
		// Proactively trigger force bind, if the pod schedule decision has already
		// been detected to be probably invalid based on current status, to reduce
		// the binding time.
		klog.Warningf(logPfx+"%v", err)
		return true
	}

	return false
}

// Bypass K8S Default Scheduler to directly trigger the bindRoutine, it can be
// considered as a normal shadow of the previous bindRoutine if called
// asynchronously.
func (s *HivedScheduler) forceBindExecutor(bindingPod *core.Pod) {
	logPfx := fmt.Sprintf("[%v]: forceBindExecutor: ", internal.Key(bindingPod))
	klog.Infof(logPfx + "Started")
	defer internal.HandleWebServerPanic(nil)
	defer internal.HandleRoutinePanic(logPfx)

	s.bindRoutine(ei.ExtenderBindingArgs{
		PodNamespace: bindingPod.Namespace,
		PodName:      bindingPod.Name,
		PodUID:       bindingPod.UID,
		Node:         bindingPod.Spec.NodeName,
	})
}

func (s *HivedScheduler) filterRoutine(args ei.ExtenderArgs) *ei.ExtenderFilterResult {
	s.schedulerLock.Lock()
	defer s.schedulerLock.Unlock()

	pod := args.Pod
	suggestedNodes := *args.NodeNames

	logPfx := fmt.Sprintf("[%v]: filterRoutine: ", internal.Key(pod))
	klog.Infof(logPfx + "Started")
	defer internal.HandleRoutinePanic(logPfx)

	podStatus := s.generalScheduleAdmissionCheck(s.podScheduleStatuses[pod.UID])
	if podStatus.PodState == internal.PodBinding {
		// Insist previous bind result, since Pod binding should be idempotent, and
		// it is already assumed as allocated by scheduling algorithm which cannot
		// be rolled back.
		bindingPod := podStatus.Pod
		podStatus.PodBindAttempts++

		if s.shouldForceBind(podStatus, suggestedNodes) {
			go s.forceBindExecutor(bindingPod)
		}
		return &ei.ExtenderFilterResult{
			NodeNames: &[]string{bindingPod.Spec.NodeName},
		}
	}

	// At this point, podState must be in:
	// {PodWaiting, PodPreempting}

	// Carry out a new scheduling
	result := s.schedulerAlgorithm.Schedule(pod, suggestedNodes, internal.FilteringPhase)

	if result.PodBindInfo != nil {
		bindingPod := internal.NewBindingPod(pod, result.PodBindInfo)

		// Assume binding pod as allocated, so that next scheduling does not need to
		// wait current binding completed.
		s.schedulerAlgorithm.AddAllocatedPod(bindingPod)

		// Transition to PodBinding only after AddAllocatedPod succeeded.
		s.podScheduleStatuses[pod.UID] = &internal.PodScheduleStatus{
			Pod:               bindingPod,
			PodState:          internal.PodBinding,
			PodScheduleResult: &result,
		}

		if s.shouldForceBind(s.podScheduleStatuses[pod.UID], suggestedNodes) {
			go s.forceBindExecutor(bindingPod)
		}

		klog.Infof(logPfx+"Pod is binding: %v", common.ToJson(result.PodBindInfo))
		return &ei.ExtenderFilterResult{
			NodeNames: &[]string{bindingPod.Spec.NodeName},
		}
	} else if result.PodPreemptInfo != nil {
		// Return FailedNodes to tell K8S Default Scheduler that preemption may help.
		failedNodes := map[string]string{}
		for _, victim := range result.PodPreemptInfo.VictimPods {
			node := victim.Spec.NodeName
			if _, ok := failedNodes[node]; !ok {
				failedNodes[node] = fmt.Sprintf(
					"node(%v) has preemptible Pods: %v",
					node, internal.Key(victim))
			} else {
				failedNodes[node] += ", " + internal.Key(victim)
			}
		}

		klog.Infof(logPfx+
			"Pod is waiting for preemptRoutine as preemptible resource appeared: %v",
			common.ToJson(failedNodes))
		return &ei.ExtenderFilterResult{
			FailedNodes: failedNodes,
		}
	} else {
		s.podScheduleStatuses[pod.UID] = &internal.PodScheduleStatus{
			Pod:               pod,
			PodState:          internal.PodWaiting,
			PodScheduleResult: &result,
		}

		// Block the whole scheduling to achieve better FIFO
		if *s.sConfig.WaitingPodSchedulingBlockMilliSec > 0 {
			time.Sleep(time.Duration(*s.sConfig.WaitingPodSchedulingBlockMilliSec) *
				time.Millisecond)
		}

		// Return fake FailedNodes, so that the waitReason can be exposed along with
		// other waitReasons generated from K8S Default Scheduler.
		failedNodes := map[string]string{}
		waitReason := "Pod is waiting for preemptible or free resource to appear"
		if result.PodWaitInfo != nil {
			waitReason += ": " + result.PodWaitInfo.Reason
		}
		failedNodes[si.ComponentName] = waitReason

		klog.Infof(logPfx + waitReason)
		return &ei.ExtenderFilterResult{
			FailedNodes: failedNodes,
		}
	}
}

// Bind the Pod based on its corresponding bindingPod.
// Notes:
// 1. It should be idempotent since it may be called multiple times for the same
//    pod. This ensures that once a specific Pod is allocated by AddAllocatedPod,
//    its placement will never be changed to another one.
func (s *HivedScheduler) bindRoutine(args ei.ExtenderBindingArgs) *ei.ExtenderBindingResult {
	s.schedulerLock.RLock()
	defer s.schedulerLock.RUnlock()

	podKey := internal.NewPodKey(args.PodNamespace, args.PodName, args.PodUID)
	bindingNode := args.Node

	logPfx := fmt.Sprintf("[%v]: bindRoutine: ", podKey)
	klog.Infof(logPfx + "Started")
	defer internal.HandleRoutinePanic(logPfx)

	podStatus := s.generalScheduleAdmissionCheck(s.podScheduleStatuses[podKey.UID])
	if podStatus.PodState == internal.PodBinding {
		bindingPod := podStatus.Pod
		if bindingPod.Spec.NodeName != bindingNode {
			// The inconsistency should can be reconciled by K8S Default Scheduler.
			panic(internal.NewBadRequestError(fmt.Sprintf(
				"Pod binding node mismatch: expected %v, received %v",
				podStatus.Pod.Spec.NodeName, bindingNode)))
		}

		internal.BindPod(s.kClient, bindingPod)
		return &ei.ExtenderBindingResult{}
	}

	// At this point, podState must be in:
	// {PodWaiting, PodPreempting}

	// The inconsistency should can be reconciled by K8S Default Scheduler.
	panic(internal.NewBadRequestError(fmt.Sprintf(
		"Pod cannot be bound without a scheduling placement: "+
			"Pod current scheduling state %v, received node %v",
		podStatus.PodState, bindingNode)))
}

func (s *HivedScheduler) preemptRoutine(args ei.ExtenderPreemptionArgs) *ei.ExtenderPreemptionResult {
	s.schedulerLock.Lock()
	defer s.schedulerLock.Unlock()

	// Preemptor and Victims can be in different namespaces.
	pod := args.Pod
	suggestedNodes := []string{}
	for node := range args.NodeNameToMetaVictims {
		suggestedNodes = append(suggestedNodes, node)
	}

	logPfx := fmt.Sprintf("[%v]: preemptRoutine: ", internal.Key(pod))
	klog.Infof(logPfx + "Started")
	defer internal.HandleRoutinePanic(logPfx)

	podStatus := s.generalScheduleAdmissionCheck(s.podScheduleStatuses[pod.UID])
	if podStatus.PodState == internal.PodBinding {
		// The inconsistency should can be reconciled by K8S Default Scheduler.
		panic(internal.NewBadRequestError(fmt.Sprintf(
			"Pod has already been binding to node %v",
			podStatus.Pod.Spec.NodeName)))
	}

	// At this point, podState must be in:
	// {PodWaiting, PodPreempting}

	// If the podState is PodWaiting:
	// Maybe filterRoutine will never be called by K8S Default Scheduler, but only
	// preemptRoutine will be called, such as lower priority Pods used all resources.
	//
	// If the podState is PodPreempting:
	// Maybe filterRoutine will no longer be called by K8S Default Scheduler, but
	// only preemptRoutine will be called, such as lower priority Pods used all
	// resources after filterRoutine is called for the last time.
	// And we should not insist previous preemption result, as it may be stale,
	// such as, even if these victim resources are all preempted, the Pod still
	// cannot be bound to them due to they become unhealthy.
	//
	// So, in either case, we need to schedule again with more suggestedNodes, as
	// lower priority Pods are ignored by K8S Default Scheduler now.
	result := s.schedulerAlgorithm.Schedule(pod, suggestedNodes, internal.PreemptingPhase)

	if result.PodBindInfo != nil {
		klog.Infof(logPfx+
			"Pod is waiting for filterRoutine as free resource appeared: %v",
			common.ToJson(result.PodBindInfo))
		return &ei.ExtenderPreemptionResult{}
	} else if result.PodPreemptInfo != nil {
		s.podScheduleStatuses[pod.UID] = &internal.PodScheduleStatus{
			Pod:               pod,
			PodState:          internal.PodPreempting,
			PodScheduleResult: &result,
		}

		victims := result.PodPreemptInfo.VictimPods
		nodesVictims := map[string]*ei.MetaVictims{}
		nodesVictimsMsg := map[string][]string{}

		for _, victim := range victims {
			node := victim.Spec.NodeName

			if _, ok := nodesVictims[node]; !ok {
				nodesVictims[node] = &ei.MetaVictims{}
			}
			nodeVictims := nodesVictims[node]

			if nodeVictims.Pods == nil {
				nodeVictims.Pods = []*ei.MetaPod{}
			}
			nodeVictims.Pods = append(nodeVictims.Pods,
				&ei.MetaPod{UID: string(victim.UID)})
			nodesVictimsMsg[node] = append(nodesVictimsMsg[node], internal.Key(victim))
		}

		klog.Infof(logPfx+"Pod is preempting: %v", common.ToJson(nodesVictimsMsg))
		return &ei.ExtenderPreemptionResult{
			NodeNameToMetaVictims: nodesVictims,
		}
	} else {
		s.podScheduleStatuses[pod.UID] = &internal.PodScheduleStatus{
			Pod:               pod,
			PodState:          internal.PodWaiting,
			PodScheduleResult: &result,
		}

		waitReason := "Pod is waiting for preemptible or free resource to appear"
		if result.PodWaitInfo != nil {
			waitReason += ": " + result.PodWaitInfo.Reason
		}
		klog.Infof(logPfx + waitReason)
		return &ei.ExtenderPreemptionResult{}
	}
}

func (s *HivedScheduler) getAllAffinityGroups() si.AffinityGroupList {
	return s.schedulerAlgorithm.GetAllAffinityGroups()
}

func (s *HivedScheduler) getAffinityGroup(name string) si.AffinityGroup {
	return s.schedulerAlgorithm.GetAffinityGroup(name)
}

func (s *HivedScheduler) getClusterStatus() si.ClusterStatus {
	return s.schedulerAlgorithm.GetClusterStatus()
}

func (s *HivedScheduler) getPhysicalClusterStatus() si.PhysicalClusterStatus {
	return s.schedulerAlgorithm.GetPhysicalClusterStatus()
}

func (s *HivedScheduler) getAllVirtualClustersStatus() map[si.VirtualClusterName]si.VirtualClusterStatus {
	return s.schedulerAlgorithm.GetAllVirtualClustersStatus()
}

func (s *HivedScheduler) getVirtualClusterStatus(vcn si.VirtualClusterName) si.VirtualClusterStatus {
	return s.schedulerAlgorithm.GetVirtualClusterStatus(vcn)
}
