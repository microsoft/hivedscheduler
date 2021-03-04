
package algorithm

import (
	"fmt"

	"github.com/microsoft/hivedscheduler/pkg/api"
	apiv2 "github.com/microsoft/hivedscheduler/pkg/api/v2"
	"github.com/microsoft/hivedscheduler/pkg/common"
)

var group1, group2, group3, group4, group5, group6, group7, group8, group9, group10, group11, group12, group13, group14,
	group15, group16, group17, group18, group19, group20, group21, group22, group23, group24, group25, group26, group27,
	group28, group29, group30, group31, group32, group33, group34 = &api.AffinityGroupSpec{
	Name:    "group1",
	Members: []api.AffinityGroupMemberSpec{{PodNumber: 1, LeafCellNumber: 1}},
}, &api.AffinityGroupSpec{
	Name:    "group2",
	Members: []api.AffinityGroupMemberSpec{{PodNumber: 1, LeafCellNumber: 1}},
}, &api.AffinityGroupSpec{
	Name:    "group3",
	Members: []api.AffinityGroupMemberSpec{{PodNumber: 1, LeafCellNumber: 8}},
}, &api.AffinityGroupSpec{
	Name:    "group4",
	Members: []api.AffinityGroupMemberSpec{{PodNumber: 1, LeafCellNumber: 1}},
}, &api.AffinityGroupSpec{
	Name:    "group5",
	Members: []api.AffinityGroupMemberSpec{{PodNumber: 2, LeafCellNumber: 16}},
}, &api.AffinityGroupSpec{
	Name:    "group6",
	Members: []api.AffinityGroupMemberSpec{{PodNumber: 1, LeafCellNumber: 1}},
}, &api.AffinityGroupSpec{
	Name:    "group7",
	Members: []api.AffinityGroupMemberSpec{{PodNumber: 3, LeafCellNumber: 8}},
}, &api.AffinityGroupSpec{
	Name:    "group8",
	Members: []api.AffinityGroupMemberSpec{{PodNumber: 1, LeafCellNumber: 8}},
}, &api.AffinityGroupSpec{
	Name:    "group9",
	Members: []api.AffinityGroupMemberSpec{{PodNumber: 1, LeafCellNumber: 7}, {PodNumber: 1, LeafCellNumber: 5}},
}, &api.AffinityGroupSpec{
	Name:    "group10",
	Members: []api.AffinityGroupMemberSpec{{PodNumber: 1, LeafCellNumber: 1}},
}, &api.AffinityGroupSpec{
	Name:    "group11",
	Members: []api.AffinityGroupMemberSpec{{PodNumber: 2, LeafCellNumber: 16}},
}, &api.AffinityGroupSpec{
	Name:    "group12",
	Members: []api.AffinityGroupMemberSpec{{PodNumber: 2, LeafCellNumber: 16}},
}, &api.AffinityGroupSpec{
	Name:    "group13",
	Members: []api.AffinityGroupMemberSpec{{PodNumber: 2, LeafCellNumber: 16}},
}, &api.AffinityGroupSpec{
	Name:    "group14",
	Members: []api.AffinityGroupMemberSpec{{PodNumber: 2, LeafCellNumber: 16}},
}, &api.AffinityGroupSpec{
	Name:    "group15",
	Members: []api.AffinityGroupMemberSpec{{PodNumber: 1, LeafCellNumber: 2}},
}, &api.AffinityGroupSpec{
	Name:    "group16",
	Members: []api.AffinityGroupMemberSpec{{PodNumber: 1, LeafCellNumber: 2}},
}, &api.AffinityGroupSpec{
	Name:    "group17",
	Members: []api.AffinityGroupMemberSpec{{PodNumber: 1, LeafCellNumber: 2}},
}, &api.AffinityGroupSpec{
	Name:    "group18",
	Members: []api.AffinityGroupMemberSpec{{PodNumber: 2, LeafCellNumber: 16}},
}, &api.AffinityGroupSpec{
	Name:    "group19",
	Members: []api.AffinityGroupMemberSpec{{PodNumber: 2, LeafCellNumber: 16}},
}, &api.AffinityGroupSpec{
	Name:    "group20",
	Members: []api.AffinityGroupMemberSpec{{PodNumber: 1, LeafCellNumber: 16}},
}, &api.AffinityGroupSpec{
	Name:    "group21",
	Members: []api.AffinityGroupMemberSpec{{PodNumber: 1, LeafCellNumber: 16}},
}, &api.AffinityGroupSpec{
	Name:    "group22",
	Members: []api.AffinityGroupMemberSpec{{PodNumber: 1, LeafCellNumber: 16}},
}, &api.AffinityGroupSpec{
	Name:    "group23",
	Members: []api.AffinityGroupMemberSpec{{PodNumber: 1, LeafCellNumber: 16}},
}, &api.AffinityGroupSpec{
	Name:    "group24",
	Members: []api.AffinityGroupMemberSpec{{PodNumber: 2, LeafCellNumber: 16}},
}, &api.AffinityGroupSpec{
	Name:    "group25",
	Members: []api.AffinityGroupMemberSpec{{PodNumber: 1, LeafCellNumber: 16}},
}, &api.AffinityGroupSpec{
	Name:    "group26",
	Members: []api.AffinityGroupMemberSpec{{PodNumber: 2, LeafCellNumber: 16}},
}, &api.AffinityGroupSpec{
	Name:    "group27",
	Members: []api.AffinityGroupMemberSpec{{PodNumber: 2, LeafCellNumber: 16}},
}, &api.AffinityGroupSpec{
	Name:    "group28",
	Members: []api.AffinityGroupMemberSpec{{PodNumber: 1, LeafCellNumber: 16}},
}, &api.AffinityGroupSpec{
	Name:    "group29",
	Members: []api.AffinityGroupMemberSpec{{PodNumber: 4, LeafCellNumber: 16}},
}, &api.AffinityGroupSpec{
	Name:    "group30",
	Members: []api.AffinityGroupMemberSpec{{PodNumber: 1, LeafCellNumber: 16}},
}, &api.AffinityGroupSpec{
	Name:    "group31",
	Members: []api.AffinityGroupMemberSpec{{PodNumber: 1, LeafCellNumber: 16}},
}, &api.AffinityGroupSpec{
	Name:    "group32",
	Members: []api.AffinityGroupMemberSpec{{PodNumber: 1, LeafCellNumber: 16}},
}, &api.AffinityGroupSpec{
	Name:    "group33",
	Members: []api.AffinityGroupMemberSpec{{PodNumber: 1, LeafCellNumber: 16}},
}, &api.AffinityGroupSpec{
	Name:    "group34",
	Members: []api.AffinityGroupMemberSpec{{PodNumber: 1, LeafCellNumber: 16}},
}

var pss = map[string]api.PodSchedulingSpec{
	"pod1": {
		VirtualCluster:       "VC1",
		Priority:             0,
		LazyPreemptionEnable: true,
		PinnedCellId:         "",
		LeafCellType:         "DGX2-V100",
		LeafCellNumber:       1,
		AffinityGroup:        group1,
	}, "pod2": { // buddy of pod1
		VirtualCluster:       "VC1",
		Priority:             1,
		LazyPreemptionEnable: true,
		PinnedCellId:         "",
		LeafCellType:         "DGX2-V100",
		LeafCellNumber:       1,
		AffinityGroup:        group2,
	}, "pod3": { // non-buddy of pod 1 & 2 (avoidance of preemption)
		VirtualCluster:       "VC1",
		Priority:             2,
		LazyPreemptionEnable: true,
		PinnedCellId:         "",
		LeafCellType:         "DGX2-V100",
		LeafCellNumber:       8,
		AffinityGroup:        group3,
	}, "pod4": { // opportunistic pod (will stay away from the guaranteed pods)
		VirtualCluster:       "VC1",
		Priority:             -1,
		LazyPreemptionEnable: true,
		PinnedCellId:         "",
		LeafCellType:         "DGX2-V100",
		LeafCellNumber:       1,
		AffinityGroup:        group4,
	}, "pod5": { // use pinned cell
		VirtualCluster:       "VC1",
		Priority:             1,
		LazyPreemptionEnable: true,
		PinnedCellId:         "VC1-YQW-DGX2",
		LeafCellType:         "DGX2-V100",
		LeafCellNumber:       16,
		AffinityGroup:        group5,
	}, "pod6": { // use pinned cell
		VirtualCluster:       "VC1",
		Priority:             1,
		LazyPreemptionEnable: true,
		PinnedCellId:         "VC1-YQW-DGX2",
		LeafCellType:         "DGX2-V100",
		LeafCellNumber:       16,
		AffinityGroup:        group5,
	}, "pod7": { // insufficient VC cells; should return PodWaitInfo
		VirtualCluster:       "VC2",
		Priority:             1,
		LazyPreemptionEnable: true,
		PinnedCellId:         "",
		LeafCellType:         "DGX1-P100",
		LeafCellNumber:       8,
		AffinityGroup:        group7,
	}, "pod8": { // any leaf cell type; heterogeneous affinity group
		VirtualCluster:       "VC2",
		Priority:             1,
		LazyPreemptionEnable: true,
		PinnedCellId:         "",
		LeafCellType:         "",
		LeafCellNumber:       7,
		AffinityGroup:        group9,
	}, "pod9": { // any leaf cell type; heterogeneous affinity group
		VirtualCluster:       "VC2",
		Priority:             1,
		LazyPreemptionEnable: true,
		PinnedCellId:         "",
		LeafCellType:         "",
		LeafCellNumber:       5,
		AffinityGroup:        group9,
	}, "pod10": { // use a leaf cell type that the VC does not have; should User Error Panic
		VirtualCluster:       "VC2",
		Priority:             1,
		LazyPreemptionEnable: true,
		PinnedCellId:         "",
		LeafCellType:         "DGX2-V100",
		LeafCellNumber:       1,
		AffinityGroup:        group6,
	}, "pod11": { // invalid affinity group configuration
		VirtualCluster:       "VC2",
		Priority:             1,
		LazyPreemptionEnable: true,
		PinnedCellId:         "",
		LeafCellType:         "DGX1-P100",
		LeafCellNumber:       2,
		AffinityGroup:        group8,
	}, "pod12": { // invalid affinity group configuration
		VirtualCluster:       "VC2",
		Priority:             1,
		LazyPreemptionEnable: true,
		PinnedCellId:         "",
		LeafCellType:         "DGX1-P100",
		LeafCellNumber:       2,
		AffinityGroup:        group8,
	}, "pod13": { // invalid VC
		VirtualCluster:       "surprise!",
		Priority:             1,
		LazyPreemptionEnable: true,
		PinnedCellId:         "",
		LeafCellType:         "DGX1-P100",
		LeafCellNumber:       1,
		AffinityGroup:        group10,
	}, "pod14": { // invalid pinned cell
		VirtualCluster:       "VC2",
		Priority:             1,
		LazyPreemptionEnable: true,
		PinnedCellId:         "surprise!",
		LeafCellType:         "DGX1-P100",
		LeafCellNumber:       1,
		AffinityGroup:        group10,
	}, "pod15": { // invalid priority
		VirtualCluster:       "VC2",
		Priority:             1001,
		LazyPreemptionEnable: true,
		PinnedCellId:         "",
		LeafCellType:         "DGX1-P100",
		LeafCellNumber:       1,
		AffinityGroup:        group10,
	}, "pod16": { // trigger preemption
		VirtualCluster:       "VC1",
		Priority:             2,
		LazyPreemptionEnable: true,
		PinnedCellId:         "VC1-YQW-DGX2",
		LeafCellType:         "DGX2-V100",
		LeafCellNumber:       16,
		AffinityGroup:        group11,
	}, "pod17": { // trigger preemption
		VirtualCluster:       "VC1",
		Priority:             2,
		LazyPreemptionEnable: true,
		PinnedCellId:         "VC1-YQW-DGX2",
		LeafCellType:         "DGX2-V100",
		LeafCellNumber:       16,
		AffinityGroup:        group11,
	}, "pod18": { // used for test splitting physical cell hierarchies in reconfiguration
		VirtualCluster:       "VC1",
		Priority:             1,
		LazyPreemptionEnable: true,
		PinnedCellId:         "",
		LeafCellType:         "DGX2-V100",
		LeafCellNumber:       16,
		AffinityGroup:        group12,
	}, "pod19": { // used for test splitting physical cell hierarchies in reconfiguration
		VirtualCluster:       "VC1",
		Priority:             1,
		LazyPreemptionEnable: true,
		PinnedCellId:         "",
		LeafCellType:         "DGX2-V100",
		LeafCellNumber:       16,
		AffinityGroup:        group12,
	}, "pod20": { // guaranteed pod in splitting physical cell hierarchies
		VirtualCluster:       "VC1",
		Priority:             1,
		LazyPreemptionEnable: true,
		PinnedCellId:         "",
		LeafCellType:         "DGX2-V100",
		LeafCellNumber:       16,
		AffinityGroup:        group13,
	}, "pod21": { // guaranteed pod in splitting physical cell hierarchies
		VirtualCluster:       "VC1",
		Priority:             1,
		LazyPreemptionEnable: true,
		PinnedCellId:         "",
		LeafCellType:         "DGX2-V100",
		LeafCellNumber:       16,
		AffinityGroup:        group13,
	}, "pod22": { // opportunistic pod in splitting physical cell hierarchies
		VirtualCluster:       "VC1",
		Priority:             -1,
		LazyPreemptionEnable: true,
		PinnedCellId:         "",
		LeafCellType:         "DGX2-V100",
		LeafCellNumber:       16,
		AffinityGroup:        group14,
	}, "pod23": { // opportunistic pod in splitting physical cell hierarchies
		VirtualCluster:       "VC1",
		Priority:             -1,
		LazyPreemptionEnable: true,
		PinnedCellId:         "",
		LeafCellType:         "DGX2-V100",
		LeafCellNumber:       16,
		AffinityGroup:        group14,
	}, "pod24": { // used for triggering intra-VC preemption
		VirtualCluster:       "VC2",
		Priority:             0,
		LazyPreemptionEnable: true,
		PinnedCellId:         "",
		LeafCellType:         "CT1",
		LeafCellNumber:       2,
		AffinityGroup:        group15,
	}, "pod25": { // trigger intra-VC preemption
		VirtualCluster:       "VC2",
		Priority:             1,
		LazyPreemptionEnable: false,
		PinnedCellId:         "",
		LeafCellType:         "CT1",
		LeafCellNumber:       2,
		AffinityGroup:        group16,
	}, "pod26": { // will preempt pod25 immediately (as lazy preemption is not enabled)
		VirtualCluster:       "VC2",
		Priority:             2,
		LazyPreemptionEnable: false,
		PinnedCellId:         "",
		LeafCellType:         "CT1",
		LeafCellNumber:       2,
		AffinityGroup:        group17,
	}, "pod27": { // will be rejected because one of the pod in this group is allocated a non-suggested node
		VirtualCluster:       "VC1",
		Priority:             1,
		LazyPreemptionEnable: false,
		PinnedCellId:         "VC1-YQW-DGX2",
		LeafCellType:         "DGX2-V100",
		LeafCellNumber:       16,
		AffinityGroup:        group18,
	}, "pod28": { // used for stateful preemption test
		VirtualCluster:       "VC1",
		Priority:             1,
		LazyPreemptionEnable: false,
		PinnedCellId:         "VC1-YQW-DGX2",
		LeafCellType:         "DGX2-V100",
		LeafCellNumber:       16,
		AffinityGroup:        group19,
	}, "pod29": { // will try to preempt pod28
		VirtualCluster:       "VC1",
		Priority:             2,
		LazyPreemptionEnable: true,
		PinnedCellId:         "VC1-YQW-DGX2",
		LeafCellType:         "DGX2-V100",
		LeafCellNumber:       16,
		AffinityGroup:        group20,
	}, "pod30": { // cannot get scheduled because pod28's still holding the resource
		VirtualCluster:       "VC1",
		Priority:             1,
		LazyPreemptionEnable: true,
		PinnedCellId:         "VC1-YQW-DGX2",
		LeafCellType:         "DGX2-V100",
		LeafCellNumber:       16,
		AffinityGroup:        group21,
	}, "pod31": { // will try to preempt pod28, and will be scheduled to a different node from pod29
		VirtualCluster:       "VC1",
		Priority:             2,
		LazyPreemptionEnable: true,
		PinnedCellId:         "VC1-YQW-DGX2",
		LeafCellType:         "DGX2-V100",
		LeafCellNumber:       16,
		AffinityGroup:        group22,
	}, "pod32": { // cannot get scheduled because VC1-YQW-DGX2 has been used up by pod29 and pod31
		VirtualCluster:       "VC1",
		Priority:             2,
		LazyPreemptionEnable: true,
		PinnedCellId:         "VC1-YQW-DGX2",
		LeafCellType:         "DGX2-V100",
		LeafCellNumber:       16,
		AffinityGroup:        group23,
	}, "pod33": { // will cancel pod29 and pod31's preemption, and continue to preempt pod28
		VirtualCluster:       "VC1",
		Priority:             3,
		LazyPreemptionEnable: true,
		PinnedCellId:         "VC1-YQW-DGX2",
		LeafCellType:         "DGX2-V100",
		LeafCellNumber:       16,
		AffinityGroup:        group24,
	}, "pod34": { // will cancel pod33's preemption, and get scheduled immediately (because pod28 has been deleted)
		VirtualCluster:       "VC1",
		Priority:             4,
		LazyPreemptionEnable: false,
		PinnedCellId:         "VC1-YQW-DGX2",
		LeafCellType:         "DGX2-V100",
		LeafCellNumber:       16,
		AffinityGroup:        group25,
	}, "pod35": { // will preempt pod34, and will be deleted before the preemption is done (so the preemption will be canceled)
		VirtualCluster:       "VC1",
		Priority:             5,
		LazyPreemptionEnable: true,
		PinnedCellId:         "VC1-YQW-DGX2",
		LeafCellType:         "DGX2-V100",
		LeafCellNumber:       16,
		AffinityGroup:        group26,
	}, "pod36": { // will iterate the leaf cell types until find a placement within suggested nodes
		VirtualCluster:       "VC1",
		Priority:             -1,
		LazyPreemptionEnable: true,
		PinnedCellId:         "",
		LeafCellType:         "",
		LeafCellNumber:       1,
		AffinityGroup:        group1,
	}, "pod37": { // used for test aware of suggested nodes in VC
		VirtualCluster:       "VC1",
		Priority:             1,
		LazyPreemptionEnable: true,
		PinnedCellId:         "VC1-YQW-DGX2",
		LeafCellType:         "DGX2-V100",
		LeafCellNumber:       1,
		AffinityGroup:        group1,
	}, "pod38": { // used for test aware of suggested nodes in VC
		VirtualCluster:       "VC1",
		Priority:             1,
		LazyPreemptionEnable: true,
		PinnedCellId:         "VC1-YQW-DGX2",
		LeafCellType:         "DGX2-V100",
		LeafCellNumber:       1,
		AffinityGroup:        group2,
	}, "pod39": { // used for triggering backtrack cell search
		VirtualCluster:       "VC1",
		Priority:             1,
		LazyPreemptionEnable: true,
		PinnedCellId:         "",
		LeafCellType:         "DGX2-V100",
		LeafCellNumber:       16,
		AffinityGroup:        group27,
	}, "pod40": { // backtrack cell search
		VirtualCluster:       "VC1",
		Priority:             1,
		LazyPreemptionEnable: true,
		PinnedCellId:         "",
		LeafCellType:         "DGX2-V100",
		LeafCellNumber:       16,
		AffinityGroup:        group28,
	}, "pod41": { // revert lazy preemption in backtrack cell search
		VirtualCluster:       "VC1",
		Priority:             2,
		LazyPreemptionEnable: true,
		PinnedCellId:         "",
		LeafCellType:         "DGX2-V100",
		LeafCellNumber:       16,
		AffinityGroup:        group29,
	}, "pod42": { // doomed bad cell test
		VirtualCluster:       "VC1",
		Priority:             0,
		LazyPreemptionEnable: true,
		PinnedCellId:         "",
		LeafCellType:         "DGX2-V100",
		LeafCellNumber:       16,
		AffinityGroup:        group30,
	}, "pod43": { // doomed bad cell test
		VirtualCluster:       "VC2",
		Priority:             0,
		LazyPreemptionEnable: true,
		PinnedCellId:         "",
		LeafCellType:         "DGX2-V100",
		LeafCellNumber:       16,
		AffinityGroup:        group31,
	}, "pod44": { // safe relaxed buddy allocate for bad node test
		VirtualCluster:       "VC1",
		Priority:             0,
		LazyPreemptionEnable: true,
		PinnedCellId:         "",
		LeafCellType:         "DGX2-V100",
		LeafCellNumber:       16,
		AffinityGroup:        group32,
	}, "pod45": { // safe relaxed buddy allocate for bad node test
		VirtualCluster:       "VC1",
		Priority:             0,
		LazyPreemptionEnable: true,
		PinnedCellId:         "",
		LeafCellType:         "DGX2-V100",
		LeafCellNumber:       16,
		AffinityGroup:        group33,
	}, "pod46": { // safe relaxed buddy allocate safety test
		VirtualCluster:       "VC1",
		Priority:             0,
		LazyPreemptionEnable: true,
		PinnedCellId:         "",
		LeafCellType:         "DGX2-V100",
		LeafCellNumber:       16,
		AffinityGroup:        group34,
	},
}

func Convert() {
	// common.InitAll()
	// var i map[string]

	new_pss := map[string]apiv2.PodSchedulingSpec{}
	for i := 1; i <= len(pss); i++ {
		podName := fmt.Sprintf("pod%v", i)
		var ps = pss[podName]
		new_ps := apiv2.PodSchedulingSpec{}
		new_ps.ConvertFromV1(&ps)
		new_pss[podName] = new_ps
	}
	s := common.ToYaml(new_pss)
	fmt.Println(s)
}