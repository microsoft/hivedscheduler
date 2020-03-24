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
	"github.com/microsoft/hivedscheduler/pkg/api"
	"math"
)

const (
	// internal cell priorities
	maxGuaranteedPriority = CellPriority(api.MaxGuaranteedPriority)
	minGuaranteedPriority = CellPriority(api.MinGuaranteedPriority)
	opportunisticPriority = CellPriority(api.OpportunisticPriority)
	freePriority          = opportunisticPriority - 1

	// lowest and highest levels in a cell chain
	lowestLevel  CellLevel = 1
	highestLevel CellLevel = math.MaxInt32

	// internal cell states

	// no affinity group is using / acquiring or has acquired the cell
	cellFree CellState = "Free"
	// an affinity group is using this cell, and no other group is acquiring it
	cellUsed CellState = "Used"
	// an affinity group is using this cell, and another group is acquiring it
	cellAcquiring CellState = "Acquiring"
	// no affinity group is using this cell, and a group has acquired it
	cellAcquired CellState = "Acquired"

	// internal affinity group states

	// the affinity group has been allocated cells and is allowed to run
	groupAllocated affinityGroupState = "Allocated"
	// the affinity group is waiting for the completion of preemption of other groups
	groupPreempting affinityGroupState = "Preempting"
	// the affinity group is being preempted by some other groups
	groupBeingPreempted affinityGroupState = "BeingPreempted"
)
