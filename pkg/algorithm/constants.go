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
	"math"

	"github.com/microsoft/hivedscheduler/pkg/api"
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

	// No affinity group is using, reserving, or has reserved the cell.
	// A Free cell's priority must be freePriority.
	// Note that a Free cell may also have binding (e.g., when it is a doomed bad cell),
	// and we should not use such Free cell for cell binding.
	cellFree CellState = "Free"
	// An affinity group is using this cell, and no other group is reserving or has reserved it.
	// A Used cell's priority is that of the group using the cell.
	cellUsed CellState = "Used"
	// An affinity group is using this cell, and another group is reserving it.
	// A Reserving cell's priority is that of the group reserving it. This means the scheduling algorithm
	// will respect the reserving group, i.e., a group with a non-higher priority cannot get this cell.
	cellReserving CellState = "Reserving"
	// No affinity group is using this cell, and a group has reserved it.
	// A Reserved cell's priority is that of the group that reserved it. This means the scheduling algorithm
	// will respect the group that reserved the cell, i.e., a group with a non-higher priority cannot get this cell.
	cellReserved CellState = "Reserved"

	// internal pod group states

	// The pod group has been allocated cells.
	// All cells in the group must be in Used state.
	podGroupAllocated PodGroupState = "Allocated"
	// The pod group is preempting other groups to get free resource.
	// Cells in the group must be in either Reserving or Reserved states.
	podGroupPreempting PodGroupState = "Preempting"
	// The pod group is being preempted by some other groups.
	// Cells in the group must be in either Used or Reserving states.
	podGroupBeingPreempted PodGroupState = "BeingPreempted"
)
