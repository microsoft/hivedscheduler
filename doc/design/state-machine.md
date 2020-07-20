# HiveD State Machines

This document presents the state machines of HiveD, and explains the life cycles of jobs and resources in HiveD.
We will describe the state machines of our scheduling unit, Affinity Group, and that of our resource unit, Cell, respectively.

## Affinity Group (AG) State Machine

An affinity group (AG) is a set of gang-scheduled pods. It is the basic scheduling unit in HiveD. The figure below shows the state machine of an affinity group.

Note that the AG state machine has interactions with the cell state machine (elaborated later). In our design, AGs influence each other only via their overlapping cells: for example, an event sent to an AG may trigger an event sent to a cell, and that cell may further trigger another event sent to another AG which is currently associated with the cell.
Therefore, the state machines of multiple AGs are effectively bridged by the state machines of their overlapping cells.

<p style="text-align: center;">
  <img src="img/AG-state-machine.png" title="AG" alt="AG" width="70%"/>
</p>

### States

__`Pending`__: the AG is waiting to be scheduled.

__`Preempting`__: the AG has reserved cells but is waiting for the completion of preemptions of other AGs.

__`Allocated`__: the AG is fully allocated cells.

__`Being preempted`__: the AG is being preempted by other AGs via their overlapping cells (the preemption is still ongoing).

__`Deleted`__: the AG is fully deleted and all of its cells are released.

Note that only ``Pending``, `Allocated`, and `Deleted` are persistent, thus they are the recovery points of HiveD. While the other AG states (`Preempting`, `Being preempted`) are volatile, so they will transition to others after scheduler crash and restart (i.e., e<sub>c</sub> in the state machine).

Also note that `Allocated` state includes updating pod annotation (pod binding) and pod running. We assume once pod annotation has been updated (pod bound to a node), the pod running is handled by K8s. We hence only describe the cell allocation state in the state machine, and do not care about the pods' real running state.

### Common AG life cycles

__No preemption involved__:
`Pending` -> `Allocated` -> `Deleted`

__Preemption involved__:
`Pending` -> `Preempting` -> `Allocated` -> `Being preempted` -> `Deleted`

### Events

__e<sub>c</sub>__:

Condition: scheduler crashes and restarts.

Operation: none.

__e<sub>0</sub>__:

Condition: all cells that the cell allocation algorithm decides to allocate to the AG are in `Free` or `Reserved` states (we assume the cell allocation algorithm has ensured that for every `Reserved` cell this AG's priority is higher than that of the AG currently associated with the cell, e.g., a `Preempting` AG).

Operation:

For all cells allocated to this AG:

`Free` -> `Used` (by this AG) (e<sub>0</sub> in cell state machine);

`Reserved` (by another AG) -> `Used` (by this AG) (e<sub>8</sub> in cell state machine).

__e<sub>1</sub>__:

Condition: there is at least one cell among those the algorithm decides to allocate to this AG in `Used` or `Reserving` states (we assume the cell allocation algorithm has ensured that for every `Used` or `Reserving` cell this AG's priority is higher than that of the AG currently associated with the cell).

Operation:

For all cells currently associated with other AGs:

`Used` (by other AGs) -> `Reserving` (by this AG) (e<sub>2</sub> in cell state machine);

`Reserving`/`Reserved` (by other AGs) -> `Reserving`/`Reserved` (by this AG) (e<sub>3</sub>/e<sub>6</sub> in cell state machine);

For free cells:

`Free` -> `Reserved` (by this AG) (e<sub>5</sub> in cell state machine).

__e<sub>2</sub>__:

Condition: all the cells that the cell allocation algorithm decided to allocate to this AG are `Reserved`.

Operation: for all cells of this AG:

`Reserved` (by this AG) -> `Used` (by this AG) (e<sub>8</sub> in cell state machine).

__e<sub>3</sub>__:

Condition: all pods of this AG are deleted.

Operation: for all cells of this AG:

`Used` (by this AG) -> `Free` (e<sub>1</sub> in cell state machine).

__e<sub>4</sub>__:

Condition: all pods of this AG are deleted.

Operation:
all cells `Used` (by this AG) -> `Free` (e<sub>1</sub> in cell state machine).

__e<sub>5</sub>__:

Condition: a `Reserving` or `Reserved` cell in this AG is being overwritten by another AG (e<sub>3</sub> or e<sub>6</sub> in cell state machine).

Operation:

All the other `Reserving` cells (by this AG) -> `Used` (by the `Being preempted` AG currently associated with the cell) (e<sub>4</sub> in cell state machine);

All the other `Reserved` cells (by this AG) -> `Free` (e<sub>7</sub> in cell state machine).


__e<sub>6</sub>__:

Condition: a cell allocated to this AG from `Used` (by this AG) to `Reserving` (by another AG) (e<sub>2</sub> in cell state machine)

Operation: none.

__e<sub>7</sub>__:

Condition: all pods of this AG are deleted.

Operation:

All the `Reserving` cells (by this AG) -> `Used` (by the `Being preempted` AG currently associated with the cell) (e<sub>4</sub> in cell state machine).

All the `Reserved` cells (by this AG) -> `Free` (e<sub>7</sub> in cell state machine);

__e<sub>8</sub>__:

Condition: all pods of this AG are deleted.

Operation: none.

## Cell State Machine

Cell is the resource unit in HiveD. The figure below shows the state machine of cell. Note that here cells are _lowest-level physical cells_, e.g., single-device cells in typical configs (we record states only in these cells).

<p style="text-align: center;">
  <img src="img/cell-state-machine.png" title="cell" alt="cell" width="70%"/>
</p>

### States

__`Free`__: no AG is associated with this cell.

__`Used`__: only an `Allocated` or a `Being preempted` AG is associated with the cell.

__`Reserved`__: only a `Preempting` AG is associated with the cell.

__`Reserving`__: a `Preempting` and a `Being preempted` AG are associated with the cell.

Note that all states are volatile; the `Free` and `Used` states are derived from the AG state machine.

Also note that the reservation of cells (`Reserved` and `Reserving` states) is not necessarily designed for preemptions (i.e., reserving resources for the `Preempting` AGs), despite the state definitions involving preemptions above. In the future it is possible that we extend this mechanism to support other features that need reservation, such as reservation during waiting to achieve strict FIFO and fairness for larger AGs.

### Common cell life cycles

__No preemption involved__:
`Free` -> `Used` -> `Free`

__Preemption involved__:
`Free` -> `Used` -> `Reserving` -> `Reserved` -> `Used` -> `Free`

### Events

Note:
"Allocate/Reserve/Release a cell" in the below descriptions means modifying the in-memory data structures for scheduling, e.g., free cell list, cell bindings, cell priorities. Allocating/reserving or releasing a cell in the cell view will modify the free cell list, and split or merge the cell, create or destroy cell bindings, and set or reset the cell priority.
These changes are immediately visible to the cell allocation algorithm when scheduling subsequent AGs.

__e<sub>c</sub>__:

Condition: scheduler crashes and restarts.

Operation: none.

__e<sub>0</sub>__:

Condition: triggered by AG from `Pending` to `Allocated` (e<sub>0</sub> in AG state machine).

Operation: allocate the cell to the AG.

__e<sub>1</sub>__:

Condition: the `Allocated` AG on this is deleted (e<sub>3</sub> in AG state machine).

Operation: release the cell.

__e<sub>2</sub>__:

Condition: triggered by another AG from `Pending` to `Preempting` (i.e., that AG is preempting the `Allocated` AG currently associated with this cell) (e<sub>1</sub> in AG state machine).

Operation:

The `Allocated` AG on this cell -> `Being preempted` (e<sub>6</sub> in AG state machine);

Release the cell, and then reserve it for the `Preempting` AG.

__e<sub>3</sub>__:

Condition: triggered by another AG from `Pending` to `Preempting` (i.e., that AG cancels the preemption of the `Preempting` AG currently associated with this cell, and continues to preempt the `Being preempted` AG associated with this cell) (e<sub>1</sub> in AG state machine).

Operation:

The original `Preempting` AG on this cell -> `Pending` (e<sub>5</sub> in AG state machine);

Release the cell, and then reserve it for the new `Preempting` AG.

__e<sub>4</sub>__:

Condition: triggered by the `Preempting` AG currently associated with this cell to `Pending` (e<sub>5</sub> in AG state machine) or to `Deleted` (e<sub>7</sub> in AG state machine).

Operation: release the cell, and then allocate it to the `Being preempted` AG on this cell (i.e., the preemption victim).

__e<sub>5</sub>__:

Condition: triggered by AG from `Pending` to `Preempting` (e<sub>1</sub> in AG state machine).

Operation: reserve the cell for the `Preempting` AG.

__e<sub>6</sub>__:

Condition: triggered by another AG from `Pending` to `Preempting` (i.e., that AG cancels the preemption of the `Preempting` AG currently associated with this cell) (e<sub>1</sub> in AG state machine).

Operation:

The original `Preempting` AG on this cell -> `Pending` (e<sub>5</sub> in AG state machine).

Release the cell, and then reserve it for the new `Preempting` AG.

__e<sub>7</sub>__:

Condition: triggered by the `Preempting` AG currently associated with this cell to `Pending` (e<sub>5</sub> in AG state machine) or to `Deleted` (e<sub>7</sub> in AG state machine).

Operation: release the cell.

__e<sub>8</sub>__:

Condition: triggered by (i) there is currently a `Preempting` AG on this cell but another `Allocated` AG is now associated with the cell (e<sub>0</sub> in AG state machine); OR (ii) the `Preempting` AG currently associated with this cell transitions to `Allocated` (e<sub>2</sub> in AG state machine).

Operation:

For (i): the `Preempting` AG on this cell -> `Pending`  (e<sub>5</sub> in AG state machine); release the cell and then allocate it to the new `Allocated` AG.

For (ii): none.

__e<sub>9</sub>__:

Condition: the pod on this cell is deleted.

Operation: none.
