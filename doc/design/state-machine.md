This document presents the state machines of HiveD, and explains the life cycles of jobs and resources in HiveD.
We will describe the state machines of our scheduling unit, Affinity Group, and that of our resource unit, Cell, respectively.

## Affinity Group (AG) State Machine

An affinity group (AG) is a set of gang-scheduled pods. It is the basic scheduling unit in HiveD. The figure below shows the state machine of an affinity group.

Note that the AG state machine has interactions with the cell state machine (elaborated later). In our design, AGs influence each other only via cell: an event of an AG may trigger an event for one of its cells, and the influenced cell may trigger another event for another AG related to the same cell.
Therefore, the state machines of multiple AGs are effectively bridged by the state machines of their overlapping cells.

![AG](AG-state-machine.png)

### States

__Pending__: the AG is waiting to be scheduled.

__Preempting__: the AG is allocated resources but is waiting for the completion of preemptions of other AGs (only after that can it start to run).

__Allocated__: the AG is allocated resources and is running normally.

__Being preempted__: the AG is being preempted by other AGs (the preemption is still ongoing).

__Deleted__: the AG is fully deleted.

Common AG life cycles:

Pending -> Allocated -> Deleted (no preemption involved)

Pending -> Preempting -> Allocated -> Being preempted -> Deleted (preemption involved)

### Events

__e<sub>c</sub>__:

Condition: scheduler crashes and restarts.

Operation: none.

Note that only Pending, Allocated, and Deleted are persistent, thus they are the recovery points of HiveD. While other AG states (Preempting, Being preempted) are volatile, so they will transition to others after e<sub>c</sub>. Also note that Allocated state includes updating pod annotation, pod binding, and pod running. We assume once pod annotation has been updated, the pod binding and pod running are handled by K8s. 

__e<sub>0</sub>__:

Condition: all cells allocated to the AG are in Free or Pre-allocated state (we assume the cell allocation algorithm has ensured that this AG’s priority is higher than that every Pre-allocated cell, i.e., that of the Preempting AG).

Operation: set the cells to Used; set the Preempting AG on the Pre-allocated cells to Pending.

__e<sub>1</sub>__:

Condition: there is at least one cell allocated to the AG in Used or Preempting state (we assume the cell allocation algorithm has ensured that this AG’s priority is higher than that of every Used and Preempting cell).

Operation: set all the Used cells to Preempting state (e<sub>2</sub> in cell state machine); overwrite the Pre-allocated (e<sub>6</sub> in cell state machine) and Preempting cells (e<sub>3</sub> in cell state machine); set the Free cell to Pre-allocated (e<sub>5</sub> in cell state machine).

__e<sub>2</sub>__:

Condition: all the cells allocated to the AG are Pre-allocated.

Operation: set all the cells to Used (e<sub>8</sub> in cell state machine).

__e<sub>3</sub>__:

Condition: all pods deleted.

Operation: set all the cells allocated to the AG in Free state (e<sub>1</sub> in cell state machine).

__e<sub>4</sub>__:

Condition: all pods deleted.

Operation: none.

__e<sub>5</sub>__:

Condition: a Preempting or Pre-allocated cell in this AG is being overwritten (e<sub>3</sub> or e<sub>6</sub> in cell state machine).

Operation: set all the other Preempting cells to Used (e<sub>4</sub> in cell state machine), Pre-allocated cells to Free (e<sub>7</sub> in cell state machine).

__e<sub>6</sub>__:

Condition: a cell from Used to Preempting (e<sub>2</sub> in cell state machine)

Operation: none.

__e<sub>7</sub>__:

Condition: AG deleted.

Operation: set the Reserved cells to Free (e<sub>7</sub> in cell state machine), set the Preempting cells to Used (e<sub>4</sub> in cell state machine).

## Cell State Machine

Cell is the resource unit in HiveD. The figure below shows the state machine of cell. Note that here cells are _lowest-level physical cells_ (we record states only in these cells).

![AG](cell-state-machine.png)

### States

__Free__: the cell is not allocated to any AG.

__Used__: the cell is allocated to an AG, and the AG is running on this cell.

__Reserved__: the cell is allocated to an AG, but there is no AG running on this cell (e.g., the AG is Preempting).

__Reserving__: the cell is allocated to an AG, but another AG is still running on the cell (e.g., the first AG is Preempting the running AG).

Note that the reservation of cells (Reserved and Reserving states) is not necessarily designed for preemptions (i.e., reserving resources for the Preempting AGs). In the future it is possible that we extend this mechanism to support other reservation mechanisms.

Common cell life cycles:

Free -> Used -> Free (no preemption involved)

Free -> Used -> Reserving -> Reserved -> Used -> Free (preemption involved)

### Events

Note: “cell view” in the below operations represents the in-memory data structures for scheduling, e.g., free cell list, cell bindings, cell priorities. Allocating / releasing a cell in the cell view will modify the free cell list (and split / merge the cell), create / destroy cell bindings, and set / reset the cell priority. These changes are immediately visible to the subsequent scheduling cycles.

__e<sub>c</sub>__:

Condition: scheduler crashes and restarts.

Operation: none.

Note that all states are volatile; the Free and Used states are derived from the AG state machine.

__e<sub>0</sub>__:

Condition: triggered by AG from Pending to Allocated (e<sub>0</sub> in AG state machine).

Operation: allocate the cell to the AG in the cell view.

__e<sub>1</sub>__:

Condition: the Allocated AG is deleted (e<sub>3</sub> in AG state machine).

Operation: release the cell in the cell view.

__e<sub>2</sub>__:

Condition: triggered by AG from Pending to Preempting (e<sub>1</sub> in AG state machine).

Operation: set the Allocated AG on this cell to Being preempted state (e<sub>6</sub> in AG state machine); release the cell from the cell view, and then allocate it to the preemptor AG in the cell view.

__e<sub>3</sub>__:

Condition: triggered by AG from Pending to Preempting (e<sub>1</sub> in AG state machine).

Operation: set the original preemptor AG to Pending state (e<sub>5</sub> in AG state machine); release the cell from the cell view, and then allocate it to the new preemptor.

__e<sub>4</sub>__:

Condition: triggered by AG from Preempting to Pending (e<sub>5</sub> in AG state machine) or from Preempting to Deleted (e7 in AG state machine).

Operation: release the cell from the cell view, and then allocate it to the running AG (i.e., the preemption victim).

__e<sub>5</sub>__:

Condition: triggered by AG from Pending to Preempting (e<sub>1</sub> in AG state machine).

Operation: allocate the cell to the preemptor in the cell view.

__e<sub>6</sub>__:

Condition: triggered by AG from Pending to Preempting (e<sub>1</sub> in AG state machine).

Operation: release the cell from the cell view, and then allocate it to the new preemptor, and set the original preemptor AG from Preempting to Pending (e<sub>5</sub> in AG state machine).

__e<sub>7</sub>__:

Condition: triggered by AG from Preempting to Pending (e<sub>5</sub> in AG state machine) or from Preempting to Completed (e<sub>7</sub> in AG state machine).

Operation: release the cell from the cell view.

__e<sub>8</sub>__:

Condition: triggered by AG from Preempting to Allocated (e<sub>2</sub> in AG state machine).

Operation: none.

__e<sub>9</sub>__:

Condition: the running pod is preempted.

Operation: none.
