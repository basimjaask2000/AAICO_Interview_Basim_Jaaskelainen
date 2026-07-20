# Design Notes

## The Problem

Workflows have tasks with dependencies: B needs A's output, C and D can run in parallel. We need to execute them efficiently and survive crashes without losing progress.

## Architecture Decisions

### Kafka for task events

Events go to Kafka, not a polling queue. This avoids constant orchestrator checks and keeps events durable on disk. If the orchestrator crashes, everything is recoverable from Kafka.

### PostgreSQL for persistent workflow state

Workflow and node state are persisted to PostgreSQL (execution_id, name, state, timestamps, outputs, errors). This provides durable storage across restarts without data loss. Redis is still used for transient orchestrator state (active workflows, retry queues) that can be safely reconstructed from the database and Kafka event log.

PostgreSQL schema:
- `workflows`: execution metadata and workflow state
- `nodes`: per-node execution status, outputs, and timestamps  
- `workflow_definitions`: DAG definitions stored as JSONB


### Separate API, Orchestrator, Worker services

Split into three processes instead monolithic repo. Lets you scale workers independently (100 workers, 1 orchestrator) and isolate failure so a crashed worker doesn't bring down the scheduler.

### Event-driven, not polling

Workers publish completion events to Kafka. The orchestrator reacts to those events, not by polling every 100ms. This allows scaling to thousands of workflows without burning CPU.

### Submit / trigger split

`POST /workflow` just validates and stores the DAG (state SUBMITTED). Nothing runs until you hit `POST /workflow/trigger/:execution_id`, which publishes the start event. The trigger can pass `node_params` that get merged into node configs, so you can feed in runtime values without resubmitting the whole DAG. Double-trigger is guarded with a Redis HSETNX, the second caller gets a 409.

## Execution Flow

1. API validates the workflow, stores the definition and PENDING node states in Redis
2. Trigger publishes a "start" event; the orchestrator merges any node params and finds nodes with no dependencies, publishing them as tasks
3. Workers execute tasks, publish results back
4. Orchestrator marks nodes done, finds next ready nodes, resolves their `{{ templates }}` against parent outputs, publishes them
5. Repeat until all nodes complete

## Readiness

A node is ready when all its dependencies are COMPLETED and it hasn't started yet. On every completion event the orchestrator re-reads node states from Redis and asks the validator for executable nodes (`get_executable_nodes`). This is a full recompute instead of keeping in-degree counters, which sounds wasteful but is O(nodes * deps) on workflows small enough to fit in a JSON payload, so it doesn't matter. The upside is there's no in-memory scheduler state: a restarted orchestrator computes the same answer as one that never died.

## Fan-in

For A -> [B, C] -> D, D has to fire exactly once when both B and C finish. This works because a single orchestrator consumes completion events one at a time. Even if B and C finish in the same millisecond, one event lands first, and only the second one sees both parents COMPLETED and publishes D. On top of that, `publish_ready` skips nodes already RUNNING/COMPLETED/FAILED, so a redelivered event or the reconcile sweep can't double-publish.

Downside is one orchestrator process is the scheduling bottleneck. Scheduling is cheap (a few Redis reads plus a Kafka publish) so it doesn't matter at this scale. Going multi-orchestrator would mean partitioning by execution_id so each workflow is still handled serially.

## Failure handling

Task failures get retried 3 times with exponential backoff (1s, 2s, 4s) through a Redis sorted set scored by due time. After max retries the node and the workflow go FAILED.

A sweep runs every 30s and fails anything stuck RUNNING longer than 5 minutes (assuming no real task takes that long). Timeouts go through the same completion path as normal failures, so they get the same retry treatment.

Orchestrator crash: state is in Redis, events are in Kafka, so on restart unprocessed events just replay from the consumer group offset. The same sweep also reconciles workflows where a completion got processed but the successors never got scheduled.

Template resolution failures are deterministic, retrying won't help, so those fail the node immediately instead of crashing the scheduler loop.

## Idempotency

Workers can see the same task twice (Kafka redelivery, or a retry racing a slow first attempt). Tasks carry an idempotency key (`execution_id:node_id`) and the worker checks Redis for a cached successful result before running anything. Only successes get cached, a failed attempt has to actually re-run on retry. Same on the orchestrator side: duplicate completion events for an already-COMPLETED node are dropped.

## Trade-offs

- At-least-once delivery instead of exactly-once. Idempotent workers make duplicates harmless and it's way simpler than Kafka transactions.
- Full state recompute per event instead of an incremental in-memory scheduler. Slower on paper, but crash recovery comes for free.
- Kafka topics are auto-created, which is fine for docker-compose. Production would provision them with explicit partitions/replication.
- PostgreSQL uses dummy local credentials (postgres:postgres) suitable for development. Production would use proper credential management and connection pooling.
