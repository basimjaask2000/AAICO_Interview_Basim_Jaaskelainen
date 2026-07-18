import os
import sys
import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, Optional, Set

import redis
sys.path.insert(0, '/app/shared')
from models import WorkflowDef, WorkflowState, NodeState, Node
from validation import DAGValidator
from storage import Storage
from templates import resolve_templates
from kafka_utils import create_consumer, create_producer

logger = logging.getLogger(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

#I assumed that a task would take no longer than 5 minutes, and if it does, its considered failed.
TASK_TIMEOUT_SECONDS = 300
MAX_RETRIES = 3

# All orchestrator state lives in Redis so a restart can pick up where it left off
ACTIVE_KEY = "active_workflows"
RETRY_KEY = "retry_queue"

consumer = create_consumer(["workflow-events", "task-results"], "orchestrator-group")
producer = create_producer()
store = Storage(os.getenv("REDIS_URL", "redis://localhost:6379"))
r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"), decode_responses=True)


def get_workflow(eid: str) -> WorkflowDef:
    data = store.get_definition(eid)
    if not data:
        raise Exception(f"Workflow {eid} not found")
    return WorkflowDef(**data)


def get_workflow_state(eid: str) -> Optional[str]:
    return r.hget(f"wf:{eid}", "state")


def get_done_nodes(eid: str, wf: WorkflowDef) -> Set[str]:
    done = set()
    for n in wf.dag.nodes:
        st = store.get_node_state(eid, n.id)
        if st and st.state == NodeState.COMPLETED:
            done.add(n.id)
    return done


def publish_ready(eid: str, wf: WorkflowDef, v: DAGValidator, done: Set[str]) -> None:
    for node_id in v.get_executable_nodes(done):
        st = store.get_node_state(eid, node_id)
        if st and st.state in (NodeState.RUNNING, NodeState.COMPLETED, NodeState.FAILED):
            continue
        if st and st.state == NodeState.PENDING and st.retry_count > 0:
            continue  # waiting out its backoff in the retry queue
        n = next(x for x in wf.dag.nodes if x.id == node_id)
        publish_task(eid, wf, n, retry_count=st.retry_count if st else 0)


def start_workflow(eid: str, node_params: Optional[Dict[str, Dict[str, Any]]] = None) -> None:
    try:
        wf = get_workflow(eid)

        # Merge trigger-time params into node configs before anything runs
        if node_params:
            for n in wf.dag.nodes:
                if n.id in node_params:
                    n.config = {**(n.config or {}), **node_params[n.id]}
            store.save_definition(eid, wf.model_dump())

        v = DAGValidator(wf.dag)
        ok, err = v.validate()
        if not ok:
            store.set_workflow_state(eid, WorkflowState.FAILED, err)
            logger.error(f"Validation failed: {err}")
            return

        store.set_workflow_state(eid, WorkflowState.RUNNING)
        r.sadd(ACTIVE_KEY, eid)

        # Start initial nodes (no deps); publish_ready skips anything already
        # in flight so a redelivered start event doesn't double-publish
        publish_ready(eid, wf, v, get_done_nodes(eid, wf))
        logger.info(f"Started {eid}")

    except Exception as e:
        logger.error(f"Start failed {eid}: {e}")
        store.set_workflow_state(eid, WorkflowState.FAILED, str(e))
        r.srem(ACTIVE_KEY, eid)


def check_timeouts() -> None:
    for eid in r.smembers(ACTIVE_KEY):
        try:
            if get_workflow_state(eid) != WorkflowState.RUNNING.value:
                r.srem(ACTIVE_KEY, eid)
                continue

            try:
                wf = get_workflow(eid)
            except Exception:
                r.srem(ACTIVE_KEY, eid)  # definition expired, nothing left to track
                continue

            for node in wf.dag.nodes:
                st = store.get_node_state(eid, node.id)
                if st and st.state == NodeState.RUNNING and st.started_at:
                    elapsed = (datetime.now() - st.started_at).total_seconds()
                    if elapsed > TASK_TIMEOUT_SECONDS:
                        logger.warning(f"Timeout {eid}:{node.id} (ran {elapsed}s)")
                        producer.send("task-results", {
                            "event": "task.completed",
                            "execution_id": eid,
                            "node_id": node.id,
                            "status": "FAILED",
                            "error": f"Task timeout after {TASK_TIMEOUT_SECONDS}s",
                            "retry_count": st.retry_count,
                        })

            # Reconcile: if a completion was processed but the orchestrator died
            # before scheduling successors, this sweep picks the workflow back up
            v = DAGValidator(wf.dag)
            done = get_done_nodes(eid, wf)
            if done == {n.id for n in wf.dag.nodes}:
                store.set_workflow_state(eid, WorkflowState.COMPLETED)
                r.srem(ACTIVE_KEY, eid)
                logger.info(f"Done {eid} (reconciled)")
            else:
                publish_ready(eid, wf, v, done)
        except Exception as e:
            logger.error(f"Timeout check error: {e}")


def publish_task(eid: str, wf: WorkflowDef, node: Node, retry_count: int = 0) -> None:
    # Include every dependency that produced any output (even falsy/empty),
    # so template resolution fails loudly only when output is truly absent
    deps: Dict[str, Any] = {}
    for dep_id in node.dependencies:
        out = store.get_node_output(eid, dep_id)
        if out is not None:
            deps[dep_id] = out

    try:
        cfg = resolve_templates(node.config, deps) if node.config else node.config
    except ValueError as e:
        # Unresolvable template is deterministic — fail the node instead of
        # crashing the event loop and leaving the workflow stuck RUNNING
        err = f"Template resolution failed for node '{node.id}': {e}"
        logger.error(f"{eid}: {err}")
        store.set_node_state(eid, node.id, NodeState.FAILED, error=err, retry_count=retry_count)
        store.set_workflow_state(eid, WorkflowState.FAILED, err)
        r.srem(ACTIVE_KEY, eid)
        return

    store.set_node_state(eid, node.id, NodeState.RUNNING, retry_count=retry_count)
    msg = {
        "event": "task.execute",
        "execution_id": eid,
        "node_id": node.id,
        "handler": node.handler,
        "config": cfg,
        "idempotency_key": f"{eid}:{node.id}",
        "retry_count": retry_count,
    }
    producer.send("task-queue", msg)
    logger.info(f"Published {eid}:{node.id} (retry {retry_count})")


def handle_completion(evt: Dict[str, Any]) -> None:
    eid = evt["execution_id"]
    nid = evt["node_id"]
    status = evt["status"]
    retry_count = int(evt.get("retry_count", 0))

    st = store.get_node_state(eid, nid)
    if st and st.state == NodeState.COMPLETED:
        return  # duplicate delivery, or a timeout event that raced the real completion

    if status == "COMPLETED":
        store.set_node_state(eid, nid, NodeState.COMPLETED,
                             output=evt.get("output"), worker_id=evt.get("worker_id"))
    else:
        if retry_count < MAX_RETRIES:
            backoff = 2 ** retry_count
            logger.info(f"Retrying {eid}:{nid} (attempt {retry_count + 1}/{MAX_RETRIES}) after {backoff}s")
            member = json.dumps({"execution_id": eid, "node_id": nid})
            r.zadd(RETRY_KEY, {member: time.time() + backoff})
            store.set_node_state(eid, nid, NodeState.PENDING, retry_count=retry_count + 1)
        else:
            store.set_node_state(eid, nid, NodeState.FAILED, error=evt.get("error"), retry_count=retry_count)
            store.set_workflow_state(eid, WorkflowState.FAILED, f"{nid} failed after {MAX_RETRIES} retries: {evt.get('error')}")
            r.srem(ACTIVE_KEY, eid)
            logger.error(f"Failed {eid}:{nid} after {MAX_RETRIES} retries")
        return

    if get_workflow_state(eid) == WorkflowState.FAILED.value:
        return  # workflow already failed; record the output but don't schedule more work

    wf = get_workflow(eid)
    v = DAGValidator(wf.dag)
    done = get_done_nodes(eid, wf)

    if done == {n.id for n in wf.dag.nodes}:
        store.set_workflow_state(eid, WorkflowState.COMPLETED)
        r.srem(ACTIVE_KEY, eid)
        logger.info(f"Done {eid}")
        return

    publish_ready(eid, wf, v, done)


def process_due_retries() -> None:
    now = time.time()
    for member in r.zrangebyscore(RETRY_KEY, 0, now):
        if not r.zrem(RETRY_KEY, member):
            continue
        try:
            item = json.loads(member)
            eid, nid = item["execution_id"], item["node_id"]
            st = store.get_node_state(eid, nid)
            if not st or st.state != NodeState.PENDING:
                continue  # node completed or was rescheduled in the meantime
            wf = get_workflow(eid)
            n = next(x for x in wf.dag.nodes if x.id == nid)
            publish_task(eid, wf, n, retry_count=st.retry_count)
        except Exception as e:
            logger.error(f"Retry publish error: {e}")


def main() -> None:
    logger.info("Orchestrator running...")
    last_timeout_check = time.time()

    while True:
        try:
            for tp, msgs in consumer.poll(timeout_ms=1000).items():
                for msg in msgs:
                    evt = msg.value
                    if tp.topic == "workflow-events" and evt.get("event") == "workflow.start":
                        start_workflow(evt["execution_id"], evt.get("node_params"))
                    elif tp.topic == "task-results" and evt.get("event") == "task.completed":
                        handle_completion(evt)

            process_due_retries()

            now = time.time()
            if now - last_timeout_check > 30:
                check_timeouts()
                last_timeout_check = now
        except Exception as e:
            logger.error(f"Error: {e}")
            time.sleep(1)


if __name__ == "__main__":
    main()
