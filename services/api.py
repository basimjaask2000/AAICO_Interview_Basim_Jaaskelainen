import os
import sys
import uuid
import logging
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException
import uvicorn
sys.path.insert(0, '/app/shared')
from models import WorkflowDef, WorkflowState, TriggerRequest
from validation import DAGValidator
from storage_postgres import PostgresStorage
from kafka_utils import create_producer

logger = logging.getLogger(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

app = FastAPI()
producer = create_producer()
store = PostgresStorage(os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/workflow_db"))


@app.on_event("startup")
def on_startup() -> None:
    try:
        producer.send("heartbeat", {}).get(timeout=5)
        logger.info("Kafka OK")
    except Exception as e:
        logger.error(f"Kafka failed: {e}")


@app.post("/workflow")
async def submit(workflow: WorkflowDef) -> Dict[str, str]:
    v = DAGValidator(workflow.dag)
    ok, msg = v.validate()
    if not ok:
        raise HTTPException(status_code=400, detail=msg)

    eid = str(uuid.uuid4())
    nodes = [n.id for n in workflow.dag.nodes]
    store.create_workflow(eid, workflow.name, nodes)
    store.save_definition(eid, workflow.model_dump())
    logger.info(f"Submitted {eid}")

    return {
        "execution_id": eid,
        "status": "SUBMITTED",
        "message": f"Workflow submitted. POST /workflow/trigger/{eid} to start execution.",
    }


@app.post("/workflow/trigger/{execution_id}")
async def trigger(execution_id: str, body: Optional[TriggerRequest] = None) -> Dict[str, str]:
    s = store.get_workflow_status(execution_id)
    if not s:
        raise HTTPException(status_code=404, detail="Not found")

    node_params: Dict[str, Dict[str, Any]] = body.node_params if body else {}
    if node_params:
        wf_def = store.get_definition(execution_id)
        known = {n["id"] for n in wf_def["dag"]["nodes"]} if wf_def else set()
        unknown = set(node_params) - known
        if unknown:
            raise HTTPException(status_code=400, detail=f"Unknown node ids in node_params: {sorted(unknown)}")

    if not store.mark_triggered(execution_id):
        raise HTTPException(status_code=409, detail="Workflow already triggered")

    producer.send("workflow-events", {
        "event": "workflow.start",
        "execution_id": execution_id,
        "node_params": node_params,
    })
    logger.info(f"Triggered {execution_id}")

    return {
        "execution_id": execution_id,
        "status": "TRIGGERED",
        "message": f"Execution started. Check /workflows/{execution_id} for status.",
    }


@app.get("/workflows/{execution_id}")
async def status(execution_id: str):
    s = store.get_workflow_status(execution_id)
    if not s:
        raise HTTPException(status_code=404, detail="Not found")
    return s


@app.get("/workflows/{execution_id}/results")
async def results(execution_id: str) -> Dict[str, Any]:
    s = store.get_workflow_status(execution_id)
    if not s:
        raise HTTPException(status_code=404, detail="Not found")

    output: Dict[str, Any] = {}
    for nid, ns in s.nodes.items():
        if ns.output:
            output[nid] = ns.output

    return {
        "execution_id": execution_id,
        "workflow_name": s.workflow_name,
        "state": s.state,
        "result": output,
        "error": s.error,
    }


@app.get("/health")
async def health_check() -> Dict[str, bool]:
    return {"ok": True}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
