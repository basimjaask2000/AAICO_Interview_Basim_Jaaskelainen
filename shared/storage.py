import json
from datetime import datetime
from typing import Optional, Dict, Any
import redis
from shared.models import NodeState, WorkflowState, NodeStatus, WorkflowStatus


class Storage:
    def __init__(self, redis_url: str):
        self.r = redis.from_url(redis_url, decode_responses=True)

    def create_workflow(self, eid: str, name: str, node_ids: list):
        wf_key = f"wf:{eid}"
        self.r.hset(wf_key, mapping={
            "name": name,
            "state": WorkflowState.SUBMITTED.value,
            "created_at": datetime.now().isoformat(),
        })
        self.r.expire(wf_key, 86400)

        for nid in node_ids:
            nk = f"wf:{eid}:node:{nid}"
            self.r.hset(nk, mapping={"state": NodeState.PENDING.value})
            self.r.expire(nk, 86400)

    def set_node_state(self, eid: str, nid: str, state: NodeState,
                       output: Optional[Dict] = None, error: Optional[str] = None,
                       worker_id: Optional[str] = None, retry_count: int = 0):
        k = f"wf:{eid}:node:{nid}"
        d = {
            "state": state.value,
            "retry_count": str(retry_count),
            "updated_at": datetime.now().isoformat(),
        }
        if output is not None:
            d["output"] = json.dumps(output)
        if error:
            d["error"] = error
        if worker_id:
            d["worker_id"] = worker_id
        if state == NodeState.RUNNING:
            d["started_at"] = datetime.now().isoformat()
        if state in [NodeState.COMPLETED, NodeState.FAILED]:
            d["completed_at"] = datetime.now().isoformat()
        self.r.hset(k, mapping=d)
        self.r.expire(k, 86400)

    def get_node_state(self, eid: str, nid: str) -> Optional[NodeStatus]:
        k = f"wf:{eid}:node:{nid}"
        d = self.r.hgetall(k)
        if not d:
            return None

        out = json.loads(d["output"]) if "output" in d else None
        started = datetime.fromisoformat(d["started_at"]) if "started_at" in d else None
        completed = datetime.fromisoformat(d["completed_at"]) if "completed_at" in d else None

        return NodeStatus(
            node_id=nid,
            state=NodeState(d.get("state", NodeState.PENDING.value)),
            output=out,
            error=d.get("error"),
            worker_id=d.get("worker_id"),
            started_at=started,
            completed_at=completed,
            retry_count=int(d.get("retry_count", 0)),
        )

    def get_workflow_nodes(self, eid: str) -> Dict[str, NodeStatus]:
        pattern = f"wf:{eid}:node:*"
        ks = self.r.keys(pattern)
        nodes = {}
        for k in ks:
            nid = k.split(":")[-1]
            st = self.get_node_state(eid, nid)
            if st:
                nodes[nid] = st
        return nodes

    def set_workflow_state(self, eid: str, state: WorkflowState,
                          error: Optional[str] = None):
        k = f"wf:{eid}"
        d = {"state": state.value}
        if error:
            d["error"] = error
        self.r.hset(k, mapping=d)
        self.r.expire(k, 86400)

    def get_workflow_status(self, eid: str) -> Optional[WorkflowStatus]:
        k = f"wf:{eid}"
        raw = self.r.hgetall(k)
        if not raw:
            return None

        d = {}
        for key, val in raw.items():
            d[key if isinstance(key, str) else key.decode()] = \
              val if isinstance(val, str) else val.decode()

        nodes = self.get_workflow_nodes(eid)
        created = d.get("created_at") or datetime.now().isoformat()
        return WorkflowStatus(
            execution_id=eid,
            workflow_name=d.get("name", ""),
            state=WorkflowState(d.get("state", WorkflowState.SUBMITTED.value)),
            nodes=nodes,
            created_at=datetime.fromisoformat(created),
            error=d.get("error"),
        )

    def get_node_output(self, eid: str, nid: str) -> Optional[Dict]:
        st = self.get_node_state(eid, nid)
        return st.output if st else None

    def save_definition(self, eid: str, wf: Dict[str, Any], ttl: int = 86400) -> None:
        self.r.setex(f"wf:{eid}:definition", ttl, json.dumps(wf))

    def get_definition(self, eid: str) -> Optional[Dict[str, Any]]:
        data = self.r.get(f"wf:{eid}:definition")
        return json.loads(data) if data else None

    def mark_triggered(self, eid: str) -> bool:
        """Atomically flag the workflow as triggered. Returns False if it already was."""
        return bool(self.r.hsetnx(f"wf:{eid}", "triggered", "1"))
