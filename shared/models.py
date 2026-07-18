from enum import Enum
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, field_validator
from datetime import datetime


class NodeState(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class WorkflowState(str, Enum):
    SUBMITTED = "SUBMITTED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class Node(BaseModel):
    id: str
    handler: str
    dependencies: List[str] = []
    config: Optional[Dict[str, Any]] = None


class DAG(BaseModel):
    nodes: List[Node]

    @field_validator("nodes")
    @classmethod
    def unique_ids(cls, v):
        if len({n.id for n in v}) != len(v):
            raise ValueError("Duplicate node IDs")
        return v


class WorkflowDef(BaseModel):
    name: str
    dag: DAG


class TriggerRequest(BaseModel):
    """Optional per-node config overrides supplied at trigger time,
    keyed by node id: {"fetch": {"url": "https://other.com"}}"""
    node_params: Dict[str, Dict[str, Any]] = {}


class NodeStatus(BaseModel):
    node_id: str
    state: NodeState
    output: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    worker_id: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    retry_count: int = 0
    max_retries: int = 3


class WorkflowStatus(BaseModel):
    execution_id: str
    workflow_name: str
    state: WorkflowState
    nodes: Dict[str, NodeStatus]
    created_at: datetime
    error: Optional[str] = None
