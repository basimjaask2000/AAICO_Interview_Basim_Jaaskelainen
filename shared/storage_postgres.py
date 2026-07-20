import json
from datetime import datetime
from typing import Optional, Dict, Any
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from models import NodeState, WorkflowState, NodeStatus, WorkflowStatus


class PostgresStorage:
    def __init__(self, connection_string: str):
        self.conn_string = connection_string
        self._init_db()

    def _get_conn(self):
        return psycopg2.connect(self.conn_string)

    def _init_db(self):
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS workflows (
                    execution_id VARCHAR(36) PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    state VARCHAR(50) NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    error TEXT,
                    triggered BOOLEAN DEFAULT FALSE
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS nodes (
                    execution_id VARCHAR(36) NOT NULL,
                    node_id VARCHAR(255) NOT NULL,
                    state VARCHAR(50) NOT NULL,
                    output JSONB,
                    error TEXT,
                    worker_id VARCHAR(255),
                    retry_count INTEGER DEFAULT 0,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL,
                    PRIMARY KEY (execution_id, node_id),
                    FOREIGN KEY (execution_id) REFERENCES workflows(execution_id) ON DELETE CASCADE
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS workflow_definitions (
                    execution_id VARCHAR(36) PRIMARY KEY,
                    workflow_def JSONB NOT NULL,
                    FOREIGN KEY (execution_id) REFERENCES workflows(execution_id) ON DELETE CASCADE
                )
            """)

            conn.commit()
        except psycopg2.errors.UniqueViolation:
            conn.rollback()
        except psycopg2.Error:
            conn.rollback()
        finally:
            cur.close()
            conn.close()

    def create_workflow(self, eid: str, name: str, node_ids: list):
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                "INSERT INTO workflows (execution_id, name, state, created_at) VALUES (%s, %s, %s, %s)",
                (eid, name, WorkflowState.SUBMITTED.value, datetime.now())
            )

            for nid in node_ids:
                cur.execute(
                    """INSERT INTO nodes (execution_id, node_id, state, updated_at)
                       VALUES (%s, %s, %s, %s)""",
                    (eid, nid, NodeState.PENDING.value, datetime.now())
                )

            conn.commit()
        finally:
            cur.close()
            conn.close()

    def set_node_state(self, eid: str, nid: str, state: NodeState,
                       output: Optional[Dict] = None, error: Optional[str] = None,
                       worker_id: Optional[str] = None, retry_count: int = 0):
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            started_at = datetime.now() if state == NodeState.RUNNING else None
            completed_at = datetime.now() if state in [NodeState.COMPLETED, NodeState.FAILED] else None

            cur.execute("""
                UPDATE nodes
                SET state = %s,
                    output = %s,
                    error = %s,
                    worker_id = %s,
                    retry_count = %s,
                    started_at = COALESCE(started_at, %s),
                    completed_at = %s,
                    updated_at = %s
                WHERE execution_id = %s AND node_id = %s
            """, (
                state.value,
                Json(output) if output is not None else None,
                error,
                worker_id,
                retry_count,
                started_at,
                completed_at,
                datetime.now(),
                eid,
                nid
            ))

            conn.commit()
        finally:
            cur.close()
            conn.close()

    def get_node_state(self, eid: str, nid: str) -> Optional[NodeStatus]:
        conn = self._get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        try:
            cur.execute(
                """SELECT * FROM nodes WHERE execution_id = %s AND node_id = %s""",
                (eid, nid)
            )
            row = cur.fetchone()
            if not row:
                return None

            return NodeStatus(
                node_id=nid,
                state=NodeState(row['state']),
                output=row['output'],
                error=row['error'],
                worker_id=row['worker_id'],
                started_at=row['started_at'],
                completed_at=row['completed_at'],
                retry_count=row['retry_count'] or 0,
            )
        finally:
            cur.close()
            conn.close()

    def get_workflow_nodes(self, eid: str) -> Dict[str, NodeStatus]:
        conn = self._get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        try:
            cur.execute(
                """SELECT * FROM nodes WHERE execution_id = %s""",
                (eid,)
            )
            rows = cur.fetchall()
            nodes = {}

            for row in rows:
                nodes[row['node_id']] = NodeStatus(
                    node_id=row['node_id'],
                    state=NodeState(row['state']),
                    output=row['output'],
                    error=row['error'],
                    worker_id=row['worker_id'],
                    started_at=row['started_at'],
                    completed_at=row['completed_at'],
                    retry_count=row['retry_count'] or 0,
                )

            return nodes
        finally:
            cur.close()
            conn.close()

    def set_workflow_state(self, eid: str, state: WorkflowState,
                          error: Optional[str] = None):
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """UPDATE workflows SET state = %s, error = %s WHERE execution_id = %s""",
                (state.value, error, eid)
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

    def get_workflow_status(self, eid: str) -> Optional[WorkflowStatus]:
        conn = self._get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        try:
            cur.execute(
                """SELECT * FROM workflows WHERE execution_id = %s""",
                (eid,)
            )
            row = cur.fetchone()
            if not row:
                return None

            nodes = self.get_workflow_nodes(eid)

            return WorkflowStatus(
                execution_id=eid,
                workflow_name=row['name'],
                state=WorkflowState(row['state']),
                nodes=nodes,
                created_at=row['created_at'],
                error=row['error'],
            )
        finally:
            cur.close()
            conn.close()

    def get_node_output(self, eid: str, nid: str) -> Optional[Dict]:
        st = self.get_node_state(eid, nid)
        return st.output if st else None

    def save_definition(self, eid: str, wf: Dict[str, Any], ttl: int = 86400) -> None:
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """INSERT INTO workflow_definitions (execution_id, workflow_def)
                   VALUES (%s, %s)
                   ON CONFLICT (execution_id) DO UPDATE
                   SET workflow_def = EXCLUDED.workflow_def""",
                (eid, Json(wf))
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

    def get_definition(self, eid: str) -> Optional[Dict[str, Any]]:
        conn = self._get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        try:
            cur.execute(
                """SELECT workflow_def FROM workflow_definitions WHERE execution_id = %s""",
                (eid,)
            )
            row = cur.fetchone()
            return row['workflow_def'] if row else None
        finally:
            cur.close()
            conn.close()

    def mark_triggered(self, eid: str) -> bool:
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """UPDATE workflows SET triggered = TRUE
                   WHERE execution_id = %s AND triggered = FALSE""",
                (eid,)
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            cur.close()
            conn.close()
