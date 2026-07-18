import os
import sys
import json
import logging
from typing import Any, Dict, Optional

import redis

sys.path.insert(0, '/app/shared')
from handlers import execute
from kafka_utils import create_consumer, create_producer

logger = logging.getLogger(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

consumer = create_consumer("task-queue", "workers")
producer = create_producer()
r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"), decode_responses=True)
WORKER_ID = os.getenv("WORKER_ID", "worker-unknown")


def get_cache(key: str) -> Optional[Dict[str, Any]]:
    res = r.get(f"result:{key}")
    return json.loads(res) if res else None

def set_cache(key: str, val: Dict[str, Any]) -> None:
    r.setex(f"result:{key}", 86400, json.dumps(val))

def is_cached(key: str) -> bool:
    return bool(r.exists(f"idempotent:{key}"))

def mark_cached(key: str) -> None:
    r.setex(f"idempotent:{key}", 86400, "1")


def run_task(task: Dict[str, Any]) -> Dict[str, Any]:
    idem_key = task.get("idempotency_key")
    eid, nid = task["execution_id"], task["node_id"]
    retry_count = task.get("retry_count", 0)

    if idem_key and is_cached(idem_key):
        cached = get_cache(idem_key)
        if cached:
            logger.info(f"Using cached {eid}:{nid}")
            return {**cached, "retry_count": retry_count}

    try:
        logger.info(f"Executing {task['handler']} on {eid}:{nid} (retry {retry_count})")
        output = execute(task["handler"], task.get("config", {}))
        result = {"status": "COMPLETED", "output": output, "retry_count": retry_count}
    except Exception as e:
        logger.error(f"Failed {eid}:{nid}: {e}")
        result = {"status": "FAILED", "error": str(e), "retry_count": retry_count}

    # Only successful results are idempotent; failures must re-execute on retry
    if idem_key and result["status"] == "COMPLETED":
        mark_cached(idem_key)
        set_cache(idem_key, result)
    return result


def main() -> None:
    logger.info(f"Worker {WORKER_ID} started")
    for msg in consumer:
        try:
            task = msg.value
            result = run_task(task)
            producer.send("task-results", {
                "event": "task.completed",
                "execution_id": task["execution_id"],
                "node_id": task["node_id"],
                "worker_id": WORKER_ID,
                "status": result["status"],
                "output": result.get("output"),
                "error": result.get("error"),
                "retry_count": result.get("retry_count", 0),
            })
        except Exception as e:
            logger.error(f"Worker error: {e}")


if __name__ == "__main__":
    main()
