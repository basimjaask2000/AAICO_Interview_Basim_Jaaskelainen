"""
Integration tests demonstrating all scenarios
Run after docker-compose is up and services are running
"""
import time
from typing import Any, Dict, Optional

import requests

BASE_URL = "http://localhost:8000"


def submit(workflow: Dict[str, Any]) -> str:
    resp = requests.post(f"{BASE_URL}/workflow", json=workflow)
    assert resp.status_code == 200, resp.text
    return resp.json()["execution_id"]


def trigger(eid: str, node_params: Optional[Dict[str, Dict[str, Any]]] = None) -> requests.Response:
    body = {"node_params": node_params} if node_params else None
    return requests.post(f"{BASE_URL}/workflow/trigger/{eid}", json=body)


def wait_for_completion(eid: str, timeout: int = 60) -> Dict[str, Any]:
    status: Dict[str, Any] = {}
    for _ in range(timeout):
        status = requests.get(f"{BASE_URL}/workflows/{eid}").json()
        print(f"  Status: {status['state']}")
        if status["state"] in ("COMPLETED", "FAILED"):
            break
        time.sleep(1)
    return status


def test_submit_does_not_start_execution():
    """Submitting alone must NOT run anything until triggered"""
    workflow = {
        "name": "no_autostart_test",
        "dag": {"nodes": [{"id": "A", "handler": "input", "dependencies": []}]}
    }
    eid = submit(workflow)

    time.sleep(3)
    status = requests.get(f"{BASE_URL}/workflows/{eid}").json()
    assert status["state"] == "SUBMITTED", f"expected SUBMITTED, got {status['state']}"
    assert status["nodes"]["A"]["state"] == "PENDING"
    print("✓ Submit does not auto-start\n")

    # now trigger it and confirm it runs
    resp = trigger(eid)
    assert resp.status_code == 200, resp.text
    status = wait_for_completion(eid)
    assert status["state"] == "COMPLETED"
    print("✓ Trigger starts execution\n")


def test_trigger_unknown_id():
    resp = trigger("does-not-exist")
    assert resp.status_code == 404
    print("✓ Trigger on unknown id returns 404\n")


def test_double_trigger_rejected():
    workflow = {
        "name": "double_trigger_test",
        "dag": {"nodes": [{"id": "A", "handler": "input", "dependencies": []}]}
    }
    eid = submit(workflow)
    assert trigger(eid).status_code == 200
    resp = trigger(eid)
    assert resp.status_code == 409, f"expected 409, got {resp.status_code}"
    wait_for_completion(eid)
    print("✓ Double trigger rejected\n")


def test_trigger_node_params():
    """Params supplied at trigger time override node config"""
    workflow = {
        "name": "node_params_test",
        "dag": {
            "nodes": [
                {"id": "fetch", "handler": "call_external_service",
                 "dependencies": [], "config": {"url": "https://original.example.com"}}
            ]
        }
    }
    eid = submit(workflow)
    resp = trigger(eid, node_params={"fetch": {"url": "https://overridden.example.com"}})
    assert resp.status_code == 200, resp.text

    status = wait_for_completion(eid)
    assert status["state"] == "COMPLETED"
    results = requests.get(f"{BASE_URL}/workflows/{eid}/results").json()
    assert "overridden.example.com" in results["result"]["fetch"]["data"]
    print("✓ Trigger node params override config\n")


def test_trigger_unknown_node_param_rejected():
    workflow = {
        "name": "bad_params_test",
        "dag": {"nodes": [{"id": "A", "handler": "input", "dependencies": []}]}
    }
    eid = submit(workflow)
    resp = trigger(eid, node_params={"nope": {"x": 1}})
    assert resp.status_code == 400, f"expected 400, got {resp.status_code}"
    print("✓ Unknown node ids in params rejected\n")


def test_linear_workflow():
    """Scenario A: Linear A → B → C"""
    workflow = {
        "name": "linear_test",
        "dag": {
            "nodes": [
                {"id": "A", "handler": "input", "dependencies": []},
                {"id": "B", "handler": "transform", "dependencies": ["A"]},
                {"id": "C", "handler": "output", "dependencies": ["B"]}
            ]
        }
    }
    eid = submit(workflow)
    assert trigger(eid).status_code == 200
    print(f"Linear workflow triggered: {eid}")

    status = wait_for_completion(eid)
    assert status["state"] == "COMPLETED"
    for nid in ("A", "B", "C"):
        assert status["nodes"][nid]["state"] == "COMPLETED"
    print("✓ Linear workflow passed\n")


def test_fan_in_workflow():
    """Scenario B: Fan-in A → [B, C] → D"""
    workflow = {
        "name": "fan_in_test",
        "dag": {
            "nodes": [
                {"id": "A", "handler": "input", "dependencies": []},
                {"id": "B", "handler": "transform", "dependencies": ["A"]},
                {"id": "C", "handler": "validate", "dependencies": ["A"]},
                {"id": "D", "handler": "output", "dependencies": ["B", "C"]}
            ]
        }
    }
    eid = submit(workflow)
    assert trigger(eid).status_code == 200
    print(f"Fan-in workflow triggered: {eid}")

    status = wait_for_completion(eid)
    assert status["state"] == "COMPLETED"
    for nid in ("B", "C", "D"):
        assert status["nodes"][nid]["state"] == "COMPLETED"
    print("✓ Fan-in workflow passed\n")


def test_template_resolution():
    """Template resolution {{ fetch.data }} passed downstream"""
    workflow = {
        "name": "template_test",
        "dag": {
            "nodes": [
                {
                    "id": "fetch",
                    "handler": "call_external_service",
                    "dependencies": [],
                    "config": {"url": "https://api.example.com"}
                },
                {
                    "id": "process",
                    "handler": "transform",
                    "dependencies": ["fetch"],
                    "config": {"input": "{{ fetch.data }}"}
                }
            ]
        }
    }
    eid = submit(workflow)
    assert trigger(eid).status_code == 200
    print(f"Template workflow triggered: {eid}")

    status = wait_for_completion(eid)
    assert status["state"] == "COMPLETED"
    assert status["nodes"]["fetch"]["state"] == "COMPLETED"
    assert status["nodes"]["process"]["state"] == "COMPLETED"

    results = requests.get(f"{BASE_URL}/workflows/{eid}/results").json()
    assert "fetch" in results["result"]
    print("✓ Template workflow passed\n")


def test_cycle_rejection():
    """Test that cycles are rejected"""
    workflow = {
        "name": "cycle_test",
        "dag": {
            "nodes": [
                {"id": "A", "handler": "input", "dependencies": ["B"]},
                {"id": "B", "handler": "transform", "dependencies": ["A"]}
            ]
        }
    }
    resp = requests.post(f"{BASE_URL}/workflow", json=workflow)
    assert resp.status_code == 400  # Should fail validation
    print("✓ Cycle rejection passed\n")


def test_missing_dependency_rejection():
    """Test that missing dependencies are rejected"""
    workflow = {
        "name": "missing_dep_test",
        "dag": {
            "nodes": [
                {"id": "B", "handler": "transform", "dependencies": ["MISSING"]}
            ]
        }
    }
    resp = requests.post(f"{BASE_URL}/workflow", json=workflow)
    assert resp.status_code == 400  # Should fail validation
    print("✓ Missing dependency rejection passed\n")


if __name__ == "__main__":
    print("Running integration tests...\n")
    print("Make sure docker-compose is running!\n")

    time.sleep(2)  # Give services time to start

    test_cycle_rejection()
    test_missing_dependency_rejection()
    test_trigger_unknown_id()
    test_submit_does_not_start_execution()
    test_double_trigger_rejected()
    test_trigger_node_params()
    test_trigger_unknown_node_param_rejected()
    test_linear_workflow()
    test_fan_in_workflow()
    test_template_resolution()

    print("\n✓✓✓ All integration tests passed! ✓✓✓")
