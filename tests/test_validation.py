import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from shared.models import Node, DAG
from shared.validation import DAGValidator


def test_linear():
    nodes = [
        Node(id="A", handler="input", dependencies=[]),
        Node(id="B", handler="transform", dependencies=["A"]),
        Node(id="C", handler="output", dependencies=["B"]),
    ]
    dag = DAG(nodes=nodes)
    v = DAGValidator(dag)
    ok, msg = v.validate()
    assert ok, msg


def test_fan_out():
    nodes = [
        Node(id="A", handler="input", dependencies=[]),
        Node(id="B", handler="transform", dependencies=["A"]),
        Node(id="C", handler="validate", dependencies=["A"]),
        Node(id="D", handler="output", dependencies=["B", "C"]),
    ]
    dag = DAG(nodes=nodes)
    v = DAGValidator(dag)
    ok, msg = v.validate()
    assert ok, msg


def test_cycle():
    nodes = [
        Node(id="A", handler="input", dependencies=["B"]),
        Node(id="B", handler="transform", dependencies=["A"]),
    ]
    dag = DAG(nodes=nodes)
    v = DAGValidator(dag)
    ok, msg = v.validate()
    assert not ok
    assert "circular" in msg.lower()


def test_missing_dep():
    nodes = [Node(id="B", handler="transform", dependencies=["A"])]
    dag = DAG(nodes=nodes)
    v = DAGValidator(dag)
    ok, msg = v.validate()
    assert not ok
    assert "missing" in msg.lower()


def test_duplicate_ids():
    try:
        nodes = [
            Node(id="A", handler="input", dependencies=[]),
            Node(id="A", handler="transform", dependencies=[]),
        ]
        dag = DAG(nodes=nodes)
        assert False
    except ValueError:
        pass


def test_executable_linear():
    nodes = [
        Node(id="A", handler="input", dependencies=[]),
        Node(id="B", handler="transform", dependencies=["A"]),
        Node(id="C", handler="output", dependencies=["B"]),
    ]
    dag = DAG(nodes=nodes)
    v = DAGValidator(dag)

    assert v.get_executable_nodes(set()) == ["A"]
    assert v.get_executable_nodes({"A"}) == ["B"]
    assert v.get_executable_nodes({"A", "B"}) == ["C"]


def test_executable_fan_in():
    nodes = [
        Node(id="A", handler="input", dependencies=[]),
        Node(id="B", handler="transform", dependencies=["A"]),
        Node(id="C", handler="validate", dependencies=["A"]),
        Node(id="D", handler="output", dependencies=["B", "C"]),
    ]
    dag = DAG(nodes=nodes)
    v = DAGValidator(dag)

    assert v.get_executable_nodes(set()) == ["A"]
    assert set(v.get_executable_nodes({"A"})) == {"B", "C"}
    assert v.get_executable_nodes({"A", "B"}) == ["C"]
    assert v.get_executable_nodes({"A", "B", "C"}) == ["D"]


if __name__ == "__main__":
    test_linear()
    test_fan_out()
    test_cycle()
    test_missing_dep()
    test_duplicate_ids()
    test_executable_linear()
    test_executable_fan_in()
    print("✓ Validation tests passed")
