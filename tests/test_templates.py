import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from shared.templates import resolve_templates


def test_simple():
    ctx = {"A": {"data": "hello"}}
    result = resolve_templates("{{ A.data }}", ctx)
    assert result == "hello"


def test_nested():
    ctx = {"A": {"output": {"message": "success"}}}
    result = resolve_templates("{{ A.output.message }}", ctx)
    assert result == "success"


def test_in_dict():
    ctx = {"A": {"data": "api.example.com"}}
    cfg = {"url": "{{ A.data }}", "timeout": 30}
    result = resolve_templates(cfg, ctx)
    assert result == {"url": "api.example.com", "timeout": 30}


def test_in_list():
    ctx = {"A": {"item": "value1"}}
    cfg = ["{{ A.item }}", "value2"]
    result = resolve_templates(cfg, ctx)
    assert result == ["value1", "value2"]


def test_json_serialize():
    ctx = {"A": {"data": {"nested": "object"}}}
    cfg = {"payload": "{{ A.data }}"}
    result = resolve_templates(cfg, ctx)
    assert result == {"payload": '{"nested": "object"}'}


def test_missing_node():
    ctx = {"A": {"data": "value"}}
    try:
        resolve_templates("{{ B.data }}", ctx)
        assert False
    except ValueError:
        pass


def test_missing_key():
    ctx = {"A": {"data": "value"}}
    try:
        resolve_templates("{{ A.missing }}", ctx)
        assert False
    except ValueError:
        pass


def test_no_templates():
    ctx = {}
    cfg = {"url": "https://api.example.com", "timeout": 30}
    assert resolve_templates(cfg, ctx) == cfg


if __name__ == "__main__":
    test_simple()
    test_nested()
    test_in_dict()
    test_in_list()
    test_json_serialize()
    test_missing_node()
    test_missing_key()
    test_no_templates()
    print("✓ Template tests passed")
