import re
import json
from typing import Any, Dict

TEMPLATE_RE = re.compile(r"\{\{\s*(\w+)\s*\.\s*([\w.]+)\s*\}\}")


def resolve_templates(obj: Any, ctx: Dict[str, Any]) -> Any:
    if isinstance(obj, str):
        return _resolve_str(obj, ctx)
    elif isinstance(obj, dict):
        return {k: resolve_templates(v, ctx) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [resolve_templates(x, ctx) for x in obj]
    return obj


def _resolve_str(text: str, ctx: Dict[str, Any]) -> str:
    def replace_match(m):
        node_id, path = m.groups()
        if node_id not in ctx:
            raise ValueError(f"Node '{node_id}' not in context")

        val = ctx[node_id]
        for key in path.split("."):
            if not isinstance(val, dict):
                raise ValueError(f"Can't access '{key}' on {type(val).__name__}")
            val = val.get(key)
            if val is None:
                raise ValueError(f"Key '{key}' missing in '{node_id}'")

        if isinstance(val, (dict, list)):
            return json.dumps(val)
        return str(val)
    return TEMPLATE_RE.sub(replace_match, text)
