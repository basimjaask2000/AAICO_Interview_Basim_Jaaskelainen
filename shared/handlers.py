import time


def handle_input(cfg):
    return {"data": "input", "ts": time.time()}


def handle_output(cfg):
    return {"result": "completed", "ts": time.time()}


def handle_call_external_service(cfg):
    time.sleep(1)
    url = cfg.get("url", "https://api.example.com")
    return {"status": "success", "data": f"from {url}", "ts": time.time()}


def handle_llm_service(cfg):
    time.sleep(0.5)
    prompt = cfg.get("prompt", "default")
    return {"result": f"Response for: {prompt}", "tokens": 42, "ts": time.time()}


def handle_transform(cfg):
    time.sleep(0.3)
    return {"transformed": True, "ts": time.time()}


def handle_validate(cfg):
    time.sleep(0.2)
    return {"valid": True, "errors": [], "ts": time.time()}


HANDLERS = {
    "input": handle_input,
    "output": handle_output,
    "call_external_service": handle_call_external_service,
    "llm_service": handle_llm_service,
    "transform": handle_transform,
    "validate": handle_validate,
}


def execute(handler_name, cfg):
    handler = HANDLERS.get(handler_name)
    if not handler:
        raise ValueError(f"Unknown handler: {handler_name}")
    return handler(cfg)
