# Summary of Repo

A distributed task orchestration system that is designed to handle complex workflows using DAG. You would submit a workflow, track its progress, and be able to poll to get your results back. 

## Getting Started

To start the services, youd need to run:

```bash
docker-compose up --build
```

This would start:
- **API** at `http://localhost:8000`
- **Redis** at `localhost:6379`
- **Orchestrator** (background service managing your workflows)
- **2 Workers** (background services executing your tasks)

To verify start up is complete, run the command below:

```bash
curl http://localhost:8000/health
```

## Using/Testing the API:

### 1. Submiting a Workflow

You create a workflow by POSTing a DAG structure to the API. Submitting only validates and stores the workflow — nothing runs until you trigger it (step 2). Here's a sample that fetches some data, transforms it, and outputs the result:

```bash
curl -X POST http://localhost:8000/workflow \
  -H "Content-Type: application/json" \
  -d '{
    "name": "data_pipeline",
    "dag": {
      "nodes": [
        {
          "id": "fetch",
          "handler": "call_external_service",
          "dependencies": [],
          "config": {"url": "https://api.example.com/data"}
        },
        {
          "id": "transform",
          "handler": "transform",
          "dependencies": ["fetch"],
          "config": {"input": "{{ fetch.data }}"}
        },
        {
          "id": "output",
          "handler": "output",
          "dependencies": ["transform"],
          "config": {}
        }
      ]
    }
  }'
```

You'll get back an execution ID which can be used for triggering and polling the workflow. The format of the return payload is below:

```json
{
  "execution_id": "abc-123-def-456",
  "status": "SUBMITTED",
  "message": "Workflow submitted. POST /workflow/trigger/{execution_id} to start execution."
}
```

### 2. Triggering the Workflow

Execution starts only when you trigger it with the execution ID from step 1:

```bash
curl -X POST http://localhost:8000/workflow/trigger/abc-123-def-456
```

You can optionally pass per-node config overrides in the body — they're merged into the matching node's `config` before execution starts:

```bash
curl -X POST http://localhost:8000/workflow/trigger/abc-123-def-456 \
  -H "Content-Type: application/json" \
  -d '{"node_params": {"fetch": {"url": "https://other-api.example.com/data"}}}'
```

Triggering the same execution twice returns `409 Conflict`; an unknown execution ID returns `404`.

```json
{
  "execution_id": "abc-123-def-456",
  "status": "TRIGGERED",
  "message": "Execution started. Check /workflows/{execution_id} for status."
}
```

### 3. Check Workflow Status

If you want to check the status of the workflow, query the status endpoint using the execution id returned when you submitted the workflow (as shown below):

```bash
curl http://localhost:8000/workflows/abc-123-def-456
```

You'll see details for each node (which ones are done, which are running, and what worker is handling them)
The return payload example is below:

```json
{
  "execution_id": "abc-123-def-456",
  "workflow_name": "data_pipeline",
  "state": "RUNNING",
  "nodes": {
    "fetch": {
      "node_id": "fetch",
      "state": "COMPLETED",
      "output": {"status": "success", "data": "..."},
      "worker_id": "worker-1",
      "started_at": "2024-01-15T10:30:00",
      "completed_at": "2024-01-15T10:30:01"
    },
    "transform": {
      "node_id": "transform",
      "state": "RUNNING",
      "output": null,
      "worker_id": "worker-2",
      "started_at": "2024-01-15T10:30:02"
    },
    "output": {
      "node_id": "output",
      "state": "PENDING",
      "output": null
    }
  },
  "created_at": "2024-01-15T10:30:00",
  "error": null
}
```

### 4. Retrieving the Results

Once the workflow has completed, you can curl using the same execution id to get the results as listed below:

```bash
curl http://localhost:8000/workflows/abc-123-def-456/results
```

This returns a view of all outputs from each node.  The return payload example is below:

```json
{
  "execution_id": "abc-123-def-456",
  "workflow_name": "data_pipeline",
  "state": "COMPLETED",
  "result": {
    "fetch": {"status": "success", "data": "..."},
    "transform": {"transformed": true, "original": {...}},
    "output": {"result": "completed"}
  },
  "error": null
}
```

## Passing Data Between Nodes

If you want to pass output from one node to another you can use template syntax to reference the outputs of completed nodes.:

```json
{
  "id": "transform",
  "handler": "transform",
  "dependencies": ["fetch"],
  "config": {
    "url": "{{ fetch.data }}",
    "prompt": "Process: {{ fetch.data }}",
    "nested": "{{ fetch.output.key }}"
  }
}
```

Before a task goes to a worker, all those `{{ node_id.path }}` references get swapped out with the real values from nodes that already ran.

## Testing

To test the functionalities, please run the test suite. The commands to do so are below:

```bash
cd tests
python test_validation.py
python test_templates.py
```

There is also an end-to-end suite that exercises the live API (submit → trigger → poll → results, including the failure cases). Run it while docker-compose is up:

```bash
cd tests
python test_integration.py
```

## Debugging & Logs

To see logs for the api, orchestrator or workers, please run the commands below:

```bash
docker-compose logs -f api
docker-compose logs -f orchestrator
docker-compose logs -f worker-1
```

