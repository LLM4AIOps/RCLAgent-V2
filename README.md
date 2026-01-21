# RCLAgent

> **Paper**: Towards In-Depth Root Cause Localization for Microservices with Multi-Agent Recursion-of-Thought

![RCLAgent Workflow](assets/RCLAgent_workflow.pdf)

> RCLAgent ...

---

## 🚀 Quick Start

1. Start the Tool Server (provides data APIs)

```shell
python3 tool_server.py
```

This launches a local HTTP server (default: http://localhost:5000) that exposes:
- GET /search_span?span_id=...
- GET /search_traces?parent_span_id=...
- GET /search_fluctuating_metrics?timestamp=...&service_name=...
- GET /search_logs?timestamp=...&service_name=...

2. Run the Coordinator

```shell
python3 coordinator.py 
```

The coordinator will:
- Read failure traces from error_traces.txt
- For each failure, recursively invoke analysis agents
- Call the print_results function to output root causes

🔍 Sample Output

```json
{
  "root_causes": [
    "recommendationservice",
    "recommendationservice-1",
    "node-1"
  ]
}
```