print_result_function = {
    "type": "function",
    "function": {
        "name": "print_result",
        "description": "Print the root cause node/service/pod and corresponding reason.",
        "parameters": {
            "type": "object",
            "properties": {
                "timestamp": {
                    "type": "string",
                    "description": "The root cause timestamp."
                },
                "node": {
                    "type": "string",
                    "description": "The root cause node (e.g. node-1)."
                },
                "service": {
                    "type": "string",
                    "description": "The root cause service (e.g. recommendationservice)."
                },
                "pod": {
                    "type": "string",
                    "description": "The root cause pod (e.g. recommendationservice-0)."
                },
                "reason": {
                    "type": "string",
                    "description": "The corresponding reason."
                },
            },
            "required": [
                "timestamp"
            ]
        }
    }
}

search_logs_function = {
    "type": "function",
    "function": {
        "name": "search_logs",
        "description": "Retrieve logs for a given service around a specific timestamp.",
        "parameters": {
            "type": "object",
            "properties": {
                "service_name": {
                    "type": "string",
                    "description": "The name of the service to be queried."
                },
                "timestamp": {
                    "type": "string",
                    "description": "The timestamp around which logs should be retrieved."
                }
            },
            "required": [
                "service_name",
                "timestamp"
            ]
        }
    }
}

search_fluctuating_metrics_function = {
    "type": "function",
    "function": {
        "name": "search_fluctuating_metrics",
        "description": "Get all fluctuating metrics with the input service_name and around the input timestamp",
        "parameters": {
            "type": "object",
            "properties": {
                "service_name": {
                    "type": "string",
                    "description": "The service_name to be queried."
                },
                "timestamp": {
                    "type": "string",
                    "description": "The timestamp to be queried."
                }
            },
            "required": [
                "service_name",
                "timestamp"
            ]
        }
    }
}
