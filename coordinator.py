import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

from llm import chat_api
from tools_gpt import (
    search_logs_function,
    search_fluctuating_metrics_function,
    print_result_function  # 用于最终输出
)

base_tool_path = "http://127.0.0.1:5000"


def pack_get_parameter(d):
    if isinstance(d, str):
        d = json.loads(d)
    return "&".join(f"{k.strip()}={str(v).strip()}" for k, v in d.items())


def get_response(path, parameters):
    url = base_tool_path + "/" + path + "?" + pack_get_parameter(parameters)
    try:
        response = requests.get(url)
        return response.content.decode("utf-8")
    except Exception:
        return ""


class AgentPool:
    def __init__(self, max_parallel_num=8):
        self.executor = ThreadPoolExecutor(max_workers=max_parallel_num)

    def submit(self, fn, *args, **kwargs):
        return self.executor.submit(fn, *args, **kwargs)

    def shutdown(self):
        self.executor.shutdown(wait=True)


# === 核心系统提示（所有 Agent 共享）===
SYSTEM_PROMPT = """
You are a Root Cause Localization (RCL) agent in a microservice system.
A user-reported failure has occurred. Your task is to analyze logs and metrics 
to identify the DEEPEST ROOT CAUSE SERVICE that initiated the failure chain.

Guidelines:
- Always assume the trace contains a real fault.
- Focus on anomalies: errors, latency spikes, resource saturation, failed dependencies.
- Prefer evidence from logs (e.g., exceptions) and metrics (e.g., CPU, error rate).
- Do NOT blame the frontend unless evidence clearly points to it.
- Return ONLY valid JSON when asked for structured output.
"""


class RCLAgent:
    def __init__(self, span_id, raw, pool: AgentPool):
        self.span_id = span_id
        self.raw = raw
        self.pool = pool
        self.children = []
        self.downstream_evidences = []
        self.self_evidence = None

    def add_child(self, child_agent):
        self.children.append(child_agent)

    def self_state_verification(self):
        """Analyze local logs/metrics for self-anomaly using iterative tool reasoning."""
        initial_prompt = f"""
        Analyze the following span for self-contained anomalies:

        Span Data:
        {json.dumps(self.raw, indent=2)}

        Steps:
        1. Use search_logs_function to fetch logs for this service around its timestamp.
        2. Use search_fluctuating_metrics_function to check for metric spikes (latency, error rate, etc.).
        3. Determine if THIS span shows symptoms of being faulty (e.g., errors, high latency, exceptions).

        You may use the provided tools iteratively. Only when you have enough evidence, produce a final JSON conclusion.
        """

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": initial_prompt}
        ]

        available_tools = [search_logs_function, search_fluctuating_metrics_function]
        tool_map = {tool["function"]["name"]: get_response for tool in available_tools}

        max_turns = 5  # prevent infinite loops
        turn = 0

        while turn < max_turns:
            content, tool_calls = chat_api(messages, tools=available_tools)

            # Otherwise, execute all requested tool calls
            for tool_call in tool_calls:
                tool_name = tool_call["function"]["name"]
                try:
                    tool_args = json.loads(tool_call["function"]["arguments"])
                except (json.JSONDecodeError, TypeError):
                    tool_args = {}

                # Execute the tool
                if tool_name in tool_map:
                    tool_result = tool_map[tool_name](tool_name, tool_args)
                else:
                    tool_result = f"Error: Unknown tool '{tool_name}'"

                # Append tool's response
                messages.append({"role": "assistant", "content": tool_result})

            turn += 1

        # Fallback: after max turns, force final structured output
        final_prompt = f"""
        Based on ALL available information:
        - Original Span Data:
        {json.dumps(self.raw, indent=2)}
        - Retrieved Logs and Metrics (from prior tool responses)

        Since this trace is confirmed to contain a failure, please analyze as thoroughly as possible whether the fault manifests within the current context.

        Summarize self-state evidence as STRICT JSON (no markdown, no explanation):
        {{
          "span_id": "...",
          "service_name": "...",
          "is_abnormal": true/false,
          "key_symptoms": "brief string",
          "hypothesis": "why it might be faulty or not"
        }}
        """
        messages.append({"role": "user", "content": final_prompt})
        final_content, _ = chat_api(messages, tools=None)
        return final_content.strip()

    def run(self):
        """Run children in parallel, then consolidate."""
        self.self_evidence = self.self_state_verification()

        futures = [self.pool.submit(child.run) for child in self.children]
        for f in as_completed(futures):
            result = f.result()
            if result:
                self.downstream_evidences.append(result)

        consolidation_prompt = f"""
        You are analyzing span: {self.raw}

        Downstream evidences from children ({len(self.downstream_evidences)} items):
        {json.dumps(self.downstream_evidences, indent=2)}

        Your own self-evidence:
        {self.self_evidence}

        Task: Synthesize these into a LOCAL root cause hypothesis for your parent.
        If any child shows strong evidence of being the root, propagate it upward.
        Otherwise, if YOU are abnormal, propose yourself.

        Output format (JSON only):
        {{
          "span_id": "...",
          "service_name": "...",
          "local_root_cause": "service name or 'self'",
          "reason": "...",
          "confidence": 0.0-1.0
        }}
        """
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": consolidation_prompt}
        ]
        content, _ = chat_api(messages, None)
        return content


def build_agent_tree(trace_node, pool):
    """Recursively build agent tree from trace graph node."""
    raw = trace_node.get("raw", {})
    span_id = trace_node.get("span_id", "unknown")
    agent = RCLAgent(span_id, raw, pool)

    for child in trace_node.get("children", []):
        child_agent = build_agent_tree(child, pool)
        agent.add_child(child_agent)
    return agent


def inspect_trace(trace_graph, max_parallel_num=8):
    pool = AgentPool(max_parallel_num=max_parallel_num)
    root_agent = build_agent_tree(trace_graph, pool)
    intermediate_result = root_agent.run()  # populate self_evidence for all nodes
    pool.shutdown()

    # === 新增：递归收集所有节点的 self_evidence ===
    def collect_all_self_evidences(agent):
        evidences = []
        if agent.self_evidence:
            evidences.append(agent.self_evidence)
        for child in agent.children:
            evidences.extend(collect_all_self_evidences(child))
        return evidences

    all_self_evidences = collect_all_self_evidences(root_agent)

    final_analysis_prompt = f"""
You are the ROOT AGENT responsible for producing the FINAL diagnosis of a system failure.

Your goal: Identify the single deepest root cause service that initiated the failure chain.

Combine with Intermediate Summary and Trace Graph to analyze.

Intermediate Summary: {intermediate_result}

Trace Graph: {json.dumps(trace_graph, indent=2, ensure_ascii=False, default=str)}

IMPORTANT: You MUST call the function `print_result_function`.

To improve accuracy, consider exploring candidates around the most likely root cause. For example, if you identify `recommendationservice-2` as a potential root cause, you should also evaluate whether the true root lies at the pod level (`recommendationservice-2`), the service level (`recommendationservice`), or the node level (`node-6`). If the evidence is inconclusive, it is acceptable—and even encouraged—to include all plausible candidates in your output, prioritized by likelihood. When the evidence is inconclusive, prioritize candidates in the following order: **service** first, then **pod**, and finally **node**.

Do not output anything else. Use the tool.
"""
    print(intermediate_result)
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": final_analysis_prompt}
    ]

    content, tools = chat_api(
        messages,
        tools=[print_result_function]
    )

    if tools:
        tool_call = tools[0]["function"]
        args = tool_call["arguments"]
        if isinstance(args, str):
            try:
                result_dict = json.loads(args)
                return json.dumps(result_dict, ensure_ascii=False)
            except:
                return args
        else:
            return json.dumps(args, ensure_ascii=False)

    return json.dumps({
        "root_cause_service": "unknown",
        "root_span": trace_graph.get("span_id", "unknown"),
        "reason": "Model did not call print_result_function",
        "confidence": 0.0
    })


def query_children(parent_span_id):
    url = f"{base_tool_path}/search_traces"
    try:
        r = requests.get(url, params={"parent_span_id": parent_span_id}, timeout=5)
        text = r.text
        text = text.replace('NaN', 'null').replace('Infinity', '"Infinity"')
        data = json.loads(text)
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"query_children failed for {parent_span_id}: {e}")
        return []


def build_trace_graph(root_span_id, max_depth=20):
    def dfs(span_id, depth):
        # Fetch span metadata (assume /search_traces returns full row including service_name, timestamp, etc.)
        children_rows = query_children(span_id) if depth < max_depth else []

        node = {
            "span_id": span_id,
            "raw": {"span_id": span_id},  # Placeholder; ideally fetched from a /get_span endpoint
            "children": []
        }

        # Try to enrich raw with actual data (optional improvement)
        # For now, we rely on children_rows having necessary fields

        for row in children_rows:
            child_span_id = row.get("span_id")
            child_node = dfs(child_span_id, depth + 1)
            child_node.update({
                "raw": row,
                "span_id": child_span_id,
                "service_name": row.get("service_name"),
                "timestamp": row.get("timestamp")
            })
            node["children"].append(child_node)

        return node

    return dfs(root_span_id, 0)


def inspect_all_traces():
    error_file_path = f"sample_data/error_traces.txt"
    print(error_file_path)
    try:
        with open(error_file_path, "r") as fr:
            lines = fr.readlines()
    except Exception as e:
        print(f"Failed to read {error_file_path}: {e}")
        return

    for i, line in enumerate(lines[1:], start=1):  # 跳过 header
        try:
            parts = line.strip().split()
            if len(parts) < 3:
                continue
            root_span_id = parts[3].strip()  # span_id 是第4列
            print(root_span_id)
            if not root_span_id:
                continue

            trace_graph = build_trace_graph(root_span_id)
            trace_graph.update({
                "raw": line
            })
            result = inspect_trace(trace_graph, max_parallel_num=100)

            conversation_file_path = f"sample_data/conversation_trace_{i}.txt"
            with open(conversation_file_path, 'w') as file:
                file.write(str(result) + "\n")
        except Exception as e:
            print(f"Error processing line {i}: {e}")


if __name__ == "__main__":
    inspect_all_traces()
