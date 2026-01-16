import json
import sys
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

from llm import chat_api
from tools_claude import (
    search_logs_function,
    search_fluctuating_metrics_function,
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
    except:
        return ""


class AgentPool:
    def __init__(self, max_parallel_num=8):
        self.executor = ThreadPoolExecutor(max_workers=max_parallel_num)

    def submit(self, fn, *args, **kwargs):
        return self.executor.submit(fn, *args, **kwargs)

    def shutdown(self):
        self.executor.shutdown(wait=True)


class RCLAgent:
    def __init__(self, span, raw, pool: AgentPool):
        self.span = span
        self.raw = raw
        self.pool = pool
        self.children = []
        self.downstream_evidences = []

    def add_child(self, child_agent):
        self.children.append(child_agent)

    def self_state_verification(self):
        """
        Perform localized analysis on this span using logs and metrics.
        """
        prompt = f"""
        You are responsible for diagnosing the following span:
        
        {self.raw}
        
        Please perform self-state verification by:
        1. Inspecting relevant logs.
        2. Inspecting fluctuating metrics.
        Summarize whether this span itself exhibits anomalous behavior.
        Return a concise evidence object.
        """
        prompts = [{"role": "user", "content": prompt}]
        content, tools = chat_api(
            prompts,
            tools=[search_logs_function, search_fluctuating_metrics_function],
        )

        for tool in tools or []:
            tool_result = get_response(tool["function"]["name"], tool["function"]["arguments"])
            prompts.append({"role": "assistant", "content": tool_result})

        final_prompt = """
        Summarize the self-state evidence for this span in a compact JSON form:
        {
          "span": "...",
          "is_abnormal": true/false,
          "key_symptoms": "...",
          "hypothesis": "..."
        }
        """
        prompts.append({"role": "user", "content": final_prompt})
        content, _ = chat_api(prompts)
        return content

    def run(self):
        """
        Recursively run child agents in parallel, then consolidate evidences.
        """
        futures = []
        for child in self.children:
            futures.append(self.pool.submit(child.run))

        for f in as_completed(futures):
            self.downstream_evidences.append(f.result())

        self_evidence = self.self_state_verification()

        consolidation_prompt = f"""
        You are an agent responsible for span:
        
        {self.raw}
        
        Downstream evidences from children:
        {json.dumps(self.downstream_evidences, indent=2)}
        
        Your own self-state evidence:
        {self_evidence}
        
        Please consolidate these evidences and output a compact hypothesis for your parent in JSON:
        {{
          "span": "...",
          "local_root_cause": "...",
          "reason": "...",
          "confidence": 0-1
        }}
        """
        prompts = [{"role": "user", "content": consolidation_prompt}]
        content, _ = chat_api(prompts)
        return content


def build_agent_tree(trace_graph, pool):
    """
    trace_graph: a tree-like structure of spans
    """
    node = RCLAgent(trace_graph["span"], trace_graph["raw"], pool)
    for child in trace_graph.get("children", []):
        child_agent = build_agent_tree(child, pool)
        node.add_child(child_agent)
    return node


def inspect_trace(trace_graph, max_parallel_num=8):
    pool = AgentPool(max_parallel_num=max_parallel_num)
    root_agent = build_agent_tree(trace_graph, pool)

    # 递归并行执行，得到 root-level evidence
    root_evidence = root_agent.run()
    pool.shutdown()

    # Root-level global synthesis
    root_prompt = f"""
    You are the Root Agent responsible for producing the final diagnosis.
    
    Your goal is to identify the true root cause service and provide a clear,
    actionable explanation based on all collected evidences.
    
    Root span:
    {trace_graph.get("raw")}
    
    Root-level evidence:
    {root_evidence}
    
    Please construct a global evidence graph implicitly from the above information
    and return the final result in the following JSON format:
    
    {{
      "root_cause_service": "...",
      "root_span": "...",
      "reason": "...",
      "confidence": 0-1
    }}
    """
    prompts = [{"role": "user", "content": root_prompt}]
    final_content, _ = chat_api(prompts)

    return final_content

def query_children(parent_span_id):
    url = f"{base_tool_path}/search_traces"
    try:
        r = requests.get(url, params={"parent_span_id": parent_span_id}, timeout=5)
        data = r.json()
        if isinstance(data, list):
            return data
        return []
    except Exception as e:
        print(f"query_children failed for {parent_span_id}: {e}")
        return []


def build_trace_graph(root_span_id, max_depth=20):
    """
    Recursively build a trace tree starting from root_span_id.
    """

    def dfs(span_id, depth):
        node = {
            "span_id": span_id,
            "children": []
        }

        if depth >= max_depth:
            return node

        children_rows = query_children(span_id)
        for row in children_rows:
            child_span_id = row.get("span_id")
            child_node = {
                "span_id": child_span_id,
                "service_name": row.get("service_name"),
                "timestamp": row.get("timestamp"),
                "raw": row,
                "children": []
            }
            # 递归展开
            child_node = dfs(child_span_id, depth + 1) | child_node
            node["children"].append(child_node)

        return node

    return dfs(root_span_id, 0)


def inspect_all_traces(sub_path):
    error_file_path = f"data/{sub_path}/hipstershop.Frontend/Recv._durations.txt"
    print(error_file_path)
    fr = open(error_file_path, "r")
    lines = fr.readlines()

    for i in range(1, len(lines)):
        try:
            root_span_id = lines[i]
            trace_graph = build_trace_graph(root_span_id)
            result = inspect_trace(trace_graph, max_parallel_num=8)

            conversation_file_path = f"data/{sub_path}/result/conversation_trace_{i}.txt"
            with open(conversation_file_path, 'w') as file:
                file.write(str(result) + "\n")
        except Exception as e:
            print(e)
            i -= 1


if __name__ == "__main__":
    inspect_all_traces(sys.argv[1])
