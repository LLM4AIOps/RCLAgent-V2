import ast
import json
import os
import re

import openai
from tenacity import retry, stop_after_attempt, wait_exponential
import requests

buy = False
model = "claude-3-5-sonnet-latest"

TOOL_CALL_PROMPT = '''"{prompt_message}\n"
    "You have access to the following tools:\n{tool_text}\n"
    "Use the following format if using a tool:\n"
    "```\n"
    "Action: tool name (one of [{tool_names}])\n"
    "Action Input: the input to the tool, in a JSON format representing the kwargs "
    """(e.g. ```{{"input": "hello world", "num_beams": 5}}```)\n"""
    "```\n"
'''

QWQ_CALL_PROMPT = '''"<|im_start|>system\nYou are a helpful assistant. {prompt_message}\n\n"
    "你拥有如下工具：\n\n"
    "{tool_text}"
    "此工具的参数填充应为JSON对象，JSON键名为参数名称，键值为参数内容。\n\n"
    "你必须在回复中插入一次以下命令以调用工具：\n\n"
    "✿FUNCTION✿: 工具名称，**能且仅能**是[{tool_names}]之一\n"
    "✿ARGS✿: 工具输入\n"
    "<|im_end|>\n<|im_start|>assistant\n"
'''


def get_tool_names(tools):
    names = []
    for tool in tools:
        names.append(tool["function"]["name"])
    return names


def get_tool_from_content(content, tool_names):
    tools = []
    lines = content.split("\n")
    name = None
    for line in lines:
        if line.startswith("Action: "):
            name = line[len("Action: "):]
        elif line.startswith("✿FUNCTION✿: "):
            name = line[len("✿FUNCTION✿: "):]
        elif line.startswith("✿ARGS✿: "):
            arguments = line[len("✿ARGS✿: "):]
            if name in tool_names:
                tools.append({
                    "function": {
                        "name": name,
                        "arguments": arguments
                    }
                })
        elif line.startswith("Action Input: "):
            arguments = line[len("Action Input: "):]
            if name in tool_names:
                tools.append({
                    "function": {
                        "name": name,
                        "arguments": arguments
                    }
                })
    return tools


import warnings

warnings.filterwarnings("ignore")


def chat_api(prompts, tools):
    if "claude" in model:
        llm_client = LLMClient(
            api_url="xxx",
            api_key="xxx"
        )
    elif "gpt" in model:
        llm_client = LLMClient(
            api_url="xxx",
            api_key="xxx"
        )
    elif "qwen" in model:
        llm_client = LLMClient(
            api_url="xxx",
            api_key="xxx"
        )
    elif "gemini" in model:
        llm_client = LLMClient(
            api_url="xxx",
            api_key="xxx"
        )
    elif "Llama" in model:
        llm_client = LLMClient(
            api_url="http://localhost:8000/v1/chat/completions",
            api_key="s"
        )
    elif "QwQ" in model:
        llm_client = LLMClient(
            api_url="http://localhost:8000/v1/chat/completions",
            api_key="s"
        )
    else:
        llm_client = LLMClient(
            api_url="http://localhost:8000/v1/chat/completions",
            api_key="s"
        )
    response = llm_client.generate(prompts, tools)
    return response


class LLMClient:
    """LLM调用客户端"""

    def __init__(self,
                 api_url: str,
                 api_key: str,
                 ):
        self.api_url = api_url
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }

    @retry(
        stop=stop_after_attempt(1),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True
    )
    def generate(self, prompts: list, tools):
        """调用LLM生成内容"""
        retry_time = 10
        while True:
            if tools:
                if 'claude' in model:
                    if buy:
                        if retry_time >= 10:
                            prompts[-1]["content"] = TOOL_CALL_PROMPT.format(
                                prompt_message=prompts[-1]["content"],
                                tool_text=json.dumps(tools),
                                tool_names=", ".join(get_tool_names(tools)))
                        payload = {
                            "model": model,
                            "messages": prompts,
                            "max_tokens": 8192
                        }
                    else:
                        payload = {
                            "model": model,
                            "messages": prompts,
                            "max_tokens": 8192,
                            "tools": tools,
                            "tool_choice": {
                                "type": "any"
                            }
                        }
                elif 'gpt' in model:
                    payload = {
                        "model": model,
                        "messages": prompts,
                        "tools": tools,
                        "tool_choice": "required"
                    }
                elif 'qwen' in model:
                    payload = {
                        "model": model,
                        "messages": prompts,
                        "tools": tools
                    }
                elif 'Llama' in model:
                    if retry_time >= 10:
                        prompts[-1]["content"] = TOOL_CALL_PROMPT.format(
                            tool_text=json.dumps(tools),
                            tool_names=",".join(get_tool_names(tools)),
                            prompt_message=prompts[-1]["content"])
                    payload = {
                        "model": model,
                        "messages": prompts
                    }
                elif 'QwQ' in model:
                    if retry_time >= 10:
                        prompts[-1]["content"] = QWQ_CALL_PROMPT.format(
                            tool_text=json.dumps(tools),
                            tool_names=",".join(get_tool_names(tools)),
                            prompt_message=prompts[-1]["content"])
                    payload = {
                        "model": model,
                        "messages": prompts,
                        "max_tokens": 1024
                    }
                else:
                    payload = {
                        "model": model,
                        "messages": prompts
                    }
            else:
                payload = {
                    "model": model,
                    "messages": prompts,
                    "max_tokens": 8192,
                }

            try:
                response = requests.post(
                    self.api_url,
                    headers=self.headers,
                    json=payload,
                    verify=False
                )
                response.raise_for_status()
            except Exception as e:
                print(e)
                continue
            if 'gpt' in model:
                return_message = response.json()['data']['response']['choices'][0]['message']
                if 'tool_calls' in return_message:
                    return return_message['content'], return_message['tool_calls']
                else:
                    return return_message, []
            elif 'claude' in model:
                if buy:
                    content = response.json()['content'][0]['text']
                    res_tools = get_tool_from_content(content, get_tool_names(tools))
                    if res_tools:
                        return content, res_tools
                else:
                    content = response.json()
                    res_tools = []
                    if content['stop_reason'] == 'tool_use':
                        for tool in content['content']:
                            res_tools.append({
                                'function': {
                                    'name': tool['name'],
                                    'arguments': tool['input']
                                }
                            })
                    if len(res_tools) > 0:
                        return content, res_tools
                retry_time -= 1
                if retry_time == 0:
                    return content, [{'function': {'name': 'print_result',
                                                   'arguments': '{"timestamp":0,"node":"0","service":"0","pod":"0","reason":"0"}'}}]
            elif 'qwen' in model:
                return_message = response.json()['choices'][0]['message']
                if 'tool_calls' in return_message:
                    return "", return_message['tool_calls']
            elif 'LLama' in model:
                content = response.json()['choices'][0]['message']['content']
                res_tools = get_tool_from_content(content, get_tool_names(tools))
                if res_tools:
                    return content, res_tools
                retry_time -= 1
                if retry_time == 0:
                    return content, [{'function': {'name': 'print_result',
                                                   'arguments': '{"timestamp":0,"node":"0","service":"0","pod":"0","reason":"0"}'}}]
            elif 'QwQ' in model:
                content = response.json()['choices'][0]['message']['content']
                res_tools = get_tool_from_content(content, get_tool_names(tools))
                if res_tools:
                    return content, res_tools
                retry_time -= 1
                if retry_time == 0:
                    return content, [{'function': {'name': 'print_result',
                                                   'arguments': '{"timestamp":0,"node":"0","service":"0","pod":"0","reason":"0"}'}}]
            else:
                content = response.json()['choices'][0]['message']['content']
                content_json = content
                if content_json:
                    return content, [{"function": ast.literal_eval(content_json)}]
                else:
                    return content, []
