import ast
import json
import os
import re

import openai
from tenacity import retry, stop_after_attempt, wait_exponential
import requests

model = "qwen-plus"


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
    llm_client = LLMClient(
        api_url="YOUR_API_URL",
        api_key="YOUR_API_KEY",
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
        while True:
            if tools:
                payload = {
                    "model": model,
                    "messages": prompts,
                    "tools": tools
                }
            else:
                payload = {
                    "model": model,
                    "messages": prompts
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
            if tools:
                return_message = response.json()['choices'][0]['message']
                if 'tool_calls' in return_message:
                    return "", return_message['tool_calls']
            else:
                return response.json()['choices'][0]['message'], []
