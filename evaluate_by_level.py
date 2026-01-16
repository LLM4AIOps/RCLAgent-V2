import ast
import json
import os
import sys
import re

import pandas as pd
from datetime import datetime

import warnings

warnings.filterwarnings("ignore")

sub_path = sys.argv[1]
rethink_level = int(sys.argv[2])
result_sub_path = "result"
groundtruth = pd.read_csv(f"data/{sub_path}/groundtruth.csv")
error_file_path = f"data/{sub_path}/hipstershop.Frontend/Recv._durations.txt"
error_lines = open(error_file_path).readlines()

groundtruth['timestamp'] = pd.to_datetime(groundtruth['timestamp'], unit='s')


def find_nearest_groundtruth_row(result_timestamp, groundtruth_df):
    if "T" in result_timestamp:
        dt_obj = datetime.strptime(result_timestamp, "%Y-%m-%dT%H:%M:%SZ")
        result_timestamp = str(dt_obj.timestamp())
    if len(result_timestamp) > 10:
        result_timestamp = result_timestamp[:10]
    result_timestamp = datetime.utcfromtimestamp(int(result_timestamp))

    groundtruth_df['diff'] = abs(groundtruth_df['timestamp'] - result_timestamp)
    nearest_row = groundtruth_df.loc[groundtruth_df['diff'].idxmin()]
    del groundtruth_df['diff']

    return nearest_row


def find_result_line(lines):
    curr_level = 0
    for line in lines:
        if "print_result" in line and "argument" in line:
            curr_level += 1
            if curr_level == rethink_level:
                return line


def extract_specific_values(json_str):
    # 确保输入转换为字符串
    if not isinstance(json_str, str):
        json_str = str(json_str)

    extracted = {}
    # 匹配键和值，允许单引号或双引号
    pattern = r"['\"](service|pod|node)['\"]\s*:\s*['\"]([^'\"]*)['\"]"
    matches = re.findall(pattern, json_str)

    for key, value in matches:
        extracted[key] = value

    return extracted


hit_num = 0
total_num = 0
# 遍历结果文件夹中的JSON文件
for result_file in os.listdir(f"data/{sub_path}/{result_sub_path}"):
    if result_file.endswith('.txt'):
        with open(f"data/{sub_path}/{result_sub_path}/{result_file}", 'r') as f:
            index = int(result_file.split("conversation_trace_")[1].split(".")[0])
            error_line = error_lines[index]
            result_timestamp = error_line.split()[1]

            total_num += 1
            result_line = find_result_line(f.readlines())
            if not result_line:
                continue
            outer_dict = ast.literal_eval(result_line)
            inner_json_string = outer_dict['content'].replace("\\\\", '')
            inner_dict = ast.literal_eval(inner_json_string)
            if 'function' in inner_dict:
                inner_dict = inner_dict['function']
            arguments_str = inner_dict['arguments']
            result = extract_specific_values(arguments_str)

            if result_timestamp and result_timestamp != 'None':
                nearest_row = find_nearest_groundtruth_row(result_timestamp, groundtruth)

                if nearest_row['level'] == 'pod' and 'pod' in result and nearest_row['cmdb_id'].lower() == result[
                    'pod'].lower():
                    hit_num += 1
                elif nearest_row['level'] == 'service' and 'service' in result and nearest_row['cmdb_id'].lower() == \
                        result[
                            'service'].lower():
                    hit_num += 1
                elif nearest_row['level'] == 'node' and 'node' in result and nearest_row['cmdb_id'].lower() == \
                        result[
                            'node'].lower():
                    hit_num += 1
                else:
                    print("-" * 20 + "Error" + "-" * 20)
                    print(f"Result:")
                    print(result)
                    print("Nearest groundtruth row:")
                    print(nearest_row)
print("Total Accuracy:", hit_num / total_num)