import json
import os
import pandas as pd
from datetime import datetime

import warnings

warnings.filterwarnings("ignore")

sub_path = "aiops2022/2022-03-20-cloudbed1"
result_sub_path = "result"
groundtruth = pd.read_csv(f"data/{sub_path}/groundtruth.csv")
error_file_path = f"data/{sub_path}/error_traces.txt"
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


r1_num = 0
r3_num = 0
r5_num = 0
r10_num = 0
total_num = 0
mrr_sum = 0.0
# 遍历结果文件夹中的JSON文件
for result_file in os.listdir(f"data/{sub_path}/{result_sub_path}"):
    if result_file.endswith('.txt'):
        with open(f"data/{sub_path}/{result_sub_path}/{result_file}", 'r') as f:
            index = int(result_file.split("conversation_trace_")[1].split(".")[0])
            error_line = error_lines[index]
            result_timestamp = error_line.split()[1]

            result = json.load(f)['root_causes']

            if result_timestamp:
                total_num += 1
                nearest_row = find_nearest_groundtruth_row(result_timestamp, groundtruth)
                label = nearest_row['cmdb_id'].lower()

                if label in result:
                    label_index = result.index(label)
                    if label_index < 1:
                        r1_num += 1
                    if label_index < 3:
                        r3_num += 1
                    if label_index < 5:
                        r5_num += 1
                    if label_index < 10:
                        r10_num += 1
                    # 计算MRR
                    reciprocal_rank = 1.0 / (label_index + 1)
                    mrr_sum += reciprocal_rank
                else:
                    print("-" * 20 + "Error" + "-" * 20)
                    print(f"Result:")
                    print(result)
                    print("Nearest groundtruth row:")
                    print(nearest_row)
print("R1 Accuracy:", r1_num / total_num)
print("R3 Accuracy:", r3_num / total_num)
print("R5 Accuracy:", r5_num / total_num)
print("R10 Accuracy:", r10_num / total_num)
print("MRR:", mrr_sum / total_num if total_num else 0.0)
