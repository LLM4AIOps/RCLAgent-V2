import ast
import json
import os
import pandas as pd
from datetime import datetime

import warnings

warnings.filterwarnings("ignore")

sub_path = "2022-03-20-cloudbed3"
result_sub_path = "result_claude"
groundtruth = pd.read_csv(f"data/{sub_path}/groundtruth.csv")

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


hit_num = 0
total_num = 0
# 遍历结果文件夹中的JSON文件
for result_file in os.listdir(f"data/{sub_path}/{result_sub_path}"):
    if result_file.endswith('.txt'):
        with open(f"data/{sub_path}/{result_sub_path}/{result_file}", 'r') as f:
            try:
                total_num += 1
                result_line = f.readlines()[-3]
                result = ast.literal_eval(ast.literal_eval(result_line)['content'])

                result_timestamp = result.get('timestamp')
                if type(result_timestamp) is not str:
                    result_timestamp = str(result_timestamp)

                if result_timestamp and result_timestamp != 'None':
                    nearest_row = find_nearest_groundtruth_row(result_timestamp, groundtruth)

                    if nearest_row['level'] == 'pod' and 'pod' in result and nearest_row['cmdb_id'].lower() == result[
                        'pod'].lower():
                        hit_num += 1
                    elif nearest_row['level'] == 'service' and 'service' in result and nearest_row['cmdb_id'].lower() == \
                            result[
                                'service'].lower():
                        hit_num += 1
                    elif nearest_row['level'] == 'node' and 'node' in result and nearest_row['cmdb_id'].lower() == result[
                        'node'].lower():
                        hit_num += 1
                    else:
                        print("-" * 20 + "Error" + "-" * 20)
                        print(f"Result:")
                        print(result)
                        print("Nearest groundtruth row:")
                        print(nearest_row)
            except Exception as e:
                print(result_line)
print("Total Accuracy:", hit_num / total_num)
