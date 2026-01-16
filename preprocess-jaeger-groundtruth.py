import json
import os.path
import sys
from datetime import datetime

import pandas as pd

sub_path = sys.argv[1]
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


def construct_all_traces():
    error_file_path = f"data/{sub_path}/hipstershop.Frontend/Recv._durations.txt"
    fr = open(error_file_path, "r")
    lines = fr.readlines()
    for i in range(1, len(lines)):
        try:
            root_trace = lines[i]
            timestamp = root_trace.split()[1]
            nearest_row = find_nearest_groundtruth_row(timestamp, groundtruth)

            if not os.path.exists(f"data/{sub_path}/jarger_trace"):
                os.makedirs(f"data/{sub_path}/jarger_trace")
            trace_file_path = f"data/{sub_path}/jarger_trace/trace_{i}_label.json"
            with open(trace_file_path, 'w') as file:
                json_data = {"level": nearest_row['level'], "cmdb_id": nearest_row['cmdb_id']}
                file.write(json.dumps(json_data))
        except Exception as e:
            print(e)
            i -= 1


if __name__ == '__main__':
    construct_all_traces()
