import json
import os
import pandas as pd
from datetime import datetime
from collections import Counter

import warnings

warnings.filterwarnings("ignore")

sub_path = "2022-03-24-cloudbed3"
result_sub_path = "result_claude2"
groundtruth = pd.read_csv(f"data/{sub_path}/groundtruth.csv")

groundtruth['timestamp'] = pd.to_datetime(groundtruth['timestamp'], unit='s')


def find_nearest_groundtruth_row(result_timestamp, groundtruth_df):
    if len(result_timestamp) > 10:
        result_timestamp = result_timestamp[:10]
    result_timestamp = datetime.utcfromtimestamp(int(result_timestamp))

    groundtruth_df['diff'] = abs(groundtruth_df['timestamp'] - result_timestamp)
    nearest_row = groundtruth_df.loc[groundtruth_df['diff'].idxmin()]
    del groundtruth_df['diff']

    return nearest_row


nearest_row_str_map = {}
groundtruth_results_map = {}
for result_file in os.listdir(f"data/{sub_path}/{result_sub_path}"):
    if result_file.endswith('.json'):
        with open(f"data/{sub_path}/{result_sub_path}/{result_file}", 'r') as f:
            result = json.load(f)

            result_timestamp = result.get('timestamp')

            if result_timestamp:
                nearest_row = find_nearest_groundtruth_row(result_timestamp, groundtruth)
                nearest_row_str = nearest_row['timestamp']
                nearest_row_str_map[nearest_row_str] = nearest_row
                if nearest_row_str not in groundtruth_results_map:
                    groundtruth_results_map[nearest_row_str] = [result]
                else:
                    groundtruth_results_map[nearest_row_str].append(result)


def evaluate_rank_n(row, service_rank, node_rank, pod_rank, rank_n):
    if row['level'] == 'pod':
        if row['cmdb_id'] in pod_rank[:rank_n]:
            return True
    elif row['level'] == 'service':
        if row['cmdb_id'] in service_rank[:rank_n]:
            return True
    elif row['level'] == 'node':
        if row['cmdb_id'] in node_rank[:rank_n]:
            return True
    return False


def calculate_mrr(groundtruth_row, service_rank, node_rank, pod_rank):
    level = groundtruth_row['level']
    cmdb_id = groundtruth_row['cmdb_id']
    rank = None

    if level == 'service':
        try:
            rank = service_rank.index(cmdb_id) + 1
        except ValueError:
            rank = float('inf')
    elif level == 'node':
        try:
            rank = node_rank.index(cmdb_id) + 1
        except ValueError:
            rank = float('inf')
    elif level == 'pod':
        try:
            rank = pod_rank.index(cmdb_id) + 1
        except ValueError:
            rank = float('inf')

    if rank == float('inf'):
        return 0.0
    else:
        return 1 / rank


hit_n = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
mrr_sum = 0
total_batches = 0

for groundtruth_row_str, results in groundtruth_results_map.items():
    groundtruth_row = nearest_row_str_map[groundtruth_row_str]

    for i in range(0, len(results), 10):  # 每10条结果作为一个窗口
        batch_results = results[i:i + 10]

        service_counter = Counter()
        node_counter = Counter()
        pod_counter = Counter()

        for result in batch_results:
            if 'service' in result:
                service_counter[result['service']] += 1
            if 'node' in result:
                node_counter[result['node']] += 1
            if 'pod' in result:
                pod_counter[result['pod']] += 1

        service_rank = [item for item, count in service_counter.most_common()]
        node_rank = [item for item, count in node_counter.most_common()]
        pod_rank = [item for item, count in pod_counter.most_common()]

        for n in hit_n.keys():
            if evaluate_rank_n(groundtruth_row, service_rank, node_rank, pod_rank, n):
                hit_n[n] += 1

        if not evaluate_rank_n(groundtruth_row, service_rank, node_rank, pod_rank, 5):
            print("=" * 20 + "Error" + "=" * 20)
            print(groundtruth_row)
            print("-" * 50)
            for result in batch_results:
                print(result)

        mrr_sum += calculate_mrr(groundtruth_row, service_rank, node_rank, pod_rank)
        total_batches += 1

for n in hit_n.keys():
    print(f"HR@{n}: {hit_n[n] / total_batches}")

mrr = mrr_sum / total_batches
print(f"MRR: {mrr:.4f}")
