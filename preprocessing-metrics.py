import os
import pickle

import pandas as pd

for sub_path in ["2022-03-20-cloudbed2", "2022-03-20-cloudbed3", "2022-03-21-cloudbed1", "2022-03-21-cloudbed2",
                 "2022-03-21-cloudbed3", "2022-03-24-cloudbed3"]:
    node_service_map = {}
    service_node_map = {}
    dataframes = []
    for metric_file in os.listdir(f'data/{sub_path}/metric/container'):
        print(metric_file)
        df = pd.read_csv(f'data/{sub_path}/metric/container/' + metric_file)
        dataframes.append(df)
    container_df = pd.concat(dataframes, ignore_index=True)
    result = []
    for _, row in container_df.iterrows():
        node_id = row['cmdb_id'].split('.')[0]
        service_name = row['cmdb_id'].split('.')[1]
        result.append({
            'timestamp': row['timestamp'],
            'node_id': node_id,
            'service_name': service_name,
            'kpi_name': row['kpi_name'],
            'value': row['value']
        })
        if node_id not in node_service_map:
            node_service_map[node_id] = {service_name}
        else:
            node_service_map[node_id].add(service_name)
        service_node_map[service_name] = node_id

    with open(f'data/{sub_path}/metric/node_service_map.pkl', 'wb') as node_service_file:
        pickle.dump(node_service_map, node_service_file)

    with open(f'data/{sub_path}/metric/service_node_map.pkl', 'wb') as service_node_file:
        pickle.dump(service_node_map, service_node_file)

    container_df = pd.DataFrame(result)

    dataframes = []
    for metric_file in os.listdir(f'data/{sub_path}/metric/node'):
        print(metric_file)
        df = pd.read_csv(f'data/{sub_path}/metric/node/' + metric_file)
        dataframes.append(df)
    node_df = pd.concat(dataframes, ignore_index=True)
    result = []
    for _, row in node_df.iterrows():
        result.append({
            'timestamp': row['timestamp'],
            'node_id': row['cmdb_id'],
            'service_name': '',
            'kpi_name': row['kpi_name'],
            'value': row['value']
        })
    node_df = pd.DataFrame(result)

    dataframes = []
    for metric_file in os.listdir(f'data/{sub_path}/metric/service'):
        print(metric_file)
        df = pd.read_csv(f'data/{sub_path}/metric/service/' + metric_file)
        dataframes.append(df)
    service_df = pd.concat(dataframes, ignore_index=True)
    kpi_names = ['rr', 'sr', 'mrt', 'count']
    result = []
    for _, row in service_df.iterrows():
        for kpi in kpi_names:
            result.append({
                'timestamp': row['timestamp'],
                'node_id': '',
                'service_name': row['service'],
                'kpi_name': kpi,
                'value': row[kpi]
            })
    service_df = pd.DataFrame(result)

    all_df = pd.concat([container_df, node_df, service_df], ignore_index=True)
    all_df.to_csv(f'data/{sub_path}/metric/all/metrics.csv', index=False)
