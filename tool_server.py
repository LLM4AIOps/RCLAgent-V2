import os
import pickle
import sys

import pandas as pd
from flask import Flask, request, jsonify

pd.set_option('display.max_rows', None)  # 不限制最大行数
pd.set_option('display.max_columns', None)  # 不限制最大列数
pd.set_option('display.width', None)  # 不限制显示宽度
pd.set_option('display.max_colwidth', None)  # 不限制列宽

app = Flask(__name__)

def load_log_df():
    """从 sample_data/log/all 目录加载所有日志文件并合并为 DataFrame"""
    base_dir = f"sample_data/log/all"
    if not os.path.exists(base_dir):
        raise FileNotFoundError(f"Directory not found: {base_dir}")

    files = [
        os.path.join(base_dir, f)
        for f in os.listdir(base_dir)
        if f.endswith('.csv')
    ]

    if not files:
        return pd.DataFrame(columns=['log_id', 'timestamp', 'cmdb_id', 'log_name', 'value'])

    dfs = []
    for file in files:
        try:
            df = pd.read_csv(file)
            if 'value' in df.columns:
                mask = ~df['value'].astype(str).str.lower().str.contains('info|debug', na=False)
                df = df[mask]
            dfs.append(df)
        except Exception as e:
            continue

    if not dfs:
        return pd.DataFrame(columns=['log_id', 'timestamp', 'cmdb_id', 'log_name', 'value'])

    combined_df = pd.concat(dfs, ignore_index=True)
    return combined_df


# node-service
fr = open(f"sample_data/metric/node_service_map.pkl", "rb")
node_service_map = pickle.load(fr)
fr = open(f"sample_data/metric/service_node_map.pkl", "rb")
service_node_map = pickle.load(fr)
# trace
trace_df = pd.read_csv(f'sample_data/trace/all/trace_jaeger-span.csv')
# metrics
metric_df = pd.read_csv(f'sample_data/metric/all/metrics.csv')
log_df = load_log_df()


@app.route('/search_span', methods=['GET'])
def search_span():
    span_id = request.args.get('span_id')
    if span_id is None:
        return jsonify({"error": "span_id 参数缺失"}), 400

    # 查找以该span_id为parent_span的行
    result_rows = trace_df[trace_df['span_id'] == span_id]

    if not result_rows.empty:
        return jsonify(result_rows.to_dict(orient='records'))
    else:
        return jsonify({"message": "No trace with parent_span = '{span_id}' 。"}), 200


@app.route('/search_traces', methods=['GET'])
def search_traces():
    span_id = request.args.get('parent_span_id')
    if span_id is None:
        return jsonify({"error": "span_id 参数缺失"}), 400

    # 查找以该span_id为parent_span的行
    result_rows = trace_df[trace_df['parent_span'] == span_id]

    if not result_rows.empty:
        return jsonify(result_rows.to_dict(orient='records'))
    else:
        return jsonify({"message": "No trace with parent_span = '{span_id}' 。"}), 200


@app.route('/search_logs', methods=['GET'])
def search_logs():
    service_name = request.args.get('service_name')
    timestamp_str = request.args.get('timestamp')

    # 参数校验
    if not service_name:
        return jsonify({"error": "Missing required parameter: service_name"}), 400
    if not timestamp_str:
        return jsonify({"error": "Missing required parameter: timestamp"}), 400

    try:
        # 截断并转换时间戳
        if len(timestamp_str) > 10:
            timestamp_str = timestamp_str[:10]
        timestamp = int(timestamp_str)
    except ValueError:
        return jsonify({"error": "Invalid timestamp format"}), 400

    # 计算时间范围（正负60秒）
    start_time = timestamp - 60
    end_time = timestamp + 60

    try:
        # 使用 Pandas 查询符合条件的日志
        filtered_df = log_df[
            (log_df['cmdb_id'].str.contains(service_name, na=False)) &
            (log_df['timestamp'] >= start_time) &
            (log_df['timestamp'] <= end_time)
            ]

        result = filtered_df.to_csv(index=False)
        print(result)
        return result
    except Exception as e:
        return jsonify({"error": "Internal server error", "details": str(e)}), 500


@app.route('/search_fluctuating_metrics', methods=['GET'])
def search_metrics():
    service_name = request.args.get('service_name')
    timestamp_str = request.args.get('timestamp')

    if len(timestamp_str) > 10:
        timestamp_str = timestamp_str[:10]
    timestamp = int(timestamp_str)

    node_id = None
    if service_name in service_node_map:
        node_id = service_node_map[service_name]

        condition = (
                (
                        (metric_df['service_name'].str.contains(service_name, case=False, na=False)) |
                        (
                                (metric_df['node_id'] == node_id) &
                                (metric_df['service_name'] == "")
                        )
                ) &
                (metric_df['timestamp'] >= timestamp - 1200) &
                (metric_df['timestamp'] <= timestamp + 1200)
        )
    else:
        condition = (
                (metric_df['service_name'].str.contains(service_name, case=False, na=False)) &
                (metric_df['timestamp'] >= timestamp - 1200) &
                (metric_df['timestamp'] <= timestamp + 1200)
        )

    result_rows = metric_df[condition]

    if not result_rows.empty:
        kpi_dict = dict()

        for (kpi, node_id, service_name), group in result_rows.groupby(['kpi_name', 'node_id', 'service_name']):
            mean_value = group['value'].mean()
            std_dev_value = group['value'].std()
            group = group[(group['timestamp'] >= timestamp - 600) & (group['timestamp'] <= timestamp + 600)]

            if pd.notna(mean_value) and pd.notna(std_dev_value):
                threshold = 3 * std_dev_value

                is_fluctuating = (
                        (group['value'] < (mean_value - threshold)) |
                        (group['value'] > (mean_value + threshold))
                ).any()

                if is_fluctuating:
                    # 初始化 key 为 kpi_name
                    key = kpi  # 直接使用 kpi，因为 groupby 的 kpi_name 是唯一的

                    # 如果 service_name 存在且不是 NaN，则添加到 key
                    if not pd.isna(service_name):
                        key = f"{service_name}.{key}"

                    # 如果 node_id 存在且不是 NaN，则添加到 key
                    if not pd.isna(node_id):
                        key = f"{node_id}.{key}"

                    # 将最终的 key 添加到集合中
                    kpi_dict[key] = {
                        "regular_mean": round(mean_value, 2),
                        "regular_std_dev": round(std_dev_value, 2),
                        "current_mean": round(group['value'].mean(), 2),
                        "current_std_dev": round(group['value'].std(), 2),
                    }

        if len(kpi_dict) > 0:
            table = []
            header = ['key', 'regular_mean', 'regular_std_dev', 'current_mean', 'current_std_dev']
            table.append(header)

            for key, values in kpi_dict.items():
                row = [key] + list(values.values())
                table.append(row)

            # 转换为 pandas DataFrame 以便于处理
            df = pd.DataFrame(table[1:], columns=table[0])

            # 打印为 CSV 格式
            print(df.to_csv(index=False))
            return df.to_csv(index=False)
        else:
            return jsonify({"message": "No fluctuating metrics found."}), 200
    else:
        return jsonify({"message": "No matching records found."}), 200


if __name__ == '__main__':
    app.run(debug=False)
