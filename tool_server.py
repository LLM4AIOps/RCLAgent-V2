import pickle
import sys

import pandas as pd
from flask import Flask, request, jsonify

pd.set_option('display.max_rows', None)  # 不限制最大行数
pd.set_option('display.max_columns', None)  # 不限制最大列数
pd.set_option('display.width', None)  # 不限制显示宽度
pd.set_option('display.max_colwidth', None)  # 不限制列宽

app = Flask(__name__)

sub_path = sys.argv[1]

# node-service
fr = open(f"data/{sub_path}/metric/node_service_map.pkl", "rb")
node_service_map = pickle.load(fr)
fr = open(f"data/{sub_path}/metric/service_node_map.pkl", "rb")
service_node_map = pickle.load(fr)
# trace
trace_df = pd.read_csv(f'data/{sub_path}/trace/all/trace_jaeger-span.csv')
# metrics
metric_df = pd.read_csv(f'data/{sub_path}/metric/all/metrics.csv')


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

    if len(timestamp_str) > 10:
        timestamp_str = timestamp_str[:10]
    timestamp = int(timestamp_str)


@app.route('/search_fluctuating_metrics', methods=['GET'])
def search_metrics():
    service_name = request.args.get('service_name')
    timestamp_str = request.args.get('timestamp')

    if len(timestamp_str) > 10:
        timestamp_str = timestamp_str[:10]
    timestamp = int(timestamp_str)

    if service_name in service_node_map:
        node_id = service_node_map[service_name]

        condition = (
                ((metric_df['service_name'].str.contains(service_name, case=False, na=False)) | (
                        metric_df['node_id'] == node_id & metric_df[
                    'service_name'] == "")) &
                (metric_df['timestamp'] >= timestamp - 600) &
                (metric_df['timestamp'] <= timestamp + 600)
        )
    else:
        condition = (
                (metric_df['service_name'].str.contains(service_name, case=False, na=False)) &
                (metric_df['timestamp'] >= timestamp - 600) &
                (metric_df['timestamp'] <= timestamp + 600)
        )

    result_rows = metric_df[condition]

    if not result_rows.empty:
        fluctuating_kpis = []

        for kpi, group in result_rows.groupby('kpi_name'):
            mean_value = group['value'].mean()
            std_dev_value = group['value'].std()

            if pd.notna(mean_value) and pd.notna(std_dev_value):
                threshold = 3 * std_dev_value

                is_fluctuating = (
                        (group['value'] < (mean_value - threshold)) |
                        (group['value'] > (mean_value + threshold))
                ).any()

                if is_fluctuating:
                    mid_index = len(group) // 2
                    start_index = mid_index // 2
                    end_index = start_index + mid_index

                    fluctuating_kpis.append(group.iloc[start_index:end_index])

        if fluctuating_kpis:
            # 合并所有有波动的KPI数据并返回
            fluctuating_data = pd.concat(fluctuating_kpis).to_dict(orient='records')
            return jsonify(fluctuating_data)
        else:
            return jsonify({"message": "No fluctuating metrics found."}), 200
    else:
        return jsonify({"message": "No matching records found."}), 200


if __name__ == '__main__':
    app.run(debug=False)
