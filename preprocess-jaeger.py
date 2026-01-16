import json
import os.path
import sys

import pandas as pd

sub_path = sys.argv[1]
trace_df = pd.read_csv(f'data/{sub_path}/trace/all/trace_jaeger-span.csv')


def find_traces(spans, root_span):
    result_rows = trace_df[trace_df['parent_span'] == root_span["spanID"]]
    curr_level_spans = []
    for index, result_row in result_rows.iterrows():
        curr_level_spans.append(
            {
                "traceID": result_row["trace_id"],
                "spanID": result_row["span_id"],
                "operationName": result_row["operation_name"],
                "references": [
                    {
                        "refType": "CHILD_OF",
                        "traceID": root_span["traceID"],
                        "spanID": root_span["spanID"]
                    }
                ],
                "startTime": int(result_row["timestamp"]),
                "duration": int(result_row["duration"]),
                "processID": result_row["cmdb_id"],
                "warnings": None
            }
        )
    if len(curr_level_spans) > 0:
        spans.extend(curr_level_spans)
        for span in curr_level_spans:
            find_traces(spans, span)


def construct_all_traces():
    error_file_path = f"data/{sub_path}/hipstershop.Frontend/Recv._durations.txt"
    fr = open(error_file_path, "r")
    lines = fr.readlines()
    for i in range(1, len(lines)):
        print(f"Progressing: {i}/{len(lines)}")
        try:
            root_trace = lines[i]
            root_span = {
                "traceID": root_trace.split()[4],
                "spanID": root_trace.split()[3],
                "operationName": root_trace.split()[8],
                "references": [],
                "startTime": int(root_trace.split()[1]),
                "duration": int(root_trace.split()[5]),
                "processID": root_trace.split()[2],
                "warnings": None
            }
            spans = [root_span]
            find_traces(spans, root_span)

            if not os.path.exists(f"data/{sub_path}/jarger_trace"):
                os.makedirs(f"data/{sub_path}/jarger_trace")
            trace_file_path = f"data/{sub_path}/jarger_trace/trace_{i}.json"
            with open(trace_file_path, 'w') as file:
                processes = {}
                for span in spans:
                    service_name = span["processID"]
                    if service_name not in processes:
                        processes[service_name] = {
                            "serviceName": service_name,
                            "tags": []
                        }
                json_data = {"data": [{
                    "processes": processes,
                    "traceId": root_span["traceID"],
                    "spans": spans,
                }]}
                file.write(json.dumps(json_data))
        except Exception as e:
            print(e)
            i -= 1


if __name__ == '__main__':
    construct_all_traces()
