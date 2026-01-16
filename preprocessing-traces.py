import os
import pandas as pd

pd.set_option('display.max_rows', None)  # 不限制最大行数
pd.set_option('display.max_columns', None)  # 不限制最大列数
pd.set_option('display.width', None)  # 不限制显示宽度
pd.set_option('display.max_colwidth', None)  # 不限制列宽

for sub_path in ["2022-03-20-cloudbed2", "2022-03-20-cloudbed3", "2022-03-21-cloudbed1", "2022-03-21-cloudbed2",
                 "2022-03-21-cloudbed3", "2022-03-24-cloudbed3"]:
    # 读取CSV文件
    df = pd.read_csv(f'data/{sub_path}/trace/all/trace_jaeger-span.csv')

    # 选择parent_span为空的行
    empty_span_id_rows = df[df['parent_span'].isna()]

    # 按operation_name分组
    grouped = empty_span_id_rows.groupby('operation_name')

    duration_threshold = 10000000


    # 确保目标目录存在
    def ensure_directory_exists(file_path):
        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)


    # 在生成文件路径时确保目录存在
    for name, group in grouped:
        print(f"Operation Name: {name}")

        # 检查status_code不为OK的条目
        status_code_errors = group[
            (group['status_code'] != 'ok') & (group['status_code'] != 0) & (group['status_code'] != '0') & (
                    group['status_code'] != 200) & (group['status_code'] != '200')]
        if not status_code_errors.empty:
            print("Status Code Errors:")
            print(status_code_errors)
            print("\n")

            # 将status_code为error的行写入单独的文件
            error_file_path = f'data/{sub_path}/{name}_errors.txt'
            ensure_directory_exists(error_file_path)
            with open(error_file_path, 'w') as file:
                file.write(f"{status_code_errors}")
            print(
                f"Error data for operation '{name}' has been written to {error_file_path}, error ratio: {len(status_code_errors) / len(group) * 100}%")

        # 将每个分组的duration列写入单独的文件
        duration_file_path = f'data/{sub_path}/{name}_durations.txt'
        ensure_directory_exists(duration_file_path)
        with open(duration_file_path, 'w') as file:
            error_durations = group[(group['duration'] > duration_threshold)]
            file.write(f"{error_durations}")
            print(
                f"Error duration data for operation '{name}' has been written to {duration_file_path}, error ratio: {len(error_durations) / len(group) * 100}%")

        print("\n" + "-" * 50 + "\n")
