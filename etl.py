import pandas as pd
import os
import glob
from log import log_decorator
from timer_decorator import time_measure_decorator

# uma funcao de extract que le e consolida os json
@log_decorator
@time_measure_decorator
def extract_data_and_consolidate(path: str) -> pd.DataFrame:
    json_files = glob.glob(os.path.join(path, '*.json'))
    df_list = [pd.read_json(file) for file in json_files]
    df_total = pd.concat(df_list, ignore_index=True)
    return df_total

# uma funcao que transforma
@log_decorator
@time_measure_decorator
def calculate_kpi_total_sales(df: pd.DataFrame) -> pd.DataFrame:
    df["Total"] = df["Quantity"] * df["Sold"]
    return df

@log_decorator
@time_measure_decorator
def load_data(df: pd.DataFrame, format_saida: list):
    for file_type in format_saida:
        if file_type == 'csv':
            df.to_csv("kpi_consolidate.csv", index=False)
        if file_type == 'parquet':
            df.to_parquet("kpi_consolidate.parquet", index=False)

@log_decorator
@time_measure_decorator
def pipeline_calculate_kpi_sales_consolidate(path: str, output_format: list):
    data_frame = extract_data_and_consolidate(path)
    data_frame_calculated = calculate_kpi_total_sales(data_frame)
    load_data(data_frame_calculated, output_format)