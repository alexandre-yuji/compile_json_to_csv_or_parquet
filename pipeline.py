from etl import pipeline_calculate_kpi_sales_consolidate

path: str = 'data'
output_format: list = ["csv", "parquet"]

pipeline_calculate_kpi_sales_consolidate(path, output_format)

# poetry run python pipeline.py