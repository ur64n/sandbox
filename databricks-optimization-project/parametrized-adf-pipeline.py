import json
from pyspark.sql.utils import AnalysisException

# 1. ADF Parameters
dbutils.widgets.text("tableName", "")
dbutils.widgets.text("columns", "")
dbutils.widgets.text("filter", "") 

table_name = dbutils.widgets.get("tableName")
columns_raw = dbutils.widgets.get("columns")
filter_expr = dbutils.widgets.get("filter")

# 2. Column Parse
try:
    columns = json.loads(columns_raw) if columns_raw else []
except (json.JSONDecodeError, TypeError) as e:
    print(f"Błąd podczas parsowania 'columns': {e}")
    columns = []

# 3. Path to dim table
read_path = f"abfss://raw@optimization0adls.dfs.core.windows.net/hr_dim_table/{table_name}/"
write_path = f"abfss://silver@optimization0adls.dfs.core.windows.net/selected_dim_table_columns/{table_name}/"

# 4. Data Load with selected columns & filters
try:
    df = spark.read.parquet(read_path)

    if filter_expr:
        df = df.filter(filter_expr)

    if columns:
        df = df.select(*columns)

    # Save to silver container
    df.write.mode("overwrite").parquet(write_path)

    print(f"Zapisano dane do: {write_path}")

except AnalysisException as e:
    print(f"Błąd podczas wczytywania danych: {e}")
    raise
