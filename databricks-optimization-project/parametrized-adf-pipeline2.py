import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import to_date, col

# 1. ADF parameters
dbutils.widgets.text("columns", "")
dbutils.widgets.text("filter", "")
dbutils.widgets.text("basePath", "")

columns_raw = dbutils.widgets.get("columns")
filter_expr = dbutils.widgets.get("filter")
base_folder = dbutils.widgets.get("basePath")

# 2. Columns parse
try:
    columns = json.loads(columns_raw) if columns_raw else []
except (json.JSONDecodeError, TypeError) as e:
    print(f"Błąd parsowania 'columns': {e}")
    columns = []

# 3. Paths
read_base_path = "abfss://raw@optimization0adls.dfs.core.windows.net/hr_fact_table/"
write_base_path = f"abfss://silver@optimization0adls.dfs.core.windows.net/{base_folder}/selected_fact_table/"

# 4. Time border past 2 years
start_date = datetime.today().replace(day=1) - relativedelta(months=24)
end_date = datetime.today().replace(day=1)

# 5. Generate partition paths
partition_paths = []
current = start_date
while current <= end_date:
    y = current.year
    m = current.month
    partition_paths.append(f"{read_base_path}year={y}/month={m}/")
    current += relativedelta(months=1)

print("Ścieżki do wczytania:")
for p in partition_paths:
    print(p)

# 6. Data Load & Transform
try:
    df = spark.read.parquet(*partition_paths)

    if filter_expr:
        df = df.filter(filter_expr)

    if columns:
        df = df.select(*columns)

    df.write.mode("overwrite").parquet(write_base_path)

    print(f"Dane zapisane do: {write_base_path}")

except AnalysisException as e:
    print(f"Błąd podczas przetwarzania: {e}")
    raise
