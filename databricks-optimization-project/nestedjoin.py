from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import col, avg

sparksession = SparkSession.builder.appName("optimization").getOrCreate()

fact_df = spark.read.parquet("abfss://raw@optimization0adls.dfs.core.windows.net/hr_fact_table/year=2023/month=5")
dim_employee_df = spark.read.parquet("abfss://raw@optimization0adls.dfs.core.windows.net/hr_dim_table/dim_employee")
 dim_project_df = spark.read.parquet("abfss://raw@optimization0adls.dfs.core.windows.net/hr_dim_table/dim_project")
 dim_time_type_df = spark.read.parquet("abfss://raw@optimization0adls.dfs.core.windows.net/hr_dim_table/dim_time_type")
 dim_project_df = spark.read.parquet("abfss://raw@optimization0adls.dfs.core.windows.net/hr_dim_table/dim_project")
 dim_department_df = spark.read.parquet("abfss://raw@optimization0adls.dfs.core.windows.net/hr_dim_table/dim_department")
dim_date_df = spark.read.parquet("abfss://raw@optimization0adls.dfs.core.windows.net/hr_dim_table/dim_date")

# Złączenie broadcast wymiarów z faktem
fact_dim_broadcast_join = fact_df \
  .join(broadcast(dim_time_type_df), fact_df["activity_type"] == dim_time_type_df["time_type"], "inner") \
  .join(broadcast(dim_project_df), "project_id", "inner") \
  .join(broadcast(dim_department), "department_id","department_name" "inner")

# Złączenie broadcast wymiarów z faktem z użyciem select

 filter_dim_date = dim_date_df.filter((col("year") == 2023) & (col("month") == 5))

# fact_dim_broadcast_join_select = fact_df \
   .join(broadcast(filter_dim_date.select("date_id","month")), fact_df["date_id"] == filter_dim_date["date_id"], "inner") \
   .join(broadcast(dim_project_df.select("project_code", "project_name")), "project_code", "inner") \
   .join(broadcast(dim_employee_df.select("employee_id")), "employee_id", "inner") \
   .join(broadcast(dim_department_df.select("department_id","department_name")), "department_id","inner") \
   .join(broadcast(dim_time_type_df.select("time_type")), "time_type", "inner")

# Data Filter
filter_dim_date = dim_date_df.filter((col("year") == 2023) & (col("month") == 5))

# Join i aggregate
result_df = fact_df \
  .join(broadcast(filter_dim_date.select("date_id")), "date_id", "inner") \
  .join(broadcast(dim_employee_df.select("employee_id", "department")), "employee_id", "inner") \
  .groupBy("department", "activity_type") \
  .agg(avg("hours_worked").alias("avg_hours_worked"))

result_df = result_df.repartition(8)
result_df.show(20)

# Sprawdzenie liczby partycji
partition_size = result_df.rdd.getNumPartitions()
print(f"Liczba partycji: {partition_size}")

# Rozkład danych 
partition_distribution = result_df.rdd.glom().map(len).collect()
print(f"Rozkład danych: {partition_distribution}")

result_df.explain(extended=True)
