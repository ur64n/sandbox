from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col
from pyspark import StorageLevel
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F
from pyspark.sql.functions import year, quarter 

sparksession = SparkSession.builder.appName("optimization").getOrCreate()

# Data read
fact_df = spark.read.parquet("abfss://raw@optimization0adls.dfs.core.windows.net/hr_fact_table/").select("employee_id","date_id","hours_worked","project_code")
dim_date_df = spark.read.parquet("abfss://raw@optimization0adls.dfs.core.windows.net/hr_dim_table/dim_date").select("date_id","date")
dim_employee_df = spark.read.parquet("abfss://raw@optimization0adls.dfs.core.windows.net/hr_dim_table/dim_employee").select("employee_id","department")

# Time border
today = datetime.today().replace(day=1)
start_date = (today - relativedelta(months=24)).strftime('%Y-%m-%d')

# Filter date based on time border
filtered_dim_date = dim_date_df.filter(F.col("date") >= F.lit(start_date))

# Join tables
joined_data = fact_df \
    .join(broadcast(filtered_dim_date), on="date_id", how="inner") \
    .join(broadcast(dim_employee_df), on="employee_id", how="inner") \

# Data Caching
cached_joined_data = joined_data.persist(StorageLevel.MEMORY_AND_DISK)

# Repartition data
repartitoned_joined_data = cached_joined_data.repartition(10, "employee_id")

# Add Quarter Column
quarter_col_df = repartitoned_joined_data.withColumn("year", year("date")).withColumn("quarter", quarter("date"))

# Aggregation based on department, year and quarter
aggregation_df = quarter_col_df.groupBy("department", "year", "quarter") \
    .agg(F.sum("hours_worked").alias("total_hours_worked"))


aggregation_df.show(10)
