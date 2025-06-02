from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import col, to_date
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import broadcast
from dateutil.relativedelta import relativedelta

spark = SparkSession.builder.appName("structure_activity").getOrCreate()

# Read Data
spark.sparkContext.setJobGroup("DATA_READ", "Odczyt danych z plików Parquet")
spark.sparkContext.setJobDescription("Odczyt dim_date_df, dim_employee_df") 

fact_df = spark.read.parquet("abfss://raw@optimization0adls.dfs.core.windows.net/hr_fact_table/") \
    .select("date_id", "employee_id", "activity_type", "hours_worked")

dim_date = spark.read.parquet("abfss://raw@optimization0adls.dfs.core.windows.net/hr_dim_table/dim_date") \
    .select("date_id", "date", "month", "year")

dim_time_type = spark.read.parquet("abfss://raw@optimization0adls.dfs.core.windows.net/hr_dim_table/dim_time_type") \
    .select("time_type_id", "time_type")

# Set Time Border
spark.sparkContext.setJobGroup("TIME_BORDER", "Obliczenie granicy czasowej")
spark.sparkContext.setJobDescription("Ustalenie daty początkowej dla filtrowania danych")

today = datetime.today().replace(day=1)
start_date = (today - relativedelta(months=24)).strftime('%Y-%m-%d')

# Filter Data
spark.sparkContext.setJobGroup("FILTER_DATE", "Filtrowanie danych na podstawie granicy czasowej")
spark.sparkContext.setJobDescription("Przekształcenie i filtrowanie dim_date_df")

filtered_dim_date = dim_date \
    .withColumn("date", to_date("date", "yyyy-MM-dd")) \
    .filter(col("date") >= start_date)

# Join tables
spark.sparkContext.setJobGroup("JOIN_TABLES", "Łączenie wymaganych tabel")
spark.sparkContext.setJobDescription("Łączenie fact_df z dim_date, dim_date_time_type")

join_data = fact_df \
    .join(broadcast(filtered_dim_date), on="date_id", how="inner") \
    .join(broadcast(dim_time_type), fact_df["activity_type"] == dim_time_type["time_type_id"], "inner") \
    .select("date", "year", "month", "employee_id", "hours_worked", "time_type")

# Optional persist
# join_data.persist(StorageLevel.MEMORY_AND_DISK)

# Save to Silver Layer
spark.sparkContext.setJobGroup("SAVE_SILVER", "Zapis danych do warstwy Silver")
spark.sparkContext.setJobDescription("Zapis złączonych danych do folderu structure_activity")

join_data.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet("abfss://silver@optimization0adls.dfs.core.windows.net/structure_activity/")
