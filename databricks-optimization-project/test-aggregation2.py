from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, to_date, year, quarter
from pyspark import StorageLevel
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("optimization").getOrCreate()

# Data read
spark.sparkContext.setJobGroup("DATA_READ", "Odczyt danych z plików Parquet")
spark.sparkContext.setJobDescription("Odczyt fact_df, dim_date_df, dim_employee_df, dim_project_df") 

fact_df = spark.read.parquet("abfss://raw@optimization0adls.dfs.core.windows.net/hr_fact_table/") \
    .select("employee_id", "date_id", "hours_worked", "project_code")

dim_date_df = spark.read.parquet("abfss://raw@optimization0adls.dfs.core.windows.net/hr_dim_table/dim_date") \
    .select("date_id", "date")

dim_employee_df = spark.read.parquet("abfss://raw@optimization0adls.dfs.core.windows.net/hr_dim_table/dim_employee") \
    .select("employee_id", "department")

dim_project_df = spark.read.parquet("abfss://raw@optimization0adls.dfs.core.windows.net/hr_dim_table/dim_project") \
    .select("project_code", "project_name")

# Time border
spark.sparkContext.setJobGroup("TIME_BORDER", "Obliczenie granicy czasowej")
spark.sparkContext.setJobDescription("Ustalenie daty początkowej dla filtrowania danych")

today = datetime.today().replace(day=1)
start_date = (today - relativedelta(months=24)).strftime('%Y-%m-%d')

# Filter date based on time border
spark.sparkContext.setJobGroup("FILTER_DATE", "Filtrowanie danych na podstawie granicy czasowej")
spark.sparkContext.setJobDescription("Przekształcenie i filtrowanie dim_date_df")

filtered_dim_date = dim_date_df \
    .withColumn("date", to_date("date", "yyyy-MM-dd")) \
    .filter(col("date") >= F.lit(start_date))

# Join tables
spark.sparkContext.setJobGroup("JOIN_TABLES", "Łączenie tabel fact i dim")
spark.sparkContext.setJobDescription("Łączenie fact_df z filtered_dim_date, dim_employee_df i dim_project_df")

joined_data = fact_df \
    .join(broadcast(filtered_dim_date), on="date_id", how="inner") \
    .join(broadcast(dim_employee_df), on="employee_id", how="inner") \
    .join(broadcast(dim_project_df), on="project_code", how="inner")

# Add year and quarter columns (przed cache)
enriched_df = joined_data \
    .withColumn("year", year("date")) \
    .withColumn("quarter", quarter("date"))

# Repartition by project_name (ważniejsze dla dalszej agregacji niż employee_id)
partitioned_df = enriched_df.repartition(50, "project_name") # Zmienic repartition i sprawdzic Shuffle Write w zakładce Stages dotyczącej agregacji.

# Cache
cached_df = partitioned_df.persist(StorageLevel.MEMORY_AND_DISK)

joined_data.explain(extended=True)

# Aggregation 1
print("Rozpoczynam agregację godzin pracy...")

spark.sparkContext.setJobGroup("AGGREGATION", "Agregacja danych godzin pracy")
spark.sparkContext.setJobDescription("Sumowanie godzin pracy wg działu i kwartału")

aggregation_df = cached_df.groupBy("department", "year", "quarter") \
    .agg(F.sum("hours_worked").alias("total_hours_worked"))
aggregation_df.show(10)

print("Agregacja godzin pracy zakończona.")

# Aggregation 2
#print("Rozpoczynam agregację obliczenia średniej godzin pracy...")
#spark.sparkContext.setJobGroup("Agregacja2", "Średnie przepracowane godziny wg działu i projektu")
#avg_hours_df = cached_df.groupBy("department", "project_name") \
#    .agg(F.avg("hours_worked").alias("avg_hours_worked"))
#avg_hours_df.show(10)
#print("Agregacja obliczenia średniej godzin pracy zakończona.")

# Aggregation 3
#print("Rozpoczynam agregację obliczenia nadgodzin pracy...")
#spark.sparkContext.setJobGroup("Agregacja3", "Filtracja dni przeciążenia pracownika")
#overload_df = cached_df.groupBy("employee_id", "department", "project_name", "date") \
#    .agg(F.sum("hours_worked").alias("daily_hours")) \
#    .filter(col("daily_hours") > 10)
#overload_df.show(10)
#print("Agregacja obliczenia nadgodzin pracy zakończona.")
