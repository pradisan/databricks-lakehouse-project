from pyspark.sql.functions import current_timestamp, lit

# Step 1: Define source
DATA_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"

# Step 2: Read data
df = spark.read.parquet(DATA_URL)

# Step 3: Add audit columns
batch_id = "batch_001"
run_id = "run_001"

df = (
    df.withColumn("ingestion_timestamp", current_timestamp())
      .withColumn("batch_id", lit(batch_id))
      .withColumn("pipeline_run_id", lit(run_id))
      .withColumn("source_file", lit(DATA_URL))
      .withColumn("validation_status", lit("valid"))
)

# Step 4: Write to Bronze Delta table
df.write.format("delta") \
    .mode("append") \
    .saveAsTable("de_portfolio_dev.bronze.taxi_trips")
