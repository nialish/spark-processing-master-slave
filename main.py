from core.data_processing import DataProcessing
from core.data_reader import DataReader
from core.data_writer import DataWriter
from pyspark.sql import SparkSession
from utils.helper import Helper

# Initialize Spark session
spark = SparkSession.builder \
    .appName("TelemetryETL") \
    .config("spark.default.parallelism", "80") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .getOrCreate()

# Load Accounts Data
data_reader = DataReader(spark)

accounts_df = data_reader.read_parquet(path="/opt/spark-apps/data/meta/accounts.parquet")

# Load Installations Data
installations_df = data_reader.read_parquet(path="/opt/spark-apps/data/meta/installations.parquet")


# Define schema for telemetry JSON files
telemetry_schema = Helper().get_telemetry_schema()

# Load telemetry data
telemetry_df = data_reader.read_json(path="/opt/spark-apps/data/telemetry/**/*.json",telemetry_schema=telemetry_schema)

aggregated_df = DataProcessing(spark).transform_data(telemetry_df,installations_df,accounts_df)
DataWriter(spark).write_parquet(aggregated_df)
# aggregated_df.write.parquet("/opt/spark-apps/output/processed_telemetry_data.parquet")
