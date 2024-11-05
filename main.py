from core.data_processing import DataProcessing
from core.data_reader import DataReader
from core.data_writer import DataWriter
from pyspark.sql import SparkSession
from utils.helper import Helper

# Initialize Spark session
spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.memory", "9g") \
    .config("spark.default.parallelism", "24") \
    .config("spark.sql.shuffle.partitions", "24") \
    .getOrCreate()

# Load Accounts Data
data_reader = DataReader(spark)

accounts_df = data_reader.read_parquet(path="/opt/spark-apps/data/meta/accounts.parquet")

# Load Installations Data
installations_df = data_reader.read_parquet(path="/opt/spark-apps/data/meta/installations.parquet")


# Define schema for telemetry JSON files
telemetry_schema = Helper().get_telemetry_schema()

# Load telemetry data
# telemetry_df = data_reader.read_json(path="/opt/spark-apps/data/telemetry/**/*.json",telemetry_schema=telemetry_schema)

# for bigger dataset
telemetry_df = data_reader.read_json(path="/opt/spark-apps/data/telemetry/biggerdataset/**/*.json",telemetry_schema=telemetry_schema)

telemetry_df,installations_df,accounts_df = DataProcessing(spark).clean_data(telemetry_df,installations_df,accounts_df)
aggregated_df = DataProcessing(spark).transform_data(telemetry_df,installations_df,accounts_df)
DataWriter(spark).write_parquet(aggregated_df)
