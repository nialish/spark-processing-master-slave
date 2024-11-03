from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date_format, to_timestamp, hour, count

class DataProcessing:
    def __init__(self, spark: SparkSession):
        """
        Initialize the DataReader with an existing Spark session.

        Parameters:
            spark (SparkSession): An active Spark session.
        """
        self.spark = spark

    def transform_data(self,telemetry_df: DataFrame,installations_df: DataFrame,accounts_df: DataFrame):
        telemetry_df = self.convert_timestamp(telemetry_df)
        telemetry_with_meta_df = self.merge_telemetry_accounts_installments_df(telemetry_df,installations_df,accounts_df)
        aggregated_df = self.calcualte_action_count(telemetry_with_meta_df)
        return aggregated_df
    
    def convert_timestamp(self,telemetry_df: DataFrame) -> DataFrame:
        # Convert timestamp to proper timestamp type
        telemetry_df = telemetry_df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyyMMddHHmmss")) \
                           .withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd")) \
                           .withColumn("hour", hour(col("timestamp")))
        
        return telemetry_df
    
    def merge_telemetry_accounts_installments_df(self,telemetry_df: DataFrame,installations_df: DataFrame,accounts_df: DataFrame):
        telemetry_with_meta_df = telemetry_df \
            .join(installations_df, telemetry_df.id_installation == installations_df.id, "left") \
            .join(accounts_df, installations_df.acc == accounts_df.id, "left")
        
        return telemetry_with_meta_df
    
    def calcualte_action_count(self,telemetry_with_meta_df: DataFrame) -> DataFrame:
        # Aggregate data to get action counts per hour per application
        aggregated_df = telemetry_with_meta_df.groupBy(
            "date",                # For daily filtering
            "hour",                # Optional if hourly breakdown is needed
            "action",             # User action
            "app",                # Application used
            "id_installation",    # Installation identifier
            col("name").alias("customer_name"),  # Customer name (from accounts_df)
            "version"             # Installation version (from installations_df)
        ).agg(count("*").alias("action_count"))
        
        return aggregated_df