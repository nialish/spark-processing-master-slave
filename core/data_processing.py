from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date_format, to_timestamp, hour, count,lower,when

class DataProcessing:
    def __init__(self, spark: SparkSession):
        """
        Initialize the DataReader with an existing Spark session.

        Parameters:
            spark (SparkSession): An active Spark session.
        """
        self.spark = spark

    def clean_data(self,telemetry_df: DataFrame,installations_df: DataFrame,accounts_df: DataFrame):
        accounts_df = self.clean_accounts(accounts_df)
        installations_df = self.clean_installations(installations_df)
        telemetry_df = self.clean_installations(telemetry_df)

        return telemetry_df,installations_df,accounts_df
    
    def clean_accounts(self,accounts_df):
        # Step 1: Handle Duplicate IDs in Accounts
        # For duplicate accounts, we’ll keep the first occurrence and remove duplicates.
        accounts_df = accounts_df.dropDuplicates(["id"])

        # Step 2: Standardize the Casing of `type` Column
        # Convert `type` column to lowercase to ensure consistency
        accounts_df = accounts_df.withColumn("type", lower(col("type")))

        # Step 3: Standardize Column Names (Lowercase All Columns)
        # Rename all columns to lowercase for consistency
        accounts_df = accounts_df.select([col(c).alias(c.lower()) for c in accounts_df.columns])

        return accounts_df
    
    def clean_installations(self,installations_df):
        # Step 1: Standardize Column Names (Lowercase All Columns)
        # Rename all columns to lowercase for consistency
        installations_df = installations_df.select([col(c).alias(c.lower()) for c in installations_df.columns])
        return installations_df

    def clean_telemetry(self,telemetry_df):
        # Step 1: Standardize the `timestamp` Field to Proper Datetime Format
        # Convert `timestamp` from string format YYYYMMDDHHMMSS to Spark's TimestampType
        telemetry_df = telemetry_df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyyMMddHHmmss"))

        # Step 2: Remove Redundant Actions
        # Option 1: Deduplicate exact duplicate rows if redundancy is in raw records
        telemetry_df = telemetry_df.dropDuplicates()

        # Option 3: If redundancy is within short timeframes, aggregate counts of actions
        # Example: Count duplicate actions by `id_installation` and `action` within the same hour
        telemetry_df = telemetry_df.withColumn("hour", hour("timestamp"))
        telemetry_df = telemetry_df.groupBy("id_installation", "action", "hour").count()

        # Step 4: Standardize Column Names to Lowercase
        # Rename columns to lowercase to maintain consistency
        telemetry_df = telemetry_df.select([col(c).alias(c.lower()) for c in telemetry_df.columns])

        # Step 5: Clean and Standardize Action Field Values (if needed)
        # If `action` values are ambiguous, flag or replace them with standardized terms
        # Example: Replace vague actions with descriptive labels
        telemetry_df = telemetry_df.withColumn(
        "action",
         when(col("action") == "Data", "View Data")
        .when(col("action") == "Rules", "Manage Rules")
        .when(col("action") == "Check Rules", "Validate Rules")
        .otherwise(col("action"))
        )

        # Step 6: Save the Cleaned Data
        # Save cleaned DataFrame back to JSON format, now as an array of objects for structured output
        cleaned_telemetry_data = [row.asDict() for row in telemetry_df.collect()]

        return cleaned_telemetry_data

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