import unittest
import sys
import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_format, hour
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from core.data_processing import DataProcessing

class TelemetryETLTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local").appName("TelemetryETLTest").getOrCreate()

        # Define sample data for accounts
        cls.accounts_data = [
            ("DAM", "Dummy Asset Management", "USA", "Test Drive 1, 12345 Testville, IL", "Customer"),
            ("FST", "Father & Son Trust", "UK", "Hollow Road 70, London", "Customer")
        ]
        cls.accounts_schema = StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("country", StringType(), True),
            StructField("address", StringType(), True),
            StructField("type", StringType(), True)
        ])
        
        # Define sample data for installations
        cls.installations_data = [
            ("DAM-2110", "DAM", "21.10", True),
            ("FST-2106", "FST", "21.06", True)
        ]
        cls.installations_schema = StructType([
            StructField("id", StringType(), True),
            StructField("acc", StringType(), True),
            StructField("version", StringType(), True),
            StructField("active", BooleanType(), True)
        ])

        # Define sample data for telemetry
        cls.telemetry_data = [
            ("DAM-2110", "20240602073210", "Asset Manager", "Calculate NAV")
        ]
        cls.telemetry_schema = StructType([
            StructField("id_installation", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("app", StringType(), True),
            StructField("action", StringType(), True)
        ])

        # Create DataFrames
        cls.accounts_df = cls.spark.createDataFrame(cls.accounts_data, schema=cls.accounts_schema)
        cls.installations_df = cls.spark.createDataFrame(cls.installations_data, schema=cls.installations_schema)
        cls.telemetry_df = cls.spark.createDataFrame(cls.telemetry_data, schema=cls.telemetry_schema)

        cls.telemetry_transformed = DataProcessing(cls.spark).convert_timestamp(cls.telemetry_df)
        cls.telemetry_with_meta = DataProcessing(cls.spark).merge_telemetry_accounts_installments_df(
            cls.telemetry_transformed,cls.installations_df,cls.accounts_df
        )
        cls.aggregated_df = DataProcessing(cls.spark).calcualte_action_count(cls.telemetry_with_meta)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_load_data(self):
        """ Test if data loads correctly with defined schemas """

        self.assertEqual(len(self.accounts_df.columns), 5)
        self.assertEqual(len(self.installations_df.columns), 4)
        self.assertEqual(len(self.telemetry_df.columns), 4)

    def test_timestamp_transformation(cls):
        """ Test if timestamp conversion and extraction of date and hour works """

        telemetry_transformed = DataProcessing(cls.spark).convert_timestamp(cls.telemetry_df)
        
        # Check if the transformation produced expected values
        row = telemetry_transformed.collect()[0]
        cls.assertEqual(row["date"], "2024-06-02")
        cls.assertEqual(row["hour"], 7)

    def test_joined_data(cls):
        """ Test if telemetry joins correctly with installations and accounts """

        telemetry_with_meta = DataProcessing(cls.spark).merge_telemetry_accounts_installments_df(
            cls.telemetry_df,cls.installations_df,cls.accounts_df
        )
        
        # Ensure the joined DataFrame has the expected columns
        expected_columns = ["id_installation", "timestamp", "app", "action", 
                            "id", "acc", "version", "active", 
                            "id", "name", "country", "address", "type"]
        cls.assertTrue(all([col in telemetry_with_meta.columns for col in expected_columns]))

    def test_aggregation(cls):
        """ Test if aggregation is correct with customer name, installation, version, and date """

        aggregated_df = DataProcessing(cls.spark).calcualte_action_count(cls.telemetry_with_meta)

        # Collect results to verify
        result = aggregated_df.collect()[0]
        cls.assertEqual(result["action_count"], 1)
        cls.assertEqual(result["customer_name"], "Dummy Asset Management")
        cls.assertEqual(result["version"], "21.10")
        cls.assertEqual(result["action"], "Calculate NAV")
        cls.assertEqual(result["app"], "Asset Manager")

    def test_output_schema(cls):
        """ Test if the final output schema matches the expected format """

        expected_columns = {"date", "hour", "action", "app", "id_installation", "customer_name", "version", "action_count"}
        cls.assertTrue(expected_columns.issubset(set(cls.aggregated_df.columns)))

if __name__ == "__main__":
    unittest.main()
