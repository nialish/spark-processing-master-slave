from pyspark.sql.types import StructType, StructField, StringType

class Helper:

    def get_telemetry_schema(self) -> StructType:
        """
        Function to create a StructType to ensure the schema of the telemetry data
        Parameters:
            No Parameters.

        Returns:
            StructType: A StructType which has 4 columns of telemetry dataset.
        """

        return StructType([
            StructField("id_installation", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("app", StringType(), True),
            StructField("action", StringType(), True)
        ])