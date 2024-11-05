# Telemetry ETL Pipeline Documentation

## Project Introduction:
The Telemetry ETL Pipeline is designed to process and transform raw telemetry data from various customer installations into an aggregated, analytics-ready dataset for business intelligence (BI) reporting. This enables Customer Success Managers and data analysts to:

1. Monitor customer interactions with the system.
2. Analyze patterns in system usage.
3. Provide actionable insights based on customer behavior.

## Project Setup:

### 1. Clone the Repository
Clone the project repository using:
git clone <repository-url>
cd <project-directory>

### 2. Build docker container
run the command **docker-compose up --build**

### 3. Verify Spark Cluster
After starting Docker, verify that the Spark master is accessible by going to:

http://localhost:8080


This URL should open the Spark UI where you can monitor jobs and worker status.

## Running the ETL Job:
### 4. Submit the Spark Job

/opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/main.py

## Testing the Pipeline:
### 5. Running Tests
 python -m unittest tests.py