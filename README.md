# Telemetry ETL Pipeline Documentation

## Project Introduction:


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
