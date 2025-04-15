""" The orchestration script is used to start the pipeline, build containers and schedule the batch processing intervals"""

#import needed packages
import time
import datetime
import logging
import os
import sys
import subprocess
import schedule
from dateutil.relativedelta import relativedelta



# Path to docker-compose.yml
DOCKER_COMPOSE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "docker-compose.yml"))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("orchestrator.log"),
        logging.StreamHandler()
    ]
)

logging.info(f"Using docker-compose file at: {DOCKER_COMPOSE_PATH}")

# Health check retry settings
MAX_RETRIES = 5
RETRY_DELAY = 10  # Seconds


def start_docker_services():
    """ Start core services: Spark Master, Spark Worker, HDFS, PostgreSQL 
        Added a project name (-p) to ensure the entire pipeline can be shut down properly.
        Without specifying a project name, only services directly started using the same docker-compose.yml 
        are recognized and can be shut down using docker-compose down.
    """
    logging.info("Starting core services: Spark Master, Spark Worker, HDFS, PostgreSQL...")
    subprocess.run(["docker-compose", "-f", DOCKER_COMPOSE_PATH,
                    "-p", "fraud_transaction_pipeline",  # The project name
                      "up", "-d"], check=True)




def check_service_health(service_name):
    """ Check if a Docker service is running and healthy. """
    retries = 0
    while retries < MAX_RETRIES:
        try:
            #inspect the service state
            result = subprocess.run(
                ["docker", "inspect", "--format", "{{.State.Status}}", service_name],
                capture_output=True, text=True
            )
            status = result.stdout.strip().lower()

            # Log the output for debugging
            logging.info(f"{service_name} status: {status}")

            if status in ["running", "healthy"]:
                logging.info(f"{service_name} is healthy!")
                return True
            else:
                logging.warning(f"{service_name} is not healthy yet. Retrying... ({retries+1}/{MAX_RETRIES})")
        except Exception as e:
            logging.error(f"Error checking {service_name} health: {e}")

        retries += 1
        time.sleep(RETRY_DELAY)

    logging.error(f"{service_name} did not become healthy after {MAX_RETRIES} retries. Exiting.")
    return False



def wait_for_spark_hdfs():
    try:

        """ Wait until Spark Master & HDFS are ready. """
        logging.info("Waiting for Spark Master, HDFS and PostgrSQL to become healthy...")

        spark_master_healthy = check_service_health("spark-master")
        spark_worker_healthy = check_service_health("spark-worker")
        hdfs_namenode_healthy = check_service_health("hdfs-namenode")  
        hdfs_datanode_healthy = check_service_health("hdfs-datanode")
        postgres_healthy = check_service_health("postgres-db")

        if not (spark_master_healthy and spark_worker_healthy 
                and hdfs_namenode_healthy and hdfs_datanode_healthy and postgres_healthy):
            logging.error("One or more critical services are unhealthy. Stopping orchestration.")
            sys.exit(1)

    except Exception as e:
        logging.error(f"An error occurred during health checks: {e}")
        sys.exit(1)

    else:
        logging.info("All critical services are healthy. Continuing orchestration.")
        




def run_container(container_name, batch_number=None, wait_time=120):
    """Build and run the containers using docker-compose."""
    logging.info(f"Triggering {container_name} container...")

    # Prepare environment and update the .env file with the correct batch_number 
    env = os.environ.copy()
    if batch_number:
        env["BATCH_NUMBER"] = str(batch_number)

    try:
        # Build and run the container while attaching the updated .env file(without dependencies)
        subprocess.run(
            ["docker-compose", "-f", DOCKER_COMPOSE_PATH, 
             "-p", "fraud_transaction_pipeline",  # the project name to be used when stopping the pipeline
             "up", "--no-deps", "--build",  "-d", container_name],  
            check=True,
            env=env
        )
        logging.info(f"{container_name} container started successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error starting {container_name} container: {e}")
        raise

    time.sleep(wait_time)




def run_pipeline(batch_number):
    """ Run the containers one after the other
     NB: The time.sleep() can be adjusted based on the size of the batches 
         to ensure each service finishes its job before the next service begins
    """
    logging.info(f"Starting data pipeline for Batch {batch_number}...")
    try:
        # Step 1: Run Spark Ingestion for the current batch
        logging.info(f"Running ingestion for batch {batch_number}...")
        run_container("spark-ingestion", batch_number, wait_time=120)

        # Step 2: Wait for ingestion process to complete
        logging.info(f"Waiting for ingestion of batch {batch_number} to finish.")
        time.sleep(180) 

        # Step 3: Run Spark Transformation for the same batch
        logging.info(f"Running transformation for batch {batch_number}...")
        run_container("spark-transformation",batch_number, wait_time =120)

        # Step 4: Wait for transformation process to complete
        logging.info(f"Waiting for transformation of batch {batch_number} to finish.")
        time.sleep(180)  # Adjust if needed

        # Step 5: Run Dash for visualization
        logging.info(f"Running visualization for batch {batch_number}...")
        run_container("visualization", batch_number = None, wait_time = 120)

        logging.info(f"Batch {batch_number} processing completed.")

    except Exception as e:
        logging.error(f"Error occurred during batch processing: {e}")
        sys.exit(1)



def check_and_run(batch_number, scheduled_date):
    """Ensure the batch runs on the correct scheduled date."""
    today = datetime.datetime.now().date()
    if today == scheduled_date.date():
        logging.info(f"Today is the scheduled date for Batch {batch_number}. Running the pipeline.")
        run_pipeline(batch_number)
    else:
        logging.info(f"Batch {batch_number} is not scheduled for today.")


def run_monthly_batches():
    """ Schedules and Runs the data pipeline"""
    logging.info("Starting the batch processing pipeline...")

    total_batches = 12
    start_date = datetime.datetime.now()

    # Run Batch 1 immediately
    logging.info("Running Batch 1 immediately...")
    run_pipeline(batch_number=1)

    for batch in range(2, total_batches + 1):  # Start scheduling from Batch 2
        # Schedule the next batch exactly one month later
        next_run_date = start_date + relativedelta(months=batch - 1)
        next_run_time = next_run_date.strftime("%H:%M")  # Keep the same time each month

        # Manually check date & schedule
        schedule.every().day.at(next_run_time).do(check_and_run, batch, next_run_date)

        logging.info(f"Batch {batch} scheduled for {next_run_date.strftime('%Y-%m-%d %H:%M')}.")

    logging.info("Batch scheduling completed.")



if __name__ == "__main__":
    logging.info("Orchestrator started. Waiting for the first scheduled run...")
    start_docker_services()  # Start Docker services (e.g., Spark, HDFS)
    
    wait_for_spark_hdfs()  # Ensure critical services are healthy before starting ingestion
    time.sleep(70)

    run_monthly_batches()  # Schedule and Run the batches one after the other.

    while True:
        schedule.run_pending()  # Execute scheduled tasks
        time.sleep(60)  # Wait for the next schedule check