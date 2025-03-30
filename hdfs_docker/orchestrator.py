#code updated to use schedule
import time
import datetime
import logging
import os
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
    """ Start core services: Spark Master, Spark Worker, HDFS, PostgreSQL """
    logging.info("Starting core services: Spark Master, Spark Worker, HDFS, PostgreSQL...")
    subprocess.run(["docker-compose", "-f", DOCKER_COMPOSE_PATH, "up", "-d"], check=True)



def run_ingestion():
    """ Runs the ingestion process inside the container. """
    logging.info("Triggering ingestion process...")
    subprocess.run(["docker-compose", "-f", DOCKER_COMPOSE_PATH, "up", "--no-deps", "--build", "-d", "spark-ingestion"], check=True)
    time.sleep(180)



def run_transformation():
    """ Runs the transformation process inside the container. """
    logging.info("Triggering transformation process...")
    subprocess.run(["docker-compose", "-f", DOCKER_COMPOSE_PATH, "up", "--no-deps", "--build", "-d", "spark-transformation"], check=True)

    # Wait a bit to allow first batch insertion into PostgreSQL
    logging.info("Waiting for transformation to insert the first batch...")
    time.sleep(120)  


def run_visualization():
    """ Runs the visualization dashboard. """
    logging.info("Starting visualization dashboard...")
    subprocess.Popen(["docker-compose", "-f", DOCKER_COMPOSE_PATH, "up", "--no-deps", "--build", "-d", "visualization"])



# Function to run the pipeline per batch
def run_pipeline(batch_number):
    logging.info(f"Starting data pipeline for Batch {batch_number}...")
    try:   
        # Step 1: Run Spark Ingestion for the current batch
        logging.info(f"Running ingestion for batch {batch_number}...")
        run_ingestion()

        #Step 2: Wait for the ingestion process to complete
        logging.info(f"Waiting for ingestion of batch {batch_number} to finish.")
        #wait_for_ingestion(batch_number)
        time.sleep(180)

        # Step 3: Run Spark Transformation for the same batch
        logging.info(f"Running transformation for batch {batch_number}...")
        run_transformation()

        #Step 4: Wait for the transformation process to complete
        logging.info(f"Waiting for transformation of batch {batch_number} to finish.")
        time.sleep(180)

        # Step 5: Run Dash for visualization
        logging.info("Running visualization")
        run_visualization()

        logging.info(f"Batch {batch_number} processing completed.")

    except KeyboardInterrupt:
        logging.info("Orchestrator stopped manually.")
    except Exception as e:
        logging.error(f"Unexpected error in orchestrator: {e}")


# Function to schedule batches to run each month
def run_monthly_batches():
    logging.info("Starting the batch processing pipeline...")

    total_batches = 10
    start_date = datetime.datetime.now()

    #Run Batch 1 immediately
    logging.info("Running Batch 1 immediately...")
    run_pipeline(batch_number=1)

    for batch in range(2, total_batches + 1):  # Start scheduling from Batch 2
        ''' Schedule the next batch exactly one month later'''
        next_run_date = start_date + relativedelta(months=batch - 1)
        next_run_time = next_run_date.strftime("%H:%M")  # Keep the same time each month

        """Manually check date & schedule"""
        schedule.every().day.at(next_run_time).do(check_and_run, batch, next_run_date)

        logging.info(f"Batch {batch} scheduled for {next_run_date.strftime('%Y-%m-%d %H:%M')}.")

    logging.info("Batch scheduling completed.")


#function to check date and run the batches
def check_and_run(batch_number, scheduled_date):
    """Ensure the batch runs on the correct scheduled date."""
    today = datetime.datetime.now().date()
    if today == scheduled_date.date():
        run_pipeline(batch_number)




def check_service_health(service_name):
    """ Check if a Docker service is running and healthy. """
    retries = 0
    while retries < MAX_RETRIES:
        try:
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
    """ Wait until Spark Master & HDFS are ready. """
    logging.info("Waiting for Spark Master and HDFS to become healthy...")

    spark_master_healthy = check_service_health("spark-master")
    hdfs_healthy = check_service_health("hdfs-namenode")  # Updated service name

    if not (spark_master_healthy and hdfs_healthy):
        logging.error("Critical services did not become healthy. Exiting orchestrator.")



# Run orchestrator       
if __name__ == "__main__":
    logging.info("Orchestrator started. Waiting for the first scheduled run...")
    # Start PostgreSQL, Spark Master, Spark Worker, and HDFS
    start_docker_services()
        
    # Ensure Spark & HDFS are healthy before starting ingestion
    wait_for_spark_hdfs()
    time.sleep(180)

    run_monthly_batches()  # Schedule all batches

    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every 60 seconds for scheduled jobs
