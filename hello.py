from prefect import flow
from prefect.logging import get_run_logger


@flow(log_prints=True)
def run_hello():
    logger = get_run_logger()
    logger.info(f"hello")
