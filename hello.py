from prefect import flow


@flow(log_prints=True)
def run_hello():
    print("hello")
