import os
import boto3
import importlib.util
import polars as pl
from prefect import flow
from prefect.tasks import Task
from prefect.states import Cancelled
from models.base import PipelineStep, Pipeline, Code
from toolkits.delta import load_delta, load_delta_cdc, save_delta
from helpers.config import Settings
from helpers.sessions import S3Session
from helpers import store


def _load_task(settings: Settings, s3: S3Session, step: PipelineStep):
    object = store.get_object(settings, s3, step.script)
    code = object.decode("utf-8")
    spec = importlib.util.spec_from_loader(step.name, loader=None)
    module = importlib.util.module_from_spec(spec)
    exec(code, module.__dict__)
    return Task(fn=module.run, name=step.name)


@flow(log_prints=True)
def run_pipeline(settings: Settings, pipeline: Pipeline):
    config = {
        "endpoint_url": settings.AWS_ENDPOINT_URL,
        "aws_access_key_id": settings.AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": settings.AWS_SECRET_ACCESS_KEY,
        "region_name": settings.AWS_REGION,
    }
    if settings.AWS_ENDPOINT_URL and settings.AWS_ENDPOINT_URL.startswith("https"):
        config["verify"] = True
    elif settings.AWS_ENDPOINT_URL and settings.AWS_ENDPOINT_URL.startswith("http://"):
        config["verify"] = False
    s3 = boto3.client("s3", **config)
    batch_id = os.getenv("BATCH_ID")
    for step in pipeline.steps:
        manifest = store.get_manifest(settings, s3, step.script)
        code = Code.model_validate_json(manifest)
        if step.options.get("read_mode", "cdc") == "cdc":
            try:
                offset = store.get_checkpoint(settings, s3, f"{batch_id}.{step.name}")
                offset = int(offset)
            except:
                offset = 0
            print(f"{step.name} >> {offset}")
            if code.type.lower() == "sql":
                object = store.get_object(settings, s3, step.script)
                query = object.decode("utf-8")
                ctx = pl.SQLContext()
                for key, value in step.inputs.items():
                    table = store.get_table_url(settings, value)
                    df, offset = load_delta_cdc(settings, table, offset)
                    ctx.register(key, df)
                try:
                    df = ctx.execute(query).collect()
                except Exception as e:
                    return Cancelled(message=str(e))
                for key, value in step.outputs.items():
                    table = store.get_table_url(settings, value)
                    save_delta(
                        settings,
                        table,
                        df,
                        write_mode=step.options.get("write_mode", "append"),
                    )
            else:
                args = {}
                for key, value in step.inputs.items():
                    table = store.get_table_url(settings, value)
                    args[key], offset = load_delta_cdc(settings, table, offset)
                task = _load_task(settings, s3, step)
                try:
                    print(f"{step.name}={args}")
                    outputs = task.submit(**args, options=step.options).result()
                    print(f"{step.name}={outputs}")
                    if outputs is not None:
                        for name, df in outputs.items():
                            table = store.get_table_url(settings, step.outputs[name])
                            save_delta(
                                settings,
                                table,
                                df,
                                write_mode=step.options.get("write_mode", "append"),
                            )
                except Exception as e:
                    return Cancelled(message=str(e))
            print(f"{step.name} << {offset + 1}")
            store.set_checkpoint(
                settings, s3, f"{batch_id}.{step.name}", str(offset + 1)
            )
        else:
            print(f"{step.name}")
            if code.type.lower() == "sql":
                object = store.get_object(settings, s3, step.script)
                query = object.decode("utf-8")
                ctx = pl.SQLContext()
                for key, value in step.inputs.items():
                    table = store.get_table_url(settings, value)
                    df = load_delta(settings, table)
                    ctx.register(key, df)
                try:
                    df = ctx.execute(query).collect()
                except Exception as e:
                    return Cancelled(message=str(e))
                for key, value in step.outputs.items():
                    table = store.get_table_url(settings, value)
                    save_delta(
                        settings,
                        table,
                        df,
                        write_mode=step.options.get("write_mode", "append"),
                    )
            else:
                args = {}
                for key, value in step.inputs.items():
                    table = store.get_table_url(settings, value)
                    args[key] = load_delta(table, settings)
                task = _load_task(settings, s3, step)
                try:
                    print(f"{step.name}={args}")
                    outputs = task.submit(**args, options=step.options).result()
                    print(f"{step.name}={outputs}")
                    if outputs is not None:
                        for name, df in outputs.items():
                            table = store.get_table_url(settings, step.outputs[name])
                            save_delta(
                                settings,
                                table,
                                df,
                                write_mode=step.options.get("write_mode", "append"),
                            )
                except Exception as e:
                    return Cancelled(message=str(e))
            print(f"{step.name}")
