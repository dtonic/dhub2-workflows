import importlib.util
import polars as pl
from prefect import flow
from prefect.tasks import Task
from prefect.states import Cancelled
from prefect.futures import wait
from prefect.logging import get_run_logger
from prefect import cache_policies

from stores.store import Store
from models.base import PipelineStep, Pipeline, Code, Checkpoint, Batch
from toolkits.delta import load_delta, load_delta_cdc, save_delta
from helpers.sessions import get_store_session
from helpers import misc, s3


def _load_script(store, step):
    object = store.get_object(step.script)
    script = object.decode("utf-8")
    return script


def _load_python_module(store, step):
    object = store.get_object(step.script)
    script = object.decode("utf-8")
    spec = importlib.util.spec_from_loader(step.name, loader=None)
    module = importlib.util.module_from_spec(spec)
    exec(script, module.__dict__)
    return module


def _run_step(store: Store, batch: str, step: PipelineStep):
    logger = get_run_logger()
    logger.info(f"step={step}")
    manifest = store.get_manifest(step.script)
    code = Code.model_validate_json(manifest)
    logger.info(f"code={code}")
    if step.options.get("read_mode", "cdc") == "cdc":
        try:
            checkpoint = store.get_checkpoint(f"{batch}.{step.name}")
        except:
            checkpoint = Checkpoint(state="none", offsets={})
        offsets = {}
        start_time = misc.make_ts()
        store.set_checkpoint(
            f"{batch}.{step.name}",
            Checkpoint(
                state="running",
                offsets=checkpoint.offsets,
                start_time=start_time,
                end_time=None,
            ),
        )
        logger.info(f"checkpoint={checkpoint}")
        if code.type.lower() == "sql":
            ctx = pl.SQLContext()
            for key, value in step.inputs.items():
                df, offsets[key] = load_delta_cdc(
                    s3.get_table_url(value),
                    checkpoint.offsets.get(key, 0),
                )
                logger.info(f"{key}={df}")
                ctx.register(key, df)
            try:
                query = _load_script(store, step)
                df = ctx.execute(query).collect()
                logger.info(f"outputs={df}")
            except Exception as e:
                store.set_checkpoint(
                    f"{batch}.{step.name}",
                    Checkpoint(
                        state="error",
                        offsets=checkpoint.offsets,
                        comment=str(e),
                        start_time=start_time,
                        end_time=misc.make_ts(),
                    ),
                )
                raise
            for key, value in step.outputs.items():
                save_delta(
                    s3.get_table_url(value),
                    df,
                    write_mode=step.options.get("write_mode", "append"),
                )
        else:
            args = {}
            for key, value in step.inputs.items():
                args[key], offsets[key] = load_delta_cdc(
                    s3.get_table_url(value),
                    checkpoint.offsets.get(key, 0),
                )
                logger.info(f"{key}={args[key]}")
            try:
                module = _load_python_module(store, step)
                outputs = module.run(**args, options=step.options)
                logger.info(f"outputs={outputs}")
                if outputs is not None:
                    for name, df in outputs.items():
                        save_delta(
                            s3.get_table_url(step.outputs[name]),
                            df,
                            write_mode=step.options.get("write_mode", "append"),
                        )
            except Exception as e:
                store.set_checkpoint(
                    f"{batch}.{step.name}",
                    Checkpoint(
                        state="error",
                        offsets=checkpoint.offsets,
                        comment=str(e),
                        start_time=start_time,
                        end_time=misc.make_ts(),
                    ),
                )
                raise
        logger.info(f"offsets={offsets}")
        store.set_checkpoint(
            f"{batch}.{step.name}",
            Checkpoint(
                state="ready",
                offsets=offsets,
                start_time=start_time,
                end_time=misc.make_ts(),
            ),
        )
    else:
        start_time = misc.make_ts()
        store.set_checkpoint(
            f"{batch}.{step.name}",
            Checkpoint(
                state="running",
                offsets={},
                start_time=start_time,
                end_time=None,
            ),
        )
        if code.type.lower() == "sql":
            ctx = pl.SQLContext()
            for key, value in step.inputs.items():
                id, version = _split_id_with_version(value)
                df = load_delta(s3.get_table_url(id), version)
                ctx.register(key, df)
                logger.info(f"{key}={df}")
            try:
                query = _load_script(store, step)
                df = ctx.execute(query).collect()
                logger.info(f"outputs={df}")
            except Exception as e:
                store.set_checkpoint(
                    f"{batch}.{step.name}",
                    Checkpoint(
                        state="error",
                        offsets={},
                        comment=str(e),
                        start_time=start_time,
                        end_time=misc.make_ts(),
                    ),
                )
                raise
            for key, value in step.outputs.items():
                save_delta(
                    s3.get_table_url(value),
                    df,
                    write_mode=step.options.get("write_mode", "append"),
                )
        else:
            args = {}
            for key, value in step.inputs.items():
                id, version = _split_id_with_version(value)
                args[key] = load_delta(s3.get_table_url(id), version)
                logger.info(f"{key}={args[key]}")
            try:
                module = _load_python_module(store, step)
                outputs = module.run(**args, options=step.options)
                logger.info(f"outputs={outputs}")
                if outputs is not None:
                    for name, df in outputs.items():
                        save_delta(
                            s3.get_table_url(step.outputs[name]),
                            df,
                            write_mode=step.options.get("write_mode", "append"),
                        )
            except Exception as e:
                store.set_checkpoint(
                    f"{batch}.{step.name}",
                    Checkpoint(
                        state="error",
                        offsets={},
                        comment=str(e),
                        start_time=start_time,
                        end_time=misc.make_ts(),
                    ),
                )
                raise
        store.set_checkpoint(
            f"{batch}.{step.name}",
            Checkpoint(
                state="ready",
                offsets={},
                start_time=start_time,
                end_time=misc.make_ts(),
            ),
        )


@flow(log_prints=True)
def run_pipeline(pipeline: Pipeline, batch: Batch):
    store = get_store_session()
    steps = pipeline.steps
    while steps:
        indices = []
        for index, step in enumerate(steps):
            if not _check_dependency(steps, step):
                indices.append(index)
        nexts = []
        for index in reversed(indices):
            step = steps.pop(index)
            nexts.append(step)
        try:
            futures = []
            for step in nexts:
                task = Task(
                    fn=_run_step,
                    name=step.name,
                    cache_policy=cache_policies.NO_CACHE,
                )
                futures.append(task.submit(store, batch.id, step))
            wait(futures)
        except Exception as e:
            return Cancelled(message=str(e))


def _check_dependency(steps, target):
    outputs = set(target.inputs.values())
    for step in steps:
        if not outputs.isdisjoint(step.outputs.values()):
            return True
    return False


def _split_id_with_version(id):
    tokens = id.split("@")
    if len(tokens) == 2:
        return tokens[0], int(tokens[1])
    return tokens[0], None
