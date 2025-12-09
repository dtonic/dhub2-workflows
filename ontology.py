from neo4j import GraphDatabase
from prefect import flow
from prefect.logging import get_run_logger

from models.base import Checkpoint
from models.ontologies import Entity, EntitySink, Relation, RelationSink
from toolkits.delta import load_delta_cdc
from helpers.config import settings
from helpers.sessions import get_store_session
from helpers import misc, s3


@flow(log_prints=True)
def run_entity_sink(sink: EntitySink):
    logger = get_run_logger()
    logger.info(f"sink={sink}")
    store = get_store_session()
    driver = GraphDatabase.driver(
        settings.NEO4J_URL,
        auth=(settings.NEO4J_USERNAME, settings.NEO4J_PASSWORD),
    )
    neo4j = driver.session(database=settings.NEO4J_DATABASE)
    try:
        checkpoint = store.get_checkpoint(sink.id)
    except:
        checkpoint = Checkpoint(state="none", offsets={})
    offsets = {}
    start_time = misc.make_ts()
    logger.info(f"checkpoint={checkpoint}")
    df, offsets[sink.name] = load_delta_cdc(
        s3.get_table_url(sink.dataset),
        checkpoint.offsets.get(sink.name, 0),
    )
    if df.is_empty() is False:
        manifest = store.get_manifest(sink.entity)
        entity = Entity.model_validate_json(manifest)
        with neo4j.begin_transaction() as tx:
            for row in df.iter_rows(named=True):
                attributes = {}
                for source, target in sink.links.items():
                    attributes[source] = row[target]
                id = attributes.pop("id")
                tx.run(
                    "MERGE (e:{}{{id: $id}}) ON CREATE SET e += $attributes ON MATCH SET e += $attributes RETURN e".format(
                        entity.name
                    ),
                    id=id,
                    attributes=attributes,
                )
        logger.info(f"offsets={offsets}")
        store.set_checkpoint(
            sink.id,
            Checkpoint(
                state="ready",
                offsets=offsets,
                start_time=start_time,
                end_time=misc.make_ts(),
            ),
        )


@flow(log_prints=True)
def run_relation_sink(sink: RelationSink):
    logger = get_run_logger()
    logger.info(f"sink={sink}")
    store = get_store_session()
    driver = GraphDatabase.driver(
        settings.NEO4J_URL,
        auth=(settings.NEO4J_USERNAME, settings.NEO4J_PASSWORD),
    )
    neo4j = driver.session(database=settings.NEO4J_DATABASE)
    try:
        checkpoint = store.get_checkpoint(sink.id)
    except:
        checkpoint = Checkpoint(state="none", offsets={})
    offsets = {}
    start_time = misc.make_ts()
    logger.info(f"checkpoint={checkpoint}")
    df, offsets[sink.name] = load_delta_cdc(
        s3.get_table_url(sink.dataset),
        checkpoint.offsets.get(sink.name, 0),
    )
    if df.is_empty() is False:
        manifest = store.get_manifest(sink.relation)
        relation = Relation.model_validate_json(manifest)
        manifest = store.get_manifest(relation.source)
        source = Entity.model_validate_json(manifest)
        manifest = store.get_manifest(relation.target)
        target = Entity.model_validate_json(manifest)
        with neo4j.begin_transaction() as tx:
            for row in df.iter_rows(named=True):
                tx.run(
                    "MATCH (s:{}{{id: $source}}), (t:{}{{id: $target}}) MERGE (s)-[r:{}]->(t) ON CREATE SET r += $attributes ON MATCH SET r += $attributes RETURN r".format(
                        source.name,
                        target.name,
                        relation.name,
                    ),
                    source=row[sink.source],
                    target=row[sink.target],
                    attributes=sink.attributes,
                )
        logger.info(f"offsets={offsets}")
        store.set_checkpoint(
            sink.id,
            Checkpoint(
                state="ready",
                offsets=offsets,
                start_time=start_time,
                end_time=misc.make_ts(),
            ),
        )
