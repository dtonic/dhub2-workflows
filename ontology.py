import os
import boto3
from neo4j import GraphDatabase
from prefect import flow

from models.ontologies import Entity, EntitySink, Relation, RelationSink
from toolkits.delta import load_delta_cdc
from helpers.config import Settings
from helpers import store


@flow(log_prints=True)
def run_entity_sink(settings: Settings, sink: EntitySink):
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
    driver = GraphDatabase.driver(
        settings.NEO4J_URL,
        auth=(settings.NEO4J_USERNAME, settings.NEO4J_PASSWORD),
    )
    neo4j = driver.session(database=settings.NEO4J_DATABASE)
    try:
        offset = store.get_checkpoint(settings, s3, os.getenv("SINK_ID"))
        offset = int(offset)
    except:
        offset = 0
    print(f"{sink.name} >> {offset}")
    table = store.get_table_url(settings, sink.dataset)
    df, offset = load_delta_cdc(settings, table, offset)
    if df.is_empty() is False:
        manifest = store.get_manifest(settings, s3, sink.entity)
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
        print(f"{sink.name} << {offset + 1}")
        store.set_checkpoint(
            settings,
            s3,
            os.getenv("SINK_ID"),
            str(offset + 1),
        )


@flow(log_prints=True)
def run_relation_sink(settings: Settings, sink: RelationSink):
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
    driver = GraphDatabase.driver(
        settings.NEO4J_URL,
        auth=(settings.NEO4J_USERNAME, settings.NEO4J_PASSWORD),
    )
    neo4j = driver.session(database=settings.NEO4J_DATABASE)
    try:
        offset = store.get_checkpoint(settings, s3, os.getenv("SINK_ID"))
        offset = int(offset)
    except:
        offset = 0
    print(f"{sink.name} >> {offset}")
    table = store.get_table_url(settings, sink.dataset)
    df, offset = load_delta_cdc(settings, table, offset)
    if df.is_empty() is False:
        manifest = store.get_manifest(settings, s3, sink.relation)
        relation = Relation.model_validate_json(manifest)
        manifest = store.get_manifest(settings, s3, relation.source)
        source = Entity.model_validate_json(manifest)
        manifest = store.get_manifest(settings, s3, relation.target)
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
        print(f"{sink.name} << {offset + 1}")
        store.set_checkpoint(
            settings,
            s3,
            os.getenv("SINK_ID"),
            str(offset + 1),
        )
