import logging
from pyhocon import ConfigFactory
import sqlite3

from databuilder.extractor.bigquery_metadata_extractor import BigQueryMetadataExtractor
from databuilder.extractor.bigquery_usage_extractor import BigQueryTableUsageExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask
from databuilder.transformer.base_transformer import NoopTransformer
from databuilder.transformer.bigquery_usage_transformer import BigqueryUsageTransformer

from fram.jobs.types import MetadataType

logging.basicConfig(level=logging.INFO)


def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Exception:
        logging.exception('exception')
    return None


def create_extractor(metadata_type):
    if metadata_type == MetadataType.DSL:
        extractor = BigQueryMetadataExtractor()
        extractor_key = 'extractor.bigquery_table_metadata.{}'.format(BigQueryMetadataExtractor.PROJECT_ID_KEY)
    elif metadata_type == MetadataType.USAGE:
        extractor = BigQueryTableUsageExtractor()
        extractor_key = 'extractor.bigquery_table_usage.{}'.format(BigQueryTableUsageExtractor.PROJECT_ID_KEY)
    else:
        raise ValueError('Invalid metadata_type')

    return extractor, extractor_key


def create_transformer(metadata_type):
    if metadata_type == MetadataType.DSL:
        return NoopTransformer()
    elif metadata_type == MetadataType.USAGE:
        return BigqueryUsageTransformer()
    else:
        raise ValueError('Invalid metadata_type')


def create_bq_job(gcloud_project, neo4j_endpoint, neo4j_user, neo4j_password, temp_folder_path, metadata_type):

    node_files_folder = '{temp_folder_path}/{metadata_type}/nodes'\
        .format(temp_folder_path=temp_folder_path, metadata_type=metadata_type)

    relationship_files_folder = '{temp_folder_path}/{metadata_type}/relationships'\
        .format(temp_folder_path=temp_folder_path, metadata_type=metadata_type)

    extractor, extractor_key = create_extractor(metadata_type=metadata_type)
    transformer = create_transformer(metadata_type=metadata_type)

    task = DefaultTask(extractor,
                       loader=FsNeo4jCSVLoader(),
                       transformer=transformer)

    job_config = ConfigFactory.from_dict({
        extractor_key:
            gcloud_project,
        'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.NODE_DIR_PATH):
            node_files_folder,
        'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.RELATION_DIR_PATH):
            relationship_files_folder,
        'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.SHOULD_DELETE_CREATED_DIR):
            True,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NODE_FILES_DIR):
            node_files_folder,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.RELATION_FILES_DIR):
            relationship_files_folder,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_END_POINT_KEY):
            neo4j_endpoint,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_USER):
            neo4j_user,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_PASSWORD):
            neo4j_password,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.JOB_PUBLISH_TAG):
            'unique_tag',  # should use unique tag here like {ds}
    })

    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=Neo4jCsvPublisher())

    return job
