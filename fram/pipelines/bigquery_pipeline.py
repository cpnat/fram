from fram.jobs.bigquery_job import create_bq_job
from fram.jobs.elasticsearch_job import create_es_publisher_job, create_es_user_job
from fram.jobs.last_updated_job import create_last_updated_job
from fram.jobs.types import MetadataType


class BigQueryPipeline:

    def __init__(self, gcloud_project, neo4j_endpoint, neo4j_user, neo4j_password, temp_folder_path):
        self.gcloud_project = gcloud_project
        self.neo4j_endpoint = neo4j_endpoint
        self.neo4j_user = neo4j_user
        self.neo4j_password = neo4j_password
        self.temp_folder_path = temp_folder_path

    def run(self):
        # extract table DSL, and load to Neo4j
        job1 = create_bq_job(gcloud_project=self.gcloud_project,
                             neo4j_endpoint=self.neo4j_endpoint,
                             neo4j_user=self.neo4j_user,
                             neo4j_password=self.neo4j_password,
                             temp_folder_path=self.temp_folder_path,
                             metadata_type=MetadataType.DSL)
        job1.launch()

        # extract usage data, and load to Neo4j
        job2 = create_bq_job(gcloud_project=self.gcloud_project,
                             neo4j_endpoint=self.neo4j_endpoint,
                             neo4j_user=self.neo4j_user,
                             neo4j_password=self.neo4j_password,
                             temp_folder_path=self.temp_folder_path,
                             metadata_type=MetadataType.USAGE)
        job2.launch()

        # publish table DSL to elasticsearch
        job3 = create_es_publisher_job(
            neo4j_endpoint=self.neo4j_endpoint,
            neo4j_user=self.neo4j_user,
            neo4j_password=self.neo4j_password,
            temp_folder_path=self.temp_folder_path,
            elasticsearch_index_alias='table_search_index',
            elasticsearch_doc_type_key='table',
            model_name='databuilder.models.table_elasticsearch_document.TableESDocument')
        job3.launch()

        # publish last updated
        job5 = create_last_updated_job(neo4j_endpoint=self.neo4j_endpoint,
                                       neo4j_user=self.neo4j_user,
                                       neo4j_password=self.neo4j_password)
        job5.launch()

        # publish users to elasticsearch
        job4 = create_es_user_job(
            neo4j_endpoint=self.neo4j_endpoint,
            neo4j_user=self.neo4j_user,
            neo4j_password=self.neo4j_password,
            temp_folder_path=self.temp_folder_path)
        job4.launch()
