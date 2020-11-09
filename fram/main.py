import sys
import os
from fram.pipelines.bigquery_pipeline import BigQueryPipeline

if __name__ == "__main__":

    gcloud_project = sys.argv[1]
    neo4j_endpoint = sys.argv[2]
    neo4j_user = sys.argv[3]
    neo4j_password = sys.argv[4]
    temp_folder_path = sys.argv[5]

    pipeline = BigQueryPipeline(gcloud_project, neo4j_endpoint, neo4j_user, neo4j_password, temp_folder_path)
    pipeline.run()
