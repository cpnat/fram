import argparse, sys
from fram.pipelines.bigquery_pipeline import BigQueryPipeline

if __name__ == "__main__":

    parser=argparse.ArgumentParser()
    parser.add_argument('--gcloud_project', help='GCP project from which to obtain Bigquery metadata')
    parser.add_argument('--neo4j_endpoint', help='Address of Neo4j HTTP enpoint')
    parser.add_argument('--neo4j_user', help='Neo4j username')
    parser.add_argument('--neo4j_password', help='Neo4j password')
    parser.add_argument('--temp_folder_path', help='Staging location for temporary files')
    args = parser.parse_args()

    pipeline = BigQueryPipeline(
        gcloud_project=args.gcloud_project,
        neo4j_endpoint=args.neo4j_endpoint,
        neo4j_user=args.neo4j_user,
        neo4j_password=args.neo4j_password,
        temp_folder_path=args.temp_folder_path
    )

    pipeline.run()
