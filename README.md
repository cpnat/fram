## Data load scripts for Amundsen experiment

![](Amundsen-Fram.jpg)

1) Pip install requirements.txt

2) Run `/ingest/bigquery_ingest.py` with the following args:-  
    gcloud_project  
    neo4j_endpoint  
    neo4j_user   
    neo4j_password   
    temp_folder_path  
    
Also see https://github.com/lyft/amundsendatabuilder#amundsen-databuilder

Note, databuilder.extractor.bigquery_usage_extractor has a bug; and requires an exception handler to wrap refTables iteration (will submit a PR - line 107)
           

