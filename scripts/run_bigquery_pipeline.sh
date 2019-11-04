#!/bin/bash

source /Users/colin/Projects/venv/env2.7/bin/activate
cd /Users/colin/Projects/fram/fram/
python main.py uc-prox-core-prod bolt://localhost:7687 neo4j test /Users/colin/Projects/amundsen_bq_loader/loader/tmp
