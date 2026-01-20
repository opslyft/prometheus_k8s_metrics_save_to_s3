#!/bin/bash
# we are using uv hence we need to activate the env in the bash script first (comment this if activated locally)

source .venv/bin/activate

export PROM_ENDPOINTS='{
  "prod": "http://{your_host_name_here:port_if_needed}/api/v1/query_range",
  "stg": "http://{your_host_name_here:port_if_needed}/api/v1/query_range"
}'

echo "Running main.py..."
python main.py
