#!/bin/bash

# Find the names of the Airflow worker and webserver containers
worker_container=$(docker ps --format '{{.Names}}' | grep master-airflow_airflow-worker)
webserver_container=$(docker ps --format '{{.Names}}' | grep master-airflow_airflow-webserver)

if [ -n "$worker_container" ]; then
  # Restart the Airflow worker container to refresh the DAGs
  docker restart "$worker_container"
  echo "Airflow worker container has been restarted."
else
  echo "Airflow worker container not found. Make sure it is running."
fi

if [ -n "$webserver_container" ]; then
  # Restart the Airflow webserver container to refresh the DAGs
  docker restart "$webserver_container"
  echo "Airflow webserver container has been restarted."
else
  echo "Airflow webserver container not found. Make sure it is running."
fi
