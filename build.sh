#!/bin/bash
dir=$(pwd)

cd "$dir/build/"
docker build . --tag extending_airflow:latest

cd "$dir"
echo -e "AIRFLOW_UID=$(id -u)" > .env

docker-compose up airflow-init
docker-compose up -d