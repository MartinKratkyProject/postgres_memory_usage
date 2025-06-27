#!/bin/bash

# Load environment variables from ../.env
ENV_FILE="$(dirname "$0")/../.env"

if [ ! -f "$ENV_FILE" ]; then
    echo "Error: .env file not found at $ENV_FILE"
    exit 1
fi

set -o allexport
source "$ENV_FILE"
set +o allexport

echo "Waiting for Airflow DB to be ready..."
airflow db upgrade

POSTGRES_URI="postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}"

if ! airflow connections get 'postgres_default' &>/dev/null; then
    echo "Creating Postgres connection..."
    airflow connections add 'postgres_default' \
        --conn-uri "$POSTGRES_URI"
else
    echo "Postgres connection already exists"
fi

if ! airflow connections get 'redis_default' &>/dev/null; then
    echo "Creating Redis connection..."
    airflow connections add 'redis_default' \
        --conn-uri 'redis://redis:6379'
else
    echo "Redis connection already exists"
fi

echo "Airflow connections ensured!"
echo "Setting Airflow Variables..."
echo "Airflow Variables set!"
