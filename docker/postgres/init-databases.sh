#!/bin/bash
# Create multiple databases and users for Airflow

set -e

# This script is run as the postgres user inside the PostgreSQL container
# Environment variables are passed by docker-compose

POSTGRES_CONN_PASSWORD="${POSTGRES_CONN_PASSWORD}"
METADATA_DATABASE_NAME="${METADATA_DATABASE_NAME}"
METADATA_DATABASE_USERNAME="${METADATA_DATABASE_USERNAME}"
METADATA_DATABASE_PASSWORD="${METADATA_DATABASE_PASSWORD}"
CELERY_BACKEND_NAME="${CELERY_BACKEND_NAME}"
CELERY_BACKEND_USERNAME="${CELERY_BACKEND_USERNAME}"
CELERY_BACKEND_PASSWORD="${CELERY_BACKEND_PASSWORD}"
ELT_DATABASE_NAME="${ELT_DATABASE_NAME}"
ELT_DATABASE_USERNAME="${ELT_DATABASE_USERNAME}"
ELT_DATABASE_PASSWORD="${ELT_DATABASE_PASSWORD}"

# Function to create database and user
create_db_and_user() {
  local db_name=$1
  local username=$2
  local password=$3
  
  echo "Creating database: $db_name with user: $username"
  
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    -- Create user if not exists
    DO \$\$ BEGIN
      CREATE USER "$username" WITH PASSWORD '$password';
    EXCEPTION WHEN DUPLICATE_OBJECT THEN
      ALTER USER "$username" WITH PASSWORD '$password';
    END
    \$\$;
    
    -- Create database
    CREATE DATABASE "$db_name" OWNER "$username";
    
    -- Grant privileges
    GRANT ALL PRIVILEGES ON DATABASE "$db_name" TO "$username";
EOSQL
}

# Create metadata database for Airflow
create_db_and_user "$METADATA_DATABASE_NAME" "$METADATA_DATABASE_USERNAME" "$METADATA_DATABASE_PASSWORD"

# Create celery backend database
create_db_and_user "$CELERY_BACKEND_NAME" "$CELERY_BACKEND_USERNAME" "$CELERY_BACKEND_PASSWORD"

# Create ELT database
create_db_and_user "$ELT_DATABASE_NAME" "$ELT_DATABASE_USERNAME" "$ELT_DATABASE_PASSWORD"

echo "All databases and users created successfully!"
