#!/bin/bash

# Set variables
NETWORK_NAME="my-network"
POSTGRES_CONTAINER="postgres-db"
SCALARDB_PROPERTIES="$(pwd)/scalardb.properties"
SCHEMA_JSON="$(pwd)/schema.json"

# Step 1: Create a Docker network (if not exists)
docker network inspect $NETWORK_NAME >/dev/null 2>&1 || \
    docker network create $NETWORK_NAME

# Step 2: Start PostgreSQL container
docker run -d --name $POSTGRES_CONTAINER \
    --network $NETWORK_NAME \
    -e POSTGRES_USER=myuser \
    -e POSTGRES_PASSWORD=mypassword \
    -e POSTGRES_DB=mydatabase \
    -p 5432:5432 \
    postgres:16

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to start..."
sleep 10

# Step 3: Create 'test' schema
docker exec -i $POSTGRES_CONTAINER psql -U myuser -d mydatabase -c "CREATE SCHEMA IF NOT EXISTS test;"

# Step 4: Run ScalarDB Schema Loader
docker run --rm --network $NETWORK_NAME \
    -v "$SCHEMA_JSON:/schema.json" \
    -v "$SCALARDB_PROPERTIES:/scalardb.properties" \
    ghcr.io/scalar-labs/scalardb-schema-loader:3.15.2-SNAPSHOT \
    -f /schema.json --config /scalardb.properties --coordinator

# Step 5: Verify schema creation
docker exec -i $POSTGRES_CONTAINER psql -U myuser -d mydatabase -c "\dn"

echo "✅ Schema Loader execution completed."

