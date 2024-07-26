#!/bin/bash
set -ue

# Get container name and password from arguments
SQL_SERVER_CONTAINER_NAME=$1
SQL_SERVER_PASSWORD=$2
MAX_RETRY_COUNT=$3
RETRY_INTERVAL=$4
COUNT=0

if [[ "$SQL_SERVER_CONTAINER_NAME" = "sqlserver22" ]]; then
  SQLCMD=/opt/mssql-tools18/bin/sqlcmd
else
  SQLCMD=/opt/mssql-tools/bin/sqlcmd
fi

echo "INFO: Creating no superuser start."

# A SQL Server container takes a few seconds to start SQL Server process
# in the container. So, first, we wait ${RETRY_INTERVAL} seconds before
# we run the `sqlcmd` command in the container.
echo "INFO: Sleep ${RETRY_INTERVAL} seconds to wait for SQL Server start."

while [[ ${COUNT} -lt ${MAX_RETRY_COUNT} ]]
do
    sleep ${RETRY_INTERVAL}

    echo "INFO: Retry count: ${COUNT}"

    docker exec -t ${SQL_SERVER_CONTAINER_NAME} ${SQLCMD} -S localhost -U sa -P ${SQL_SERVER_PASSWORD} -d master -C -Q "SELECT 1"

    if [[ $? -eq 0 ]]; then
        break
    else
        echo "INFO: sqlcmd command failed. Will retry after ${RETRY_INTERVAL} seconds."
    fi

    COUNT=$((COUNT + 1))

    if [[ ${COUNT} -eq ${MAX_RETRY_COUNT} ]]; then
        echo "ERROR: sqlcmd command failed ${MAX_RETRY_COUNT} times. Please check your configuration." >&2
        exit 1
    fi
done

echo "INFO: sqlcmd command succeeded. Continue creating no superuser."

# Create login
echo "INFO: Create login start"
docker exec -t ${SQL_SERVER_CONTAINER_NAME} ${SQLCMD} -S localhost -U sa -P ${SQL_SERVER_PASSWORD} -d master -C -Q "CREATE LOGIN no_superuser WITH PASSWORD = 'no_superuser_password', DEFAULT_DATABASE = master , CHECK_POLICY = OFF, CHECK_EXPIRATION = OFF"
echo "INFO: Create login end"

# Create database
echo "INFO: Create database start"
docker exec -t ${SQL_SERVER_CONTAINER_NAME} ${SQLCMD} -S localhost -U sa -P ${SQL_SERVER_PASSWORD} -d master -C -Q "CREATE DATABASE test_db COLLATE Japanese_BIN2"
echo "INFO: Create database end"

# Create no_superuser
echo "INFO: Create no_superuser start"
docker exec -t ${SQL_SERVER_CONTAINER_NAME} ${SQLCMD} -S localhost -U sa -P ${SQL_SERVER_PASSWORD} -d test_db -C -Q "CREATE USER no_superuser FOR LOGIN no_superuser"
echo "INFO: Create no_superuser end"

# Add roles
echo "INFO: Add role db_ddladmin start"
docker exec -t ${SQL_SERVER_CONTAINER_NAME} ${SQLCMD} -S localhost -U sa -P ${SQL_SERVER_PASSWORD} -d test_db -C -Q "EXEC sp_addrolemember @rolename = 'db_ddladmin', @membername = 'no_superuser'"
echo "INFO: Add role db_ddladmin end"

echo "INFO: Add role db_datawriter start"
docker exec -t ${SQL_SERVER_CONTAINER_NAME} ${SQLCMD} -S localhost -U sa -P ${SQL_SERVER_PASSWORD} -d test_db -C -Q "EXEC sp_addrolemember @rolename = 'db_datawriter', @membername = 'no_superuser'"
echo "INFO: Add role db_datawriter end"

echo "INFO: Add role db_datareader start"
docker exec -t ${SQL_SERVER_CONTAINER_NAME} ${SQLCMD} -S localhost -U sa -P ${SQL_SERVER_PASSWORD} -d test_db -C -Q "EXEC sp_addrolemember @rolename = 'db_datareader', @membername = 'no_superuser'"
echo "INFO: Add role db_datareader end"

# Check the collation of test_db (for debugging purposes)
echo "INFO: Check collation start"
docker exec -t ${SQL_SERVER_CONTAINER_NAME} ${SQLCMD} -S localhost -U no_superuser -P no_superuser_password -d test_db -C -Q "SELECT name, collation_name FROM sys.databases"
echo "INFO: Check collation end"

# Check if no_superuser can access SQL Server (for debugging purposes)
echo "INFO: SELECT @@version start"
docker exec -t ${SQL_SERVER_CONTAINER_NAME} ${SQLCMD} -S localhost -U no_superuser -P no_superuser_password -d test_db -C -Q "SELECT @@version"
echo "INFO: SELECT @@version end"

echo "INFO: Creating no superuser succeeded."
