#!/bin/bash
set -u

# Get container name and password from arguments
SQL_SERVER_CONTAINER_NAME=$1
SQL_SERVER_PASSWORD=$2
MAX_RETRY_COUNT=$3
RETRY_INTERVAL=$4
COUNT=0

# A SQL Server container takes a few seconds to start SQL Server process
# in the container. So, first, we wait ${RETRY_INTERVAL} seconds before
# we run the `sqlcmd` command in the container.
echo "Sleep ${RETRY_INTERVAL} seconds to wait for SQL Server start."

while [[ ${COUNT} -lt ${MAX_RETRY_COUNT} ]]
do
    sleep ${RETRY_INTERVAL}

    echo "Retry count: ${COUNT}"

    docker exec -t ${SQL_SERVER_CONTAINER_NAME} /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P ${SQL_SERVER_PASSWORD} -d master -Q "SELECT 1"
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
docker exec -t ${SQL_SERVER_CONTAINER_NAME} /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P ${SQL_SERVER_PASSWORD} -d master -Q "CREATE LOGIN no_superuser WITH PASSWORD = 'no_superuser_password', DEFAULT_DATABASE = master , CHECK_POLICY = OFF, CHECK_EXPIRATION = OFF"

# Create database
docker exec -t ${SQL_SERVER_CONTAINER_NAME} /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P ${SQL_SERVER_PASSWORD} -d master -Q "CREATE DATABASE test_db"

# Create no_superuser
docker exec -t ${SQL_SERVER_CONTAINER_NAME} /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P ${SQL_SERVER_PASSWORD} -d test_db -Q "CREATE USER no_superuser FOR LOGIN no_superuser"

# Add roles
docker exec -t ${SQL_SERVER_CONTAINER_NAME} /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P ${SQL_SERVER_PASSWORD} -d test_db -Q "EXEC sp_addrolemember @rolename = 'db_ddladmin', @membername = 'no_superuser'"
docker exec -t ${SQL_SERVER_CONTAINER_NAME} /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P ${SQL_SERVER_PASSWORD} -d test_db -Q "EXEC sp_addrolemember @rolename = 'db_datawriter', @membername = 'no_superuser'"
docker exec -t ${SQL_SERVER_CONTAINER_NAME} /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P ${SQL_SERVER_PASSWORD} -d test_db -Q "EXEC sp_addrolemember @rolename = 'db_datareader', @membername = 'no_superuser'"

# Check if no_superuser can access SQL Server (for debugging purposes)
docker exec -t ${SQL_SERVER_CONTAINER_NAME} /opt/mssql-tools/bin/sqlcmd -S localhost -U no_superuser -P no_superuser_password -d test_db -Q "SELECT @@version"
echo "INFO: Creating no superuser succeeded."
