#!/bin/bash
set -u

# Get container name and password from arguments
SQL_SERVER_CONTAINER_NAME=$1
SQL_SERVER_PASSWORD=$2
MAX_RETRY_COUNT=$3
RETRY_INTERVAL=$4
COUNT=0

echo "INFO: Creating no superuser start."

# A SQL Server container takes a few seconds to start SQL Server process
# in the container. So, first, we wait ${RETRY_INTERVAL} seconds before
# we run the `sqlcmd` command in the container.
echo "INFO: Sleep ${RETRY_INTERVAL} seconds to wait for SQL Server start."

while [[ ${COUNT} -lt ${MAX_RETRY_COUNT} ]]
do
    sleep ${RETRY_INTERVAL}

    echo "INFO: Retry count: ${COUNT}"

    if [[ "$1" = "sqlserver22" ]]; then
        docker exec -t ${SQL_SERVER_CONTAINER_NAME} /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P ${SQL_SERVER_PASSWORD} -d master -C -Q "SELECT 1"
    else
        docker exec -t ${SQL_SERVER_CONTAINER_NAME} /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P ${SQL_SERVER_PASSWORD} -d master -Q "SELECT 1"
    fi

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
if [[ "$1" = "sqlserver22" ]]; then
    docker exec -t ${SQL_SERVER_CONTAINER_NAME} /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P ${SQL_SERVER_PASSWORD} -d master -C -Q "CREATE LOGIN no_superuser WITH PASSWORD = 'no_superuser_password', DEFAULT_DATABASE = master , CHECK_POLICY = OFF, CHECK_EXPIRATION = OFF"
else
    docker exec -t ${SQL_SERVER_CONTAINER_NAME} /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P ${SQL_SERVER_PASSWORD} -d master -Q "CREATE LOGIN no_superuser WITH PASSWORD = 'no_superuser_password', DEFAULT_DATABASE = master , CHECK_POLICY = OFF, CHECK_EXPIRATION = OFF"
fi
echo "INFO: Create login end"

# Create database
echo "INFO: Create database start"
if [[ "$1" = "sqlserver22" ]]; then
    docker exec -t ${SQL_SERVER_CONTAINER_NAME} /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P ${SQL_SERVER_PASSWORD} -d master -C -Q "CREATE DATABASE test_db COLLATE Japanese_BIN2"
else
    docker exec -t ${SQL_SERVER_CONTAINER_NAME} /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P ${SQL_SERVER_PASSWORD} -d master -Q "CREATE DATABASE test_db COLLATE Japanese_BIN2"
fi
echo "INFO: Create database end"

# Create no_superuser
echo "INFO: Create no_superuser start"
if [[ "$1" = "sqlserver22" ]]; then
    docker exec -t ${SQL_SERVER_CONTAINER_NAME} /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P ${SQL_SERVER_PASSWORD} -d test_db -C -Q "CREATE USER no_superuser FOR LOGIN no_superuser"
else
    docker exec -t ${SQL_SERVER_CONTAINER_NAME} /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P ${SQL_SERVER_PASSWORD} -d test_db -Q "CREATE USER no_superuser FOR LOGIN no_superuser"
fi
echo "INFO: Create no_superuser end"

# Add roles
echo "INFO: Add role db_ddladmin start"
if [[ "$1" = "sqlserver22" ]]; then
    docker exec -t ${SQL_SERVER_CONTAINER_NAME} /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P ${SQL_SERVER_PASSWORD} -d test_db -C -Q "EXEC sp_addrolemember @rolename = 'db_ddladmin', @membername = 'no_superuser'"
else
    docker exec -t ${SQL_SERVER_CONTAINER_NAME} /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P ${SQL_SERVER_PASSWORD} -d test_db -Q "EXEC sp_addrolemember @rolename = 'db_ddladmin', @membername = 'no_superuser'"
fi
echo "INFO: Add role db_ddladmin end"

echo "INFO: Add role db_datawriter start"
if [[ "$1" = "sqlserver22" ]]; then
    docker exec -t ${SQL_SERVER_CONTAINER_NAME} /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P ${SQL_SERVER_PASSWORD} -d test_db -C -Q "EXEC sp_addrolemember @rolename = 'db_datawriter', @membername = 'no_superuser'"
else
    docker exec -t ${SQL_SERVER_CONTAINER_NAME} /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P ${SQL_SERVER_PASSWORD} -d test_db -Q "EXEC sp_addrolemember @rolename = 'db_datawriter', @membername = 'no_superuser'"
fi
echo "INFO: Add role db_datawriter end"

echo "INFO: Add role db_datareader start"
if [[ "$1" = "sqlserver22" ]]; then
    docker exec -t ${SQL_SERVER_CONTAINER_NAME} /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P ${SQL_SERVER_PASSWORD} -d test_db -C -Q "EXEC sp_addrolemember @rolename = 'db_datareader', @membername = 'no_superuser'"
else
    docker exec -t ${SQL_SERVER_CONTAINER_NAME} /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P ${SQL_SERVER_PASSWORD} -d test_db -Q "EXEC sp_addrolemember @rolename = 'db_datareader', @membername = 'no_superuser'"
fi
echo "INFO: Add role db_datareader end"

# Check the collation of test_db (for debugging purposes)
echo "INFO: Check collation start"
if [[ "$1" = "sqlserver22" ]]; then
    docker exec -t ${SQL_SERVER_CONTAINER_NAME} /opt/mssql-tools18/bin/sqlcmd -S localhost -U no_superuser -P no_superuser_password -d test_db -C -Q "SELECT name, collation_name FROM sys.databases" -W
else
    docker exec -t ${SQL_SERVER_CONTAINER_NAME} /opt/mssql-tools/bin/sqlcmd -S localhost -U no_superuser -P no_superuser_password -d test_db -Q "SELECT name, collation_name FROM sys.databases" -W
fi
echo "INFO: Check collation end"

# Check if no_superuser can access SQL Server (for debugging purposes)
echo "INFO: SELECT @@version start"
if [[ "$1" = "sqlserver22" ]]; then
    docker exec -t ${SQL_SERVER_CONTAINER_NAME} /opt/mssql-tools18/bin/sqlcmd -S localhost -U no_superuser -P no_superuser_password -d test_db -C -Q "SELECT @@version"
else
    docker exec -t ${SQL_SERVER_CONTAINER_NAME} /opt/mssql-tools/bin/sqlcmd -S localhost -U no_superuser -P no_superuser_password -d test_db -Q "SELECT @@version"
fi
echo "INFO: SELECT @@version end"

echo "INFO: Creating no superuser succeeded."
