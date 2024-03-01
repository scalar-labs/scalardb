#!/bin/bash
set -eu

# Get container name and password from arguments
SQL_SERVER_CONTAINER_NAME=$1
SQL_SERVER_PASSWORD=$2

# Create login
docker exec -t ${SQL_SERVER_CONTAINER_NAME} /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P ${SQL_SERVER_PASSWORD} -d master -Q "CREATE LOGIN no_superuser WITH PASSWORD = 'no_superuser_password', DEFAULT_DATABASE = master , CHECK_POLICY = OFF, CHECK_EXPIRATION = OFF"

# Create database
docker exec -t ${SQL_SERVER_CONTAINER_NAME} /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P ${SQL_SERVER_PASSWORD} -d master -Q "CREATE DATABASE test_db"

# Create no_superuser
docker exec -t ${SQL_SERVER_CONTAINER_NAME} /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P ${SQL_SERVER_PASSWORD} -d test_db -Q "CREATE USER no_superuser FOR LOGIN no_superuser"

# Add roles
docker exec -t ${SQL_SERVER_CONTAINER_NAME} /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P ${SQL_SERVER_PASSWORD} -d test_db -Q "EXEC sp_addrolemember @rolename = 'db_ddladmin', @membername = 'no_superuser'"
docker exec -t ${SQL_SERVER_CONTAINER_NAME} /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P ${SQL_SERVER_PASSWORD} -d test_db -Q "EXEC sp_addrolemember @rolename = 'db_datawriter', @membername = 'no_superuser'"
docker exec -t ${SQL_SERVER_CONTAINER_NAME} /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P ${SQL_SERVER_PASSWORD} -d test_db -Q "EXEC sp_addrolemember @rolename = 'db_datareader', @membername = 'no_superuser'"

# Check if no_superuser can access SQL Server (for debugging purposes)
docker exec -t ${SQL_SERVER_CONTAINER_NAME} /opt/mssql-tools/bin/sqlcmd -S localhost -U no_superuser -P no_superuser_password -d test_db -Q "SELECT @@version"
