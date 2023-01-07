package com.scalar.db.storage.jdbc;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.scalar.db.storage.jdbc.query.QueryUtils.enclosedFullTableName;
import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

class RdbEnginePostgresql extends RdbEngineStrategy {

    RdbEnginePostgresql(BasicDataSource dataSource, RdbEngine rdbEngine, String metadataSchema) {
        super(dataSource, rdbEngine, metadataSchema);
    }

    @Override
    protected void createNamespaceExecute(Connection connection, String fullNamespace) throws SQLException {
        execute(connection, "CREATE SCHEMA " + fullNamespace);
    }

    @Override
    String createTableInternalPrimaryKeyClause(boolean hasDescClusteringOrder, TableMetadata metadata) {
        return "PRIMARY KEY ("
                   + Stream.concat(
                metadata.getPartitionKeyNames().stream(),
                metadata.getClusteringKeyNames().stream())
                         .map(this::enclose)
                         .collect(Collectors.joining(","))
                   + "))";
    }

    @Override
    void createTableInternalExecuteAfterCreateTable(boolean hasDescClusteringOrder, Connection connection, String schema, String table, TableMetadata metadata) throws SQLException {
        if (hasDescClusteringOrder) {
            // Create a unique index for the clustering orders
            String createUniqueIndexStatement =
                "CREATE UNIQUE INDEX "
                    + enclose(getFullTableName(schema, table) + "_clustering_order_idx")
                    + " ON "
                    + enclosedFullTableName(schema, table, RdbEngine.POSTGRESQL)
                    + " ("
                    + Stream.concat(
                        metadata.getPartitionKeyNames().stream().map(c -> enclose(c) + " ASC"),
                        metadata.getClusteringKeyNames().stream()
                            .map(c -> enclose(c) + " " + metadata.getClusteringOrder(c)))
                          .collect(Collectors.joining(","))
                    + ")";
            execute(connection, createUniqueIndexStatement);
        }
    }

    @Override
    String getDataTypeForEngine(DataType scalarDbDataType) {
        switch (scalarDbDataType) {
            case BIGINT:
                return "BIGINT";
            case BLOB:
                return "BYTEA";
            case BOOLEAN:
                return "BOOLEAN";
            case DOUBLE:
                return "DOUBLE PRECISION";
            case FLOAT:
                return "FLOAT";
            case INT:
                return "INT";
            case TEXT:
                return "TEXT";
            default:
                assert false;
                return null;
        }
    }
}