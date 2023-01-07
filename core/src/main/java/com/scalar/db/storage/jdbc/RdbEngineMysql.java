package com.scalar.db.storage.jdbc;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.RdbEngineStrategy;
import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class RdbEngineMysql extends RdbEngineStrategy {

    RdbEngineMysql(BasicDataSource dataSource, RdbEngine rdbEngine, String metadataSchema) {
        super(dataSource, rdbEngine, metadataSchema);
    }

    @Override
    protected void createNamespaceExecute(Connection connection, String fullNamespace) throws SQLException {
        execute(
            connection, "CREATE SCHEMA " + fullNamespace + " character set utf8 COLLATE utf8_bin");
    }

    @Override
    String createTableInternalPrimaryKeyClause(boolean hasDescClusteringOrder, TableMetadata metadata) {
        if (hasDescClusteringOrder) {
            return "PRIMARY KEY ("
                    + Stream.concat(
                        metadata.getPartitionKeyNames().stream().map(c -> enclose(c) + " ASC"),
                        metadata.getClusteringKeyNames().stream()
                            .map(c -> enclose(c) + " " + metadata.getClusteringOrder(c)))
                          .collect(Collectors.joining(","))
                    + "))";
        } else {
            return "PRIMARY KEY ("
                    + Stream.concat(
                        metadata.getPartitionKeyNames().stream(),
                        metadata.getClusteringKeyNames().stream())
                          .map(this::enclose)
                          .collect(Collectors.joining(","))
                    + "))";
        }
    }

    @Override
    String getDataTypeForEngine(DataType scalarDbDataType) {
        switch (scalarDbDataType) {
            case BIGINT:
                return "BIGINT";
            case BLOB:
                return "LONGBLOB";
            case BOOLEAN:
                return "BOOLEAN";
            case DOUBLE:
            case FLOAT:
                return "DOUBLE";
            case INT:
                return "INT";
            case TEXT:
                return "LONGTEXT";
            default:
                assert false;
                return null;
        }
    }
}