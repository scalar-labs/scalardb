package com.scalar.db.storage.jdbc;

import com.scalar.db.io.DataType;
import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Connection;
import java.sql.SQLException;

class RdbEngineOracle extends RdbEngineStrategy {

    RdbEngineOracle(BasicDataSource dataSource, RdbEngine rdbEngine, String metadataSchema) {
        super(dataSource, rdbEngine, metadataSchema);
    }

    @Override
    protected void createNamespaceExecute(Connection connection, String fullNamespace) throws SQLException {
        execute(connection, "CREATE USER " + fullNamespace + " IDENTIFIED BY \"oracle\"");
        execute(connection, "ALTER USER " + fullNamespace + " quota unlimited on USERS");
    }

    @Override
    String getDataTypeForEngine(DataType scalarDbDataType) {
        switch (scalarDbDataType) {
            case BIGINT:
                return "NUMBER(19)";
            case BLOB:
                return "RAW(2000)";
            case BOOLEAN:
                return "NUMBER(1)";
            case DOUBLE:
                return "BINARY_DOUBLE";
            case FLOAT:
                return "BINARY_FLOAT";
            case INT:
                return "INT";
            case TEXT:
                return "VARCHAR2(4000)";
            default:
                assert false;
                return null;
        }
    }
}