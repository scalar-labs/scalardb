package com.scalar.db.storage.jdbc.db;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.JdbcConfig;
import com.scalar.db.storage.jdbc.RdbEngineStrategy;
import org.apache.commons.dbcp2.BasicDataSource;

public class Oracle implements RdbEngineStrategy {

    @Override
    public String getDataTypeForEngine(DataType scalarDbDataType) {
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