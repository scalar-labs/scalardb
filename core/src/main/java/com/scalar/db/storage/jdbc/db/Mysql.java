package com.scalar.db.storage.jdbc.db;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.JdbcConfig;
import com.scalar.db.storage.jdbc.RdbEngineStrategy;
import org.apache.commons.dbcp2.BasicDataSource;

public class Mysql extends RdbEngineStrategy {

    @Override
    public String getDataTypeForEngine(DataType scalarDbDataType) {
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