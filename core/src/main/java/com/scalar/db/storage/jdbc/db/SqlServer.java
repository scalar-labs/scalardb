package com.scalar.db.storage.jdbc.db;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.JdbcConfig;
import com.scalar.db.storage.jdbc.RdbEngineStrategy;
import org.apache.commons.dbcp2.BasicDataSource;

public class SqlServer extends RdbEngineStrategy {

    @Override
    public String getDataTypeForEngine(DataType scalarDbDataType) {
        switch (scalarDbDataType) {
            case BIGINT:
                return "BIGINT";
            case BLOB:
                return "VARBINARY(8000)";
            case BOOLEAN:
                return "BIT";
            case DOUBLE:
                return "FLOAT";
            case FLOAT:
                return "FLOAT(24)";
            case INT:
                return "INT";
            case TEXT:
                return "VARCHAR(8000) COLLATE Latin1_General_BIN";
            default:
                assert false;
                return null;
        }
    }
}