package com.scalar.db.storage.jdbc;

import com.scalar.db.io.DataType;
import org.apache.commons.dbcp2.BasicDataSource;

class RdbEngineSqlServer extends RdbEngineStrategy {

    RdbEngineSqlServer(BasicDataSource dataSource, RdbEngine rdbEngine, String metadataSchema) {
        super(dataSource, rdbEngine, metadataSchema);
    }

    @Override
    String getDataTypeForEngine(DataType scalarDbDataType) {
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