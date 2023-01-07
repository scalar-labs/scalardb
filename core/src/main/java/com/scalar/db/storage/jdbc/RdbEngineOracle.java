package com.scalar.db.storage.jdbc;

import com.scalar.db.io.DataType;
import org.apache.commons.dbcp2.BasicDataSource;

class RdbEngineOracle extends RdbEngineStrategy {

    RdbEngineOracle(BasicDataSource dataSource, RdbEngine rdbEngine, String metadataSchema) {
        super(dataSource, rdbEngine, metadataSchema);
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