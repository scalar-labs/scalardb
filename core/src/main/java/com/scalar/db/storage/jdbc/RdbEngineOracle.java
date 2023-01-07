package com.scalar.db.storage.jdbc;

import com.scalar.db.io.DataType;

class RdbEngineOracle extends RdbEngineStrategy {

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