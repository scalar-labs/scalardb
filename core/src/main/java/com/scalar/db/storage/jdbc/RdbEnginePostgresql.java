package com.scalar.db.storage.jdbc;

import com.scalar.db.io.DataType;

class RdbEnginePostgresql extends RdbEngineStrategy {

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