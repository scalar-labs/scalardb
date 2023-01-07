package com.scalar.db.storage.jdbc;

import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.RdbEngineStrategy;
import org.apache.commons.dbcp2.BasicDataSource;

class RdbEngineMysql extends RdbEngineStrategy {

    RdbEngineMysql(BasicDataSource dataSource, RdbEngine rdbEngine, String metadataSchema) {
        super(dataSource, rdbEngine, metadataSchema);
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