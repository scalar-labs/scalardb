package com.scalar.db.storage.jdbc.db;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.JdbcAdmin;
import com.scalar.db.storage.jdbc.JdbcConfig;
import org.apache.commons.dbcp2.BasicDataSource;

public class PostgresqlAdmin extends JdbcAdmin {

    public PostgresqlAdmin(DatabaseConfig databaseConfig) {
        super(databaseConfig);
    }

    protected PostgresqlAdmin(BasicDataSource dataSource, JdbcConfig config) {
        super(dataSource, config);
    }

    @Override
    protected String getDataTypeForEngine(DataType scalarDbDataType) {
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