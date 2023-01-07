package com.scalar.db.storage.jdbc.db;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.JdbcAdmin;
import com.scalar.db.storage.jdbc.JdbcConfig;
import org.apache.commons.dbcp2.BasicDataSource;

public class SqlServerAdmin extends JdbcAdmin {

    public SqlServerAdmin(DatabaseConfig databaseConfig) {
        super(databaseConfig);
    }

    protected SqlServerAdmin(BasicDataSource dataSource, JdbcConfig config) {
        super(dataSource, config);
    }

    @Override
    protected String getDataTypeForEngine(DataType scalarDbDataType) {
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