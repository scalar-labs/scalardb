package com.scalar.db.storage.jdbc;

import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.scalar.db.api.LikeExpression;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.query.MergeIntoQuery;
import com.scalar.db.storage.jdbc.query.SelectQuery;
import com.scalar.db.storage.jdbc.query.SelectWithFetchFirstNRowsOnly;
import com.scalar.db.storage.jdbc.query.UpsertQuery;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RdbEngineOracle implements RdbEngineStrategy {
  private static final Logger logger = LoggerFactory.getLogger(RdbEngineOracle.class);

  @Override
  public String[] createSchemaSqls(String fullSchema) {
    return new String[] {
      "CREATE USER " + enclose(fullSchema) + " IDENTIFIED BY \"Oracle1234!@#$\"",
      "ALTER USER " + enclose(fullSchema) + " quota unlimited on USERS",
    };
  }

  @Override
  public String[] createSchemaIfNotExistsSqls(String schema) {
    return createSchemaSqls(schema);
  }

  @Override
  public String createTableInternalPrimaryKeyClause(
      boolean hasDescClusteringOrder, TableMetadata metadata) {
    return "PRIMARY KEY ("
        + Stream.concat(
                metadata.getPartitionKeyNames().stream(), metadata.getClusteringKeyNames().stream())
            .map(this::enclose)
            .collect(Collectors.joining(","))
        + ")) ROWDEPENDENCIES"; // add ROWDEPENDENCIES to the table to improve the performance
  }

  @Override
  public String[] createTableInternalSqlsAfterCreateTable(
      boolean hasDifferentClusteringOrders,
      String schema,
      String table,
      TableMetadata metadata,
      boolean ifNotExists) {
    ArrayList<String> sqls = new ArrayList<>();

    // Set INITRANS to 3 and MAXTRANS to 255 for the table to improve the
    // performance
    sqls.add("ALTER TABLE " + encloseFullTableName(schema, table) + " INITRANS 3 MAXTRANS 255");

    if (hasDifferentClusteringOrders) {
      // Create a unique index for the clustering orders only when both ASC and DESC are contained
      // in the clustering keys. If all the clustering key orders are DESC, the PRIMARY KEY index
      // can be used.
      Optional<String> primaryKeyIndexName = getPrimaryKeyIndexName(schema, table);
      assert primaryKeyIndexName.isPresent();
      sqls.add(
          "CREATE UNIQUE INDEX "
              + enclose(primaryKeyIndexName.get())
              + " ON "
              + encloseFullTableName(schema, table)
              + " ("
              + Stream.concat(
                      metadata.getPartitionKeyNames().stream().map(c -> enclose(c) + " ASC"),
                      metadata.getClusteringKeyNames().stream()
                          .map(c -> enclose(c) + " " + metadata.getClusteringOrder(c)))
                  .collect(Collectors.joining(","))
              + ")");
    }

    return sqls.toArray(new String[0]);
  }

  @Override
  public String tryAddIfNotExistsToCreateTableSql(String createTableSql) {
    return createTableSql;
  }

  @Override
  public boolean isCreateMetadataSchemaDuplicateSchemaError(SQLException e) {
    // ORA-01920: user name 'string' conflicts with another user or role name
    return e.getErrorCode() == 1920;
  }

  @Override
  public String deleteMetadataSchemaSql(String metadataSchema) {
    return "DROP USER " + enclose(metadataSchema);
  }

  @Override
  public String dropNamespaceSql(String namespace) {
    return "DROP USER " + enclose(namespace);
  }

  @Override
  public void dropNamespaceTranslateSQLException(SQLException e, String namespace)
      throws ExecutionException {
    throw new ExecutionException("Dropping the user failed: " + namespace, e);
  }

  @Override
  public String alterColumnTypeSql(
      String namespace, String table, String columnName, String columnType) {
    return "ALTER TABLE "
        + encloseFullTableName(namespace, table)
        + " MODIFY ( "
        + enclose(columnName)
        + " "
        + columnType
        + " )";
  }

  @Override
  public String tableExistsInternalTableCheckSql(String fullTableName) {
    return "SELECT 1 FROM " + fullTableName + " FETCH FIRST 1 ROWS ONLY";
  }

  @Override
  public String dropIndexSql(String schema, String table, String indexName) {
    return "DROP INDEX " + enclose(indexName);
  }

  @Override
  public String enclose(String name) {
    return "\"" + name + "\"";
  }

  @Override
  public SelectQuery buildSelectQuery(SelectQuery.Builder builder, int limit) {
    return new SelectWithFetchFirstNRowsOnly(builder, limit);
  }

  @Override
  public UpsertQuery buildUpsertQuery(UpsertQuery.Builder builder) {
    return new MergeIntoQuery(builder);
  }

  @Override
  public boolean isDuplicateTableError(SQLException e) {
    // ORA-00955: name is already used by an existing object
    return e.getErrorCode() == 955;
  }

  @Override
  public boolean isDuplicateKeyError(SQLException e) {
    if (e.getSQLState() == null) {
      return false;
    }
    // Integrity constraint violation
    return e.getSQLState().equals("23000");
  }

  @Override
  public boolean isUndefinedTableError(SQLException e) {
    // ORA-00942: Table or view does not exist
    return e.getErrorCode() == 942;
  }

  @Override
  public boolean isConflict(SQLException e) {
    // ORA-08177: can't serialize access for this transaction
    // ORA-00060: deadlock detected while waiting for resource
    return e.getErrorCode() == 8177 || e.getErrorCode() == 60;
  }

  @Override
  public boolean isDuplicateIndexError(SQLException e) {
    // https://docs.oracle.com/en/error-help/db/ora-00955/
    // code : 955
    // message : name is already used by an existing object
    return e.getErrorCode() == 955;
  }

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
        throw new AssertionError();
    }
  }

  @Override
  public String getDataTypeForKey(DataType dataType) {
    switch (dataType) {
      case TEXT:
        return "VARCHAR2(64)";
      case BLOB:
        return "RAW(64)";
      default:
        return null;
    }
  }

  @Override
  public DataType getDataTypeForScalarDb(
      JDBCType type, String typeName, int columnSize, int digits, String columnDescription) {
    String numericTypeDescription = String.format("%s(%d, %d)", typeName, columnSize, digits);
    switch (type) {
      case NUMERIC:
        if (columnSize > 15) {
          throw new IllegalArgumentException(
              CoreError.JDBC_IMPORT_DATA_TYPE_NOT_SUPPORTED.buildMessage(
                  numericTypeDescription, columnDescription));
        }
        if (digits == 0) {
          logger.info(
              "Data type larger than that of underlying database is assigned: {} to BIGINT",
              numericTypeDescription);
          return DataType.BIGINT;
        } else {
          logger.info(
              "Fixed-point data type is casted, be aware round-up or round-off can be happen in underlying database: {} ({} to DOUBLE)",
              columnDescription,
              numericTypeDescription);
          return DataType.DOUBLE;
        }
      case REAL:
        return DataType.FLOAT;
      case FLOAT:
        if (columnSize > 53) {
          throw new IllegalArgumentException(
              CoreError.JDBC_IMPORT_DATA_TYPE_NOT_SUPPORTED.buildMessage(
                  numericTypeDescription, columnDescription));
        }
        if (columnSize < 53) {
          logger.info(
              "Data type larger than that of underlying database is assigned: {} to DOUBLE",
              numericTypeDescription);
        }
        return DataType.DOUBLE;
      case DOUBLE:
        return DataType.DOUBLE;
      case CHAR:
      case NCHAR:
      case VARCHAR:
      case NVARCHAR:
      case CLOB:
      case NCLOB:
        logger.info(
            "Data type larger than that of underlying database is assigned: {} ({} to TEXT)",
            columnDescription,
            typeName);
        return DataType.TEXT;
      case LONGVARCHAR:
        return DataType.TEXT;
      case VARBINARY:
        logger.info(
            "Data type larger than that of underlying database is assigned: {} ({} to BLOB)",
            columnDescription,
            typeName);
        return DataType.BLOB;
      case LONGVARBINARY:
        return DataType.BLOB;
      case BLOB:
        logger.warn(
            "Data type that may be smaller than that of underlying database is assigned: {} (Oracle {} to ScalarDB BLOB)",
            columnDescription,
            typeName);
        return DataType.BLOB;
      default:
        throw new IllegalArgumentException(
            CoreError.JDBC_IMPORT_DATA_TYPE_NOT_SUPPORTED.buildMessage(
                typeName, columnDescription));
    }
  }

  /**
   * Takes JDBC column information and returns a corresponding ScalarDB data type. The data type
   * mapping logic in this method is based on {@link RdbEngineOracle#getDataTypeForScalarDb} which
   * is created only for the table import feature and a bit too strict for other usages.
   *
   * @param type A JDBC type.
   * @param typeName A JDBC column type name.
   * @param columnSize A JDBC column size.
   * @param digits A JDBC column digits.
   * @param columnDescription A JDBC column description.
   * @return A corresponding ScalarDB data type.
   */
  @Override
  public DataType getDataTypeForScalarDbLeniently(
      JDBCType type, String typeName, int columnSize, int digits, String columnDescription) {
    switch (type) {
      case BOOLEAN:
        return DataType.BOOLEAN;
      case NUMERIC:
        if (digits == 0) {
          if (columnSize == 1) {
            return DataType.BOOLEAN;
          }
          return DataType.BIGINT;
        } else {
          return DataType.DOUBLE;
        }
      case REAL:
        return DataType.FLOAT;
      case FLOAT:
      case DOUBLE:
        return DataType.DOUBLE;
      case CHAR:
      case NCHAR:
      case VARCHAR:
      case NVARCHAR:
      case CLOB:
      case NCLOB:
      case LONGVARCHAR:
        return DataType.TEXT;
      case VARBINARY:
      case LONGVARBINARY:
      case BLOB:
        return DataType.BLOB;
      default:
        throw new IllegalArgumentException(
            String.format(
                "Unexpected data type. JDBC type: %s, Type name: %s, Column size: %d, Column digits: %d, Column desc: %s",
                type, typeName, columnSize, digits, columnDescription));
    }
  }

  @Override
  public int getSqlTypes(DataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return Types.BIT;
      case INT:
        return Types.INTEGER;
      case BIGINT:
        return Types.BIGINT;
      case FLOAT:
        return Types.FLOAT;
      case DOUBLE:
        return Types.DOUBLE;
      case TEXT:
        return Types.VARCHAR;
      case BLOB:
        return Types.BLOB;
      default:
        throw new AssertionError();
    }
  }

  @Override
  public String getTextType(int charLength) {
    return String.format("VARCHAR2(%s)", charLength);
  }

  @Override
  public String computeBooleanValue(boolean value) {
    return value ? "1" : "0";
  }

  @Override
  public Driver getDriver() {
    return new oracle.jdbc.driver.OracleDriver();
  }

  @Override
  public String getEscape(LikeExpression likeExpression) {
    String escape = likeExpression.getEscape();
    return escape.isEmpty() ? null : escape;
  }

  @Override
  public String tryAddIfNotExistsToCreateIndexSql(String createIndexSql) {
    return createIndexSql;
  }

  @Override
  public boolean isIndexInfoSupported(DatabaseMetaData metaData) {
    // `java.sql.DatabaseMetaData.getIndexInfo()` with Oracle returns index information as follows:
    //
    // TABLE_SCHEM:int_test_tx_admin_repair, TABLE_NAME:test_table, INDEX_NAME:null, TYPE:0,
    // COLUMN_NAME:null, ASC_OR_DESC:null
    // TABLE_SCHEM:int_test_tx_admin_repair, TABLE_NAME:test_table, INDEX_NAME:SYS_C007351, TYPE:1,
    // COLUMN_NAME:c2, ASC_OR_DESC:null
    // TABLE_SCHEM:int_test_tx_admin_repair, TABLE_NAME:test_table, INDEX_NAME:SYS_C007351, TYPE:1,
    // COLUMN_NAME:c1, ASC_OR_DESC:null
    // TABLE_SCHEM:int_test_tx_admin_repair, TABLE_NAME:test_table, INDEX_NAME:SYS_C007351, TYPE:1,
    // COLUMN_NAME:c4, ASC_OR_DESC:null
    // TABLE_SCHEM:int_test_tx_admin_repair, TABLE_NAME:test_table, INDEX_NAME:SYS_C007351, TYPE:1,
    // COLUMN_NAME:c3, ASC_OR_DESC:null
    // TABLE_SCHEM:SYSTEM, TABLE_NAME:test_table,
    // INDEX_NAME:int_test_tx_admin_repair.test_table_clustering_order_idx, TYPE:1, COLUMN_NAME:c2,
    // ASC_OR_DESC:null
    // TABLE_SCHEM:SYSTEM, TABLE_NAME:test_table,
    // INDEX_NAME:int_test_tx_admin_repair.test_table_clustering_order_idx, TYPE:1, COLUMN_NAME:c1,
    // ASC_OR_DESC:null
    // TABLE_SCHEM:SYSTEM, TABLE_NAME:test_table,
    // INDEX_NAME:int_test_tx_admin_repair.test_table_clustering_order_idx, TYPE:1, COLUMN_NAME:c4,
    // ASC_OR_DESC:null
    // TABLE_SCHEM:SYSTEM, TABLE_NAME:test_table,
    // INDEX_NAME:int_test_tx_admin_repair.test_table_clustering_order_idx, TYPE:1,
    // COLUMN_NAME:SYS_NC00029$, ASC_OR_DESC:null
    // TABLE_SCHEM:SYSTEM, TABLE_NAME:test_table,
    // INDEX_NAME:index_int_test_tx_admin_repair_test_table_c5, TYPE:1, COLUMN_NAME:c5,
    // ASC_OR_DESC:null
    // TABLE_SCHEM:SYSTEM, TABLE_NAME:test_table,
    // INDEX_NAME:index_int_test_tx_admin_repair_test_table_c6, TYPE:1, COLUMN_NAME:c6,
    // ASC_OR_DESC:null
    //
    // The necessary information spans over 2 schemas, and some index names and column names are
    // `SYS_xxxxxxx`, which are hard to use.
    // Therefore, index information isn't supported with Oracle at this moment to avoid complicated
    // naive implementation.
    return false;
  }

  @Override
  public boolean isDefaultPrimaryKeyIndex(String namespace, String table, String indexName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<String> getPrimaryKeyIndexName(String namespace, String table) {
    return Optional.of(getFullTableName(namespace, table) + "_clustering_order_idx");
  }
}
