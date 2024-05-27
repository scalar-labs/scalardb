package com.scalar.db.storage.jdbc;

import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.query.MergeIntoQuery;
import com.scalar.db.storage.jdbc.query.SelectQuery;
import com.scalar.db.storage.jdbc.query.SelectWithFetchFirstNRowsOnly;
import com.scalar.db.storage.jdbc.query.UpsertQuery;
import java.sql.Driver;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RdbEngineOracle implements RdbEngineStrategy {
  private static final Logger logger = LoggerFactory.getLogger(RdbEngineOracle.class);

  @Override
  public String[] createNamespaceSqls(String fullNamespace) {
    return new String[] {
      "CREATE USER " + fullNamespace + " IDENTIFIED BY \"Oracle1234!@#$\"",
      "ALTER USER " + fullNamespace + " quota unlimited on USERS",
    };
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
      boolean hasDescClusteringOrder, String schema, String table, TableMetadata metadata) {
    ArrayList<String> sqls = new ArrayList<>();

    // Set INITRANS to 3 and MAXTRANS to 255 for the table to improve the
    // performance
    sqls.add("ALTER TABLE " + encloseFullTableName(schema, table) + " INITRANS 3 MAXTRANS 255");

    if (hasDescClusteringOrder) {
      // Create a unique index for the clustering orders
      sqls.add(
          "CREATE UNIQUE INDEX "
              + enclose(getFullTableName(schema, table) + "_clustering_order_idx")
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
  public String[] createMetadataSchemaIfNotExistsSql(String metadataSchema) {
    return new String[] {
      "CREATE USER " + enclose(metadataSchema) + " IDENTIFIED BY \"Oracle1234!@#$\"",
      "ALTER USER " + enclose(metadataSchema) + " quota unlimited on USERS",
    };
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
    throw new ExecutionException("dropping the user failed: " + namespace, e);
  }

  @Override
  public String namespaceExistsStatement() {
    return "SELECT 1 FROM " + enclose("ALL_USERS") + " WHERE " + enclose("USERNAME") + " = ?";
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
  public boolean isConflictError(SQLException e) {
    // ORA-08177: can't serialize access for this transaction
    // ORA-00060: deadlock detected while waiting for resource
    return e.getErrorCode() == 8177 || e.getErrorCode() == 60;
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
        assert false;
        return null;
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
              String.format(
                  "data type %s is unsupported: %s", numericTypeDescription, columnDescription));
        }
        if (digits == 0) {
          logger.info(
              "data type larger than that of underlying database is assigned: {} to BIGINT",
              numericTypeDescription);
          return DataType.BIGINT;
        } else {
          logger.info(
              "fixed-point data type is casted, be aware round-up or round-off can be happen in underlying database: {} ({} to DOUBLE)",
              columnDescription,
              numericTypeDescription);
          return DataType.DOUBLE;
        }
      case REAL:
        return DataType.FLOAT;
      case FLOAT:
        if (columnSize > 53) {
          throw new IllegalArgumentException(
              String.format(
                  "data type %s is unsupported: %s", numericTypeDescription, columnDescription));
        }
        if (columnSize < 53) {
          logger.info(
              "data type larger than that of underlying database is assigned: {} to DOUBLE",
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
            "data type larger than that of underlying database is assigned: {} ({} to TEXT)",
            columnDescription,
            typeName);
        return DataType.TEXT;
      case LONGVARCHAR:
        return DataType.TEXT;
      case VARBINARY:
        logger.info(
            "data type larger than that of underlying database is assigned: {} ({} to BLOB)",
            columnDescription,
            typeName);
        return DataType.BLOB;
      case LONGVARBINARY:
        return DataType.BLOB;
      case BLOB:
        logger.warn(
            "data type that may be smaller than that of underlying database is assigned: {} (Oracle {} to ScalarDB BLOB)",
            columnDescription,
            typeName);
        return DataType.BLOB;
      default:
        throw new IllegalArgumentException(
            String.format("data type %s is unsupported: %s", typeName, columnDescription));
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
}
