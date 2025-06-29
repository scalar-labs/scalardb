package com.scalar.db.storage.jdbc;

import static com.scalar.db.config.ConfigUtils.getBoolean;
import static com.scalar.db.config.ConfigUtils.getInt;
import static com.scalar.db.config.ConfigUtils.getString;

import com.scalar.db.common.CoreError;
import com.scalar.db.config.DatabaseConfig;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
public class JdbcConfig {
  private static final Logger logger = LoggerFactory.getLogger(JdbcConfig.class);

  public static final String STORAGE_NAME = "jdbc";
  public static final String TRANSACTION_MANAGER_NAME = STORAGE_NAME;
  public static final String PREFIX = DatabaseConfig.PREFIX + STORAGE_NAME + ".";
  public static final String CONNECTION_POOL_MIN_IDLE = PREFIX + "connection_pool.min_idle";
  public static final String CONNECTION_POOL_MAX_IDLE = PREFIX + "connection_pool.max_idle";
  public static final String CONNECTION_POOL_MAX_TOTAL = PREFIX + "connection_pool.max_total";
  public static final String PREPARED_STATEMENTS_POOL_ENABLED =
      PREFIX + "prepared_statements_pool.enabled";
  public static final String PREPARED_STATEMENTS_POOL_MAX_OPEN =
      PREFIX + "prepared_statements_pool.max_open";

  public static final String ISOLATION_LEVEL = PREFIX + "isolation_level";

  /** @deprecated As of 5.0, will be removed. */
  @Deprecated public static final String TABLE_METADATA_SCHEMA = PREFIX + "table_metadata.schema";

  public static final String TABLE_METADATA_CONNECTION_POOL_MIN_IDLE =
      PREFIX + "table_metadata.connection_pool.min_idle";
  public static final String TABLE_METADATA_CONNECTION_POOL_MAX_IDLE =
      PREFIX + "table_metadata.connection_pool.max_idle";
  public static final String TABLE_METADATA_CONNECTION_POOL_MAX_TOTAL =
      PREFIX + "table_metadata.connection_pool.max_total";

  public static final String ADMIN_CONNECTION_POOL_MIN_IDLE =
      PREFIX + "admin.connection_pool.min_idle";
  public static final String ADMIN_CONNECTION_POOL_MAX_IDLE =
      PREFIX + "admin.connection_pool.max_idle";
  public static final String ADMIN_CONNECTION_POOL_MAX_TOTAL =
      PREFIX + "admin.connection_pool.max_total";

  public static final String MYSQL_VARIABLE_KEY_COLUMN_SIZE =
      PREFIX + "mysql.variable_key_column_size";
  public static final String ORACLE_VARIABLE_KEY_COLUMN_SIZE =
      PREFIX + "oracle.variable_key_column_size";
  public static final String DB2_VARIABLE_KEY_COLUMN_SIZE = PREFIX + "db2.variable_key_column_size";
  public static final String ORACLE_TIME_COLUMN_DEFAULT_DATE_COMPONENT =
      PREFIX + "oracle.time_column.default_date_component";
  public static final String DB2_TIME_COLUMN_DEFAULT_DATE_COMPONENT =
      PREFIX + "db2.time_column.default_date_component";
  public static final int DEFAULT_CONNECTION_POOL_MIN_IDLE = 20;
  public static final int DEFAULT_CONNECTION_POOL_MAX_IDLE = 50;
  public static final int DEFAULT_CONNECTION_POOL_MAX_TOTAL = 200;
  public static final boolean DEFAULT_PREPARED_STATEMENTS_POOL_ENABLED = false;
  public static final int DEFAULT_PREPARED_STATEMENTS_POOL_MAX_OPEN = -1;

  public static final int DEFAULT_TABLE_METADATA_CONNECTION_POOL_MIN_IDLE = 5;
  public static final int DEFAULT_TABLE_METADATA_CONNECTION_POOL_MAX_IDLE = 10;
  public static final int DEFAULT_TABLE_METADATA_CONNECTION_POOL_MAX_TOTAL = 25;

  public static final int DEFAULT_ADMIN_CONNECTION_POOL_MIN_IDLE = 5;
  public static final int DEFAULT_ADMIN_CONNECTION_POOL_MAX_IDLE = 10;
  public static final int DEFAULT_ADMIN_CONNECTION_POOL_MAX_TOTAL = 25;

  // MySQL, Oracle and Db2 have limitations regarding the total size of key columns. Thus, we should
  // set a small but enough key column size so that users can create multiple key columns without
  // exceeding the limit and changing the default. Since we found the old default size of 64 bytes
  // was small for some applications, we changed it based on the following specifications. See the
  // official documents for details.
  // 1) In MySQL, the maximum total size of key columns is 3072 bytes in the default, and thus,
  // depending on the charset, it can be up to 768 characters long. It can further be reduced if the
  // different settings are used.
  // 2) In Oracle, the maximum total size of key columns is approximately 75% of the database block
  // size minus some overhead. The default block size is 8KB, and it is typically 4kB or 8kB. Thus,
  // the maximum size can be similar to MySQL.
  // 3) In Db2, the maximum total size of key columns is indexPageSize/4, since the default page
  // size is 4KB, the default key column size is 1024 bytes. So the maximum size can be similar to
  // MySQL.
  //
  // See the official documents for details.
  // https://dev.mysql.com/doc/refman/8.0/en/innodb-limits.html
  // https://docs.oracle.com/en/database/oracle/oracle-database/23/refrn/logical-database-limits.html
  // https://www.ibm.com/docs/en/db2/12.1.0?topic=sql-xml-limits
  public static final int DEFAULT_VARIABLE_KEY_COLUMN_SIZE = 128;
  // As the partition key `tx_id` of the coordinator state table, we need a UUID string plus a
  // 25-byte prefix for the group commit feature; thus, we set 64 bytes as the minimum.
  public static final int MINIMUM_VARIABLE_KEY_COLUMN_SIZE = 64;

  // In Oracle, there is no data type to only store a time component without a date. So ScalarDB
  // stores every value of the TIME type with the same date component columns for ease of
  // comparison and sorting. The default date component is 1970-01-01.
  public static final String DEFAULT_ORACLE_TIME_COLUMN_DEFAULT_DATE_COMPONENT = "1970-01-01";

  // In Db2, to store a date with a fractional second, the timestamp type is used since Db2 TIME
  // type does not provide fractional seconds. So ScalarDB stores every value of the TIME type with
  // the same date component columns for ease of comparison and sorting. The default date component
  // is 1970-01-01.
  public static final String DEFAULT_DB2_TIME_COLUMN_DEFAULT_DATE_COMPONENT = "1970-01-01";

  private final DatabaseConfig databaseConfig;

  private final String jdbcUrl;
  @Nullable private final String username;
  @Nullable private final String password;

  private final int connectionPoolMinIdle;
  private final int connectionPoolMaxIdle;
  private final int connectionPoolMaxTotal;
  private final boolean preparedStatementsPoolEnabled;
  private final int preparedStatementsPoolMaxOpen;

  @Nullable private final Isolation isolation;

  private final String metadataSchema;
  private final int tableMetadataConnectionPoolMinIdle;
  private final int tableMetadataConnectionPoolMaxIdle;
  private final int tableMetadataConnectionPoolMaxTotal;

  private final int adminConnectionPoolMinIdle;
  private final int adminConnectionPoolMaxIdle;
  private final int adminConnectionPoolMaxTotal;

  private final int mysqlVariableKeyColumnSize;
  private final int oracleVariableKeyColumnSize;
  private final int db2VariableKeyColumnSize;

  private final LocalDate oracleTimeColumnDefaultDateComponent;
  private final LocalDate db2TimeColumnDefaultDateComponent;

  public JdbcConfig(DatabaseConfig databaseConfig) {
    this.databaseConfig = databaseConfig;
    String storage = databaseConfig.getStorage();
    String transactionManager = databaseConfig.getTransactionManager();
    if (!storage.equals(STORAGE_NAME) && !transactionManager.equals(TRANSACTION_MANAGER_NAME)) {
      throw new IllegalArgumentException(
          DatabaseConfig.STORAGE
              + " or "
              + DatabaseConfig.TRANSACTION_MANAGER
              + " should be '"
              + STORAGE_NAME
              + "'");
    }

    if (databaseConfig.getContactPoints().isEmpty()) {
      throw new IllegalArgumentException(CoreError.INVALID_CONTACT_POINTS.buildMessage());
    }
    jdbcUrl = databaseConfig.getContactPoints().get(0);
    username = databaseConfig.getUsername().orElse(null);
    password = databaseConfig.getPassword().orElse(null);

    connectionPoolMinIdle =
        getInt(
            databaseConfig.getProperties(),
            CONNECTION_POOL_MIN_IDLE,
            DEFAULT_CONNECTION_POOL_MIN_IDLE);
    connectionPoolMaxIdle =
        getInt(
            databaseConfig.getProperties(),
            CONNECTION_POOL_MAX_IDLE,
            DEFAULT_CONNECTION_POOL_MAX_IDLE);
    connectionPoolMaxTotal =
        getInt(
            databaseConfig.getProperties(),
            CONNECTION_POOL_MAX_TOTAL,
            DEFAULT_CONNECTION_POOL_MAX_TOTAL);
    preparedStatementsPoolEnabled =
        getBoolean(
            databaseConfig.getProperties(),
            PREPARED_STATEMENTS_POOL_ENABLED,
            DEFAULT_PREPARED_STATEMENTS_POOL_ENABLED);
    preparedStatementsPoolMaxOpen =
        getInt(
            databaseConfig.getProperties(),
            PREPARED_STATEMENTS_POOL_MAX_OPEN,
            DEFAULT_PREPARED_STATEMENTS_POOL_MAX_OPEN);

    String isolationLevel = getString(databaseConfig.getProperties(), ISOLATION_LEVEL, null);
    if (isolationLevel != null) {
      isolation = Isolation.valueOf(isolationLevel.toUpperCase(Locale.ROOT));
    } else {
      isolation = null;
    }

    tableMetadataConnectionPoolMinIdle =
        getInt(
            databaseConfig.getProperties(),
            TABLE_METADATA_CONNECTION_POOL_MIN_IDLE,
            DEFAULT_TABLE_METADATA_CONNECTION_POOL_MIN_IDLE);
    tableMetadataConnectionPoolMaxIdle =
        getInt(
            databaseConfig.getProperties(),
            TABLE_METADATA_CONNECTION_POOL_MAX_IDLE,
            DEFAULT_TABLE_METADATA_CONNECTION_POOL_MAX_IDLE);
    tableMetadataConnectionPoolMaxTotal =
        getInt(
            databaseConfig.getProperties(),
            TABLE_METADATA_CONNECTION_POOL_MAX_TOTAL,
            DEFAULT_TABLE_METADATA_CONNECTION_POOL_MAX_TOTAL);

    adminConnectionPoolMinIdle =
        getInt(
            databaseConfig.getProperties(),
            ADMIN_CONNECTION_POOL_MIN_IDLE,
            DEFAULT_ADMIN_CONNECTION_POOL_MIN_IDLE);
    adminConnectionPoolMaxIdle =
        getInt(
            databaseConfig.getProperties(),
            ADMIN_CONNECTION_POOL_MAX_IDLE,
            DEFAULT_ADMIN_CONNECTION_POOL_MAX_IDLE);
    adminConnectionPoolMaxTotal =
        getInt(
            databaseConfig.getProperties(),
            ADMIN_CONNECTION_POOL_MAX_TOTAL,
            DEFAULT_ADMIN_CONNECTION_POOL_MAX_TOTAL);

    mysqlVariableKeyColumnSize =
        getInt(
            databaseConfig.getProperties(),
            MYSQL_VARIABLE_KEY_COLUMN_SIZE,
            DEFAULT_VARIABLE_KEY_COLUMN_SIZE);

    oracleVariableKeyColumnSize =
        getInt(
            databaseConfig.getProperties(),
            ORACLE_VARIABLE_KEY_COLUMN_SIZE,
            DEFAULT_VARIABLE_KEY_COLUMN_SIZE);

    db2VariableKeyColumnSize =
        getInt(
            databaseConfig.getProperties(),
            DB2_VARIABLE_KEY_COLUMN_SIZE,
            DEFAULT_VARIABLE_KEY_COLUMN_SIZE);

    if (mysqlVariableKeyColumnSize < MINIMUM_VARIABLE_KEY_COLUMN_SIZE
        || oracleVariableKeyColumnSize < MINIMUM_VARIABLE_KEY_COLUMN_SIZE
        || db2VariableKeyColumnSize < MINIMUM_VARIABLE_KEY_COLUMN_SIZE) {
      throw new IllegalArgumentException(CoreError.INVALID_VARIABLE_KEY_COLUMN_SIZE.buildMessage());
    }

    String oracleTimeColumnDefaultDateComponentString =
        getString(
            databaseConfig.getProperties(),
            ORACLE_TIME_COLUMN_DEFAULT_DATE_COMPONENT,
            DEFAULT_ORACLE_TIME_COLUMN_DEFAULT_DATE_COMPONENT);
    assert oracleTimeColumnDefaultDateComponentString != null;
    oracleTimeColumnDefaultDateComponent =
        LocalDate.parse(
            oracleTimeColumnDefaultDateComponentString, DateTimeFormatter.ISO_LOCAL_DATE);
    String db2TimeColumnDefaultDateComponentString =
        getString(
            databaseConfig.getProperties(),
            DB2_TIME_COLUMN_DEFAULT_DATE_COMPONENT,
            DEFAULT_DB2_TIME_COLUMN_DEFAULT_DATE_COMPONENT);
    assert db2TimeColumnDefaultDateComponentString != null;
    db2TimeColumnDefaultDateComponent =
        LocalDate.parse(db2TimeColumnDefaultDateComponentString, DateTimeFormatter.ISO_LOCAL_DATE);

    if (databaseConfig.getProperties().containsKey(TABLE_METADATA_SCHEMA)) {
      logger.warn(
          "The configuration property \""
              + TABLE_METADATA_SCHEMA
              + "\" is deprecated and will be removed in 5.0.0.");

      metadataSchema =
          getString(
              databaseConfig.getProperties(),
              TABLE_METADATA_SCHEMA,
              DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME);
    } else {
      metadataSchema = databaseConfig.getSystemNamespaceName();
    }
  }

  public DatabaseConfig getDatabaseConfig() {
    return databaseConfig;
  }

  public String getJdbcUrl() {
    return jdbcUrl;
  }

  public Optional<String> getUsername() {
    return Optional.ofNullable(username);
  }

  public Optional<String> getPassword() {
    return Optional.ofNullable(password);
  }

  public int getConnectionPoolMinIdle() {
    return connectionPoolMinIdle;
  }

  public int getConnectionPoolMaxIdle() {
    return connectionPoolMaxIdle;
  }

  public int getConnectionPoolMaxTotal() {
    return connectionPoolMaxTotal;
  }

  public boolean isPreparedStatementsPoolEnabled() {
    return preparedStatementsPoolEnabled;
  }

  public int getPreparedStatementsPoolMaxOpen() {
    return preparedStatementsPoolMaxOpen;
  }

  public Optional<Isolation> getIsolation() {
    return Optional.ofNullable(isolation);
  }

  public String getMetadataSchema() {
    return metadataSchema;
  }

  public int getTableMetadataConnectionPoolMinIdle() {
    return tableMetadataConnectionPoolMinIdle;
  }

  public int getTableMetadataConnectionPoolMaxIdle() {
    return tableMetadataConnectionPoolMaxIdle;
  }

  public int getTableMetadataConnectionPoolMaxTotal() {
    return tableMetadataConnectionPoolMaxTotal;
  }

  public int getAdminConnectionPoolMinIdle() {
    return adminConnectionPoolMinIdle;
  }

  public int getAdminConnectionPoolMaxIdle() {
    return adminConnectionPoolMaxIdle;
  }

  public int getAdminConnectionPoolMaxTotal() {
    return adminConnectionPoolMaxTotal;
  }

  public int getMysqlVariableKeyColumnSize() {
    return mysqlVariableKeyColumnSize;
  }

  public int getOracleVariableKeyColumnSize() {
    return oracleVariableKeyColumnSize;
  }

  public int getDb2VariableKeyColumnSize() {
    return db2VariableKeyColumnSize;
  }

  public LocalDate getOracleTimeColumnDefaultDateComponent() {
    return oracleTimeColumnDefaultDateComponent;
  }

  public LocalDate getDb2TimeColumnDefaultDateComponent() {
    return db2TimeColumnDefaultDateComponent;
  }
}
