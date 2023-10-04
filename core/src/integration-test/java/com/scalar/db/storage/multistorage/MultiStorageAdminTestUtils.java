package com.scalar.db.storage.multistorage;

import static com.datastax.driver.core.Metadata.quoteIfNecessary;
import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.cassandra.ClusterManager;
import com.scalar.db.storage.jdbc.JdbcAdmin;
import com.scalar.db.storage.jdbc.JdbcConfig;
import com.scalar.db.storage.jdbc.JdbcUtils;
import com.scalar.db.storage.jdbc.RdbEngineFactory;
import com.scalar.db.storage.jdbc.RdbEngineStrategy;
import com.scalar.db.util.AdminTestUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.apache.commons.dbcp2.BasicDataSource;

public class MultiStorageAdminTestUtils extends AdminTestUtils {
  // for Cassandra
  private final ClusterManager clusterManager;
  // for JDBC
  private final JdbcConfig jdbcConfig;
  private final String jdbcMetadataSchema;
  private final RdbEngineStrategy rdbEngine;

  public MultiStorageAdminTestUtils(Properties cassandraProperties, Properties jdbcProperties) {
    // Cassandra has the coordinator tables
    super(cassandraProperties);
    clusterManager = new ClusterManager(new DatabaseConfig(cassandraProperties));

    // for JDBC
    jdbcConfig = new JdbcConfig(new DatabaseConfig(jdbcProperties));
    jdbcMetadataSchema = jdbcConfig.getMetadataSchema().orElse(JdbcAdmin.METADATA_SCHEMA);
    rdbEngine = RdbEngineFactory.create(jdbcConfig);
  }

  @Override
  public void dropMetadataTable() throws SQLException {
    // Do nothing for Cassandra

    // for JDBC
    execute(
        "DROP TABLE "
            + rdbEngine.encloseFullTableName(jdbcMetadataSchema, JdbcAdmin.METADATA_TABLE));
  }

  @Override
  public void truncateMetadataTable() throws Exception {
    // Do nothing for Cassandra

    // for JDBC
    String truncateTableStatement =
        "TRUNCATE TABLE "
            + rdbEngine.encloseFullTableName(jdbcMetadataSchema, JdbcAdmin.METADATA_TABLE);
    execute(truncateTableStatement);
  }

  @Override
  @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
  public void corruptMetadata(String namespace, String table) throws Exception {
    // Do nothing for Cassandra

    // for JDBC
    String insertCorruptedMetadataStatement =
        "INSERT INTO "
            + rdbEngine.encloseFullTableName(jdbcMetadataSchema, JdbcAdmin.METADATA_TABLE)
            + " VALUES ('"
            + getFullTableName(namespace, table)
            + "','corrupted','corrupted','corrupted','corrupted','0','0')";
    execute(insertCorruptedMetadataStatement);
  }

  private void execute(String sql) throws SQLException {
    try (BasicDataSource dataSource = JdbcUtils.initDataSourceForAdmin(jdbcConfig, rdbEngine);
        Connection connection = dataSource.getConnection();
        Statement stmt = connection.createStatement()) {
      stmt.execute(sql);
    }
  }

  @Override
  public boolean tableExists(String namespace, String table) throws Exception {
    boolean existsOnCassandraStorage = clusterManager.getMetadata(namespace, table) != null;
    boolean existsOnJdbcStorage = tableExistsOnJdbc(namespace, table);
    if (existsOnCassandraStorage && existsOnJdbcStorage) {
      throw new IllegalStateException(
          String.format(
              "The %s table should not exists on both storages",
              getFullTableName(namespace, table)));
    }
    return existsOnCassandraStorage || existsOnJdbcStorage;
  }

  private boolean tableExistsOnCassandra(String namespace, String table) {
    return clusterManager.getMetadata(namespace, table) != null;
  }

  private boolean tableExistsOnJdbc(String namespace, String table) throws Exception {
    String fullTableName = rdbEngine.encloseFullTableName(namespace, table);
    String sql = rdbEngine.tableExistsInternalTableCheckSql(fullTableName);
    try (BasicDataSource dataSource = JdbcUtils.initDataSourceForAdmin(jdbcConfig, rdbEngine);
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement(); ) {
      statement.execute(sql);
      return true;
    } catch (SQLException e) {
      // An exception will be thrown if the table does not exist when executing the select
      // query
      if (rdbEngine.isUndefinedTableError(e)) {
        return false;
      }
      throw new Exception(
          String.format(
              "Checking if the %s table exists failed", getFullTableName(namespace, table)),
          e);
    }
  }

  @Override
  public void dropTable(String namespace, String table) throws Exception {
    boolean tableExistsOnCassandra = tableExistsOnCassandra(namespace, table);
    boolean tableExistsOnJdbc = tableExistsOnJdbc(namespace, table);

    if (tableExistsOnCassandra && tableExistsOnJdbc) {
      throw new IllegalStateException(
          String.format(
              "The %s table should not exist on both storages",
              getFullTableName(namespace, table)));
    } else if (!(tableExistsOnCassandra || tableExistsOnJdbc)) {
      throw new IllegalStateException(
          String.format(
              "The %s table does not exist on both storages", getFullTableName(namespace, table)));
    }

    if (tableExistsOnCassandra) {
      String dropTableQuery =
          SchemaBuilder.dropTable(quoteIfNecessary(namespace), quoteIfNecessary(table))
              .getQueryString();
      clusterManager.getSession().execute(dropTableQuery);
    } else {
      String dropTableStatement = "DROP TABLE " + rdbEngine.encloseFullTableName(namespace, table);
      execute(dropTableStatement);
    }
  }
}
