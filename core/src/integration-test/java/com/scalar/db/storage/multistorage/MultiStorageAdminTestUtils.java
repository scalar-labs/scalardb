package com.scalar.db.storage.multistorage;

import static com.datastax.driver.core.Metadata.quoteIfNecessary;
import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.cassandra.CassandraAdmin;
import com.scalar.db.storage.cassandra.CassandraConfig;
import com.scalar.db.storage.cassandra.ClusterManager;
import com.scalar.db.storage.jdbc.JdbcAdmin;
import com.scalar.db.storage.jdbc.JdbcConfig;
import com.scalar.db.storage.jdbc.JdbcTestUtils;
import com.scalar.db.storage.jdbc.JdbcUtils;
import com.scalar.db.storage.jdbc.RdbEngineFactory;
import com.scalar.db.storage.jdbc.RdbEngineStrategy;
import com.scalar.db.util.AdminTestUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.apache.commons.dbcp2.BasicDataSource;

public class MultiStorageAdminTestUtils extends AdminTestUtils {
  // for Cassandra
  private final ClusterManager clusterManager;
  private final String cassandraMetadataNamespace;
  // for JDBC
  private final String jdbcMetadataSchema;
  private final RdbEngineStrategy rdbEngine;
  private final BasicDataSource dataSource;

  public MultiStorageAdminTestUtils(Properties cassandraProperties, Properties jdbcProperties) {
    // Cassandra has the coordinator tables
    super(cassandraProperties);
    CassandraConfig cassandraConfig = new CassandraConfig(new DatabaseConfig(cassandraProperties));
    clusterManager = new ClusterManager(new DatabaseConfig(cassandraProperties));
    cassandraMetadataNamespace = cassandraConfig.getMetadataKeyspace();

    // for JDBC
    JdbcConfig jdbcConfig = new JdbcConfig(new DatabaseConfig(jdbcProperties));
    jdbcMetadataSchema = jdbcConfig.getMetadataSchema();
    rdbEngine = RdbEngineFactory.create(jdbcConfig);
    dataSource = JdbcUtils.initDataSourceForAdmin(jdbcConfig, rdbEngine);
  }

  @Override
  public void dropNamespacesTable() throws SQLException {
    // for Cassandra
    String dropTableQuery =
        SchemaBuilder.dropTable(
                quoteIfNecessary(cassandraMetadataNamespace),
                quoteIfNecessary(CassandraAdmin.NAMESPACES_TABLE))
            .getQueryString();
    clusterManager.getSession().execute(dropTableQuery);

    // for JDBC
    execute(
        "DROP TABLE "
            + rdbEngine.encloseFullTableName(jdbcMetadataSchema, JdbcAdmin.NAMESPACES_TABLE));
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
  public void truncateNamespacesTable() throws SQLException {
    // for Cassandra
    String truncateTableQuery =
        QueryBuilder.truncate(
                quoteIfNecessary(cassandraMetadataNamespace),
                quoteIfNecessary(CassandraAdmin.NAMESPACES_TABLE))
            .getQueryString();
    clusterManager.getSession().execute(truncateTableQuery);

    // for JDBC
    String truncateTableStatement =
        rdbEngine.truncateTableSql(jdbcMetadataSchema, JdbcAdmin.NAMESPACES_TABLE);
    execute(truncateTableStatement);
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

  @Override
  public void dropNamespace(String namespace) throws SQLException {
    boolean existsOnCassandra = namespaceExistsOnCassandra(namespace);
    boolean existsOnJdbc = namespaceExistsOnJdbc(namespace);

    if (existsOnCassandra && existsOnJdbc) {
      throw new IllegalStateException(
          "The " + namespace + " namespace should not exist on both storages");
    } else if (!(existsOnCassandra || existsOnJdbc)) {
      throw new IllegalStateException(
          "The " + namespace + " namespace does not exist on both storages");
    }
    if (existsOnCassandra) {
      String dropKeyspaceQuery =
          SchemaBuilder.dropKeyspace(quoteIfNecessary(namespace)).getQueryString();
      clusterManager.getSession().execute(dropKeyspaceQuery);
    } else {
      execute(rdbEngine.dropNamespaceSql(namespace));
    }
  }

  @Override
  public boolean namespaceExists(String namespace) throws SQLException {
    boolean existsOnCassandra = namespaceExistsOnCassandra(namespace);
    boolean existsOnJdbc = namespaceExistsOnJdbc(namespace);

    if (existsOnCassandra && existsOnJdbc) {
      throw new IllegalStateException(
          "The " + namespace + " namespace should not exist on both storages");
    }
    return existsOnCassandra || existsOnJdbc;
  }

  private boolean namespaceExistsOnCassandra(String namespace) {
    return clusterManager.getSession().getCluster().getMetadata().getKeyspace(namespace) != null;
  }

  private boolean namespaceExistsOnJdbc(String namespace) throws SQLException {
    String sql;
    // RdbEngine classes are not publicly exposed, so we test the type using hard coded class name
    if (JdbcTestUtils.isMysql(rdbEngine)) {
      sql = "SELECT 1 FROM information_schema.schemata WHERE schema_name = ?";
    } else if (JdbcTestUtils.isPostgresql(rdbEngine)) {
      sql = "SELECT 1 FROM pg_namespace WHERE nspname = ?";
    } else if (JdbcTestUtils.isOracle(rdbEngine)) {
      sql = "SELECT 1 FROM all_users WHERE username = ?";
    } else if (JdbcTestUtils.isSqlServer(rdbEngine)) {
      sql = "SELECT 1 FROM sys.schemas WHERE name = ?";
    } else if (JdbcTestUtils.isSqlite(rdbEngine)) {
      // SQLite has no concept of namespace
      return true;
    } else {
      throw new AssertionError("Unsupported engine : " + rdbEngine.getClass().getSimpleName());
    }

    try (Connection connection = dataSource.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
      preparedStatement.setString(1, namespace);
      ResultSet resultSet = preparedStatement.executeQuery();

      return resultSet.next();
    }
  }

  private void execute(String sql) throws SQLException {
    try (Connection connection = dataSource.getConnection();
        Statement stmt = connection.createStatement()) {
      stmt.execute(sql);
    }
  }

  @Override
  public boolean tableExists(String namespace, String table) throws Exception {
    boolean existsOnCassandra = tableExistsOnCassandra(namespace, table);
    boolean existsOnJdbc = tableExistsOnJdbc(namespace, table);
    if (existsOnCassandra && existsOnJdbc) {
      throw new IllegalStateException(
          String.format(
              "The %s table should not exist on both storages",
              getFullTableName(namespace, table)));
    }
    return existsOnCassandra || existsOnJdbc;
  }

  private boolean tableExistsOnCassandra(String namespace, String table) {
    return clusterManager.getMetadata(namespace, table) != null;
  }

  private boolean tableExistsOnJdbc(String namespace, String table) throws Exception {
    String fullTableName = rdbEngine.encloseFullTableName(namespace, table);
    String sql = rdbEngine.tableExistsInternalTableCheckSql(fullTableName);
    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement()) {
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
    boolean existsOnCassandra = tableExistsOnCassandra(namespace, table);
    boolean existsOnJdbc = tableExistsOnJdbc(namespace, table);

    if (existsOnCassandra && existsOnJdbc) {
      throw new IllegalStateException(
          String.format(
              "The %s table should not exist on both storages",
              getFullTableName(namespace, table)));
    } else if (!(existsOnCassandra || existsOnJdbc)) {
      throw new IllegalStateException(
          String.format(
              "The %s table does not exist on both storages", getFullTableName(namespace, table)));
    }

    if (existsOnCassandra) {
      String dropTableQuery =
          SchemaBuilder.dropTable(quoteIfNecessary(namespace), quoteIfNecessary(table))
              .getQueryString();
      clusterManager.getSession().execute(dropTableQuery);
    } else {
      String dropTableStatement = "DROP TABLE " + rdbEngine.encloseFullTableName(namespace, table);
      execute(dropTableStatement);
    }
  }

  @Override
  public void close() throws SQLException {
    clusterManager.close();
    dataSource.close();
  }
}
