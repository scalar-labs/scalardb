package com.scalar.db.storage.multistorage;

import static com.datastax.driver.core.Metadata.quoteIfNecessary;
import static com.scalar.db.storage.jdbc.query.QueryUtils.enclosedFullTableName;
import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.cassandra.CassandraAdmin;
import com.scalar.db.storage.cassandra.CassandraConfig;
import com.scalar.db.storage.cassandra.ClusterManager;
import com.scalar.db.storage.jdbc.JdbcAdmin;
import com.scalar.db.storage.jdbc.JdbcConfig;
import com.scalar.db.storage.jdbc.JdbcUtils;
import com.scalar.db.storage.jdbc.RdbEngine;
import com.scalar.db.util.AdminTestUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.apache.commons.dbcp2.BasicDataSource;

public class MultiStorageAdminTestUtils extends AdminTestUtils {
  // for Cassandra
  private final String cassandraMetadataKeyspace;
  private final ClusterManager clusterManager;
  // for JDBC
  private final JdbcConfig jdbcConfig;
  private final String jdbcMetadataSchema;
  private final RdbEngine rdbEngine;

  public MultiStorageAdminTestUtils(Properties cassandraProperties, Properties jdbcProperties) {
    // Cassandra has the coordinator tables
    super(cassandraProperties);

    // for Cassandra
    DatabaseConfig databaseConfig = new DatabaseConfig(cassandraProperties);
    CassandraConfig cassandraConfig = new CassandraConfig(databaseConfig);
    cassandraMetadataKeyspace =
        cassandraConfig.getMetadataKeyspace().orElse(CassandraAdmin.METADATA_KEYSPACE);
    clusterManager = new ClusterManager(databaseConfig);

    // for JDBC
    jdbcConfig = new JdbcConfig(new DatabaseConfig(jdbcProperties));
    jdbcMetadataSchema = jdbcConfig.getMetadataSchema().orElse(JdbcAdmin.METADATA_SCHEMA);
    rdbEngine = JdbcUtils.getRdbEngine(jdbcConfig.getJdbcUrl());
  }

  @Override
  public void dropMetadataTable() throws SQLException {
    // Do nothing for Cassandra

    // for JDBC
    execute(
        "DROP TABLE "
            + enclosedFullTableName(jdbcMetadataSchema, JdbcAdmin.METADATA_TABLE, rdbEngine));
  }

  @Override
  public void truncateMetadataTable() throws Exception {
    // Do nothing for Cassandra

    // for JDBC
    String truncateTableStatement =
        "TRUNCATE TABLE "
            + enclosedFullTableName(jdbcMetadataSchema, JdbcAdmin.METADATA_TABLE, rdbEngine);
    execute(truncateTableStatement);
  }

  @Override
  @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
  public void corruptMetadata(String namespace, String table) throws Exception {
    // Do nothing for Cassandra

    // for JDBC
    String insertCorruptedMetadataStatement =
        "INSERT INTO "
            + enclosedFullTableName(jdbcMetadataSchema, JdbcAdmin.METADATA_TABLE, rdbEngine)
            + " VALUES ('"
            + getFullTableName(namespace, table)
            + "','corrupted','corrupted','corrupted','corrupted','0','0')";
    execute(insertCorruptedMetadataStatement);
  }

  @Override
  public void dropNamespacesTable() throws Exception {
    // for Cassandra
    String dropKeyspaceQuery =
        SchemaBuilder.dropKeyspace(quoteIfNecessary(cassandraMetadataKeyspace)).getQueryString();
    clusterManager.getSession().execute(dropKeyspaceQuery);

    // for JDBC
    execute(
        "DROP TABLE "
            + enclosedFullTableName(jdbcMetadataSchema, JdbcAdmin.NAMESPACES_TABLE, rdbEngine));
  }

  private void execute(String sql) throws SQLException {
    try (BasicDataSource dataSource = JdbcUtils.initDataSourceForAdmin(jdbcConfig);
        Connection connection = dataSource.getConnection();
        Statement stmt = connection.createStatement()) {
      stmt.execute(sql);
    }
  }
}
