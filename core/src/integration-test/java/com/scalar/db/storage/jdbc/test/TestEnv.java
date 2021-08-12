package com.scalar.db.storage.jdbc.test;

import static com.scalar.db.util.Utility.getFullNamespaceName;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.storage.jdbc.JdbcConfig;
import com.scalar.db.storage.jdbc.JdbcDatabaseAdmin;
import com.scalar.db.storage.jdbc.JdbcTableMetadataManager;
import com.scalar.db.storage.jdbc.JdbcUtils;
import com.scalar.db.storage.jdbc.RdbEngine;
import com.scalar.db.storage.jdbc.query.QueryUtils;
import com.scalar.db.util.Utility;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.apache.commons.dbcp2.BasicDataSource;

@SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
public class TestEnv implements Closeable {

  private static final String PROP_JDBC_URL = "scalardb.jdbc.url";
  private static final String PROP_JDBC_USERNAME = "scalardb.jdbc.username";
  private static final String PROP_JDBC_PASSWORD = "scalardb.jdbc.password";
  private static final String PROP_NAMESPACE_PREFIX = "scalardb.namespace_prefix";

  private final RdbEngine rdbEngine;
  private final BasicDataSource dataSource;
  private final JdbcConfig config;

  private final List<String> schemaList;
  private final List<String> tableList;
  private final List<TableMetadata> metadataList;

  private final JdbcDatabaseAdmin jdbcAdmin;

  public TestEnv() {
    this(
        System.getProperty(PROP_JDBC_URL),
        System.getProperty(PROP_JDBC_USERNAME, ""),
        System.getProperty(PROP_JDBC_PASSWORD, ""),
        Optional.ofNullable(System.getProperty(PROP_NAMESPACE_PREFIX)));
  }

  public TestEnv(
      String jdbcUrl, String username, String password, Optional<String> namespacePrefix) {
    rdbEngine = JdbcUtils.getRdbEngine(jdbcUrl);

    dataSource = new BasicDataSource();
    dataSource.setUrl(jdbcUrl);
    dataSource.setUsername(username);
    dataSource.setPassword(password);
    dataSource.setMinIdle(5);
    dataSource.setMaxIdle(10);
    dataSource.setMaxTotal(25);

    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, jdbcUrl);
    props.setProperty(DatabaseConfig.USERNAME, username);
    props.setProperty(DatabaseConfig.PASSWORD, password);
    props.setProperty(DatabaseConfig.STORAGE, "jdbc");
    namespacePrefix.ifPresent(s -> props.setProperty(DatabaseConfig.NAMESPACE_PREFIX, s));
    config = new JdbcConfig(props);

    schemaList = new ArrayList<>();
    tableList = new ArrayList<>();
    metadataList = new ArrayList<>();
    jdbcAdmin = new JdbcDatabaseAdmin(config);
  }

  public void createTable(String schema, String table, TableMetadata metadata)
      throws ExecutionException {
    schemaList.add(schema);
    tableList.add(table);
    metadataList.add(metadata);
    jdbcAdmin.createTable(schema, table, metadata, new HashMap<>());
  }

  private void deleteMetadataSchemaAndTable() throws SQLException {
    String enclosedFullMetadataTableName =
        QueryUtils.enclosedFullTableName(
            Utility.getFullNamespaceName(
                config.getNamespacePrefix(), JdbcTableMetadataManager.SCHEMA),
            JdbcTableMetadataManager.TABLE,
            rdbEngine);
    execute("DROP TABLE " + enclosedFullMetadataTableName);

    String enclosedFullMetadataSchema =
        QueryUtils.enclose(
            getFullNamespaceName(config.getNamespacePrefix(), JdbcTableMetadataManager.SCHEMA),
            rdbEngine);
    if (rdbEngine == RdbEngine.ORACLE) {
      execute("DROP USER " + enclosedFullMetadataSchema);
    } else {
      execute("DROP SCHEMA " + enclosedFullMetadataSchema);
    }
  }

  @SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
  private void execute(String sql) throws SQLException {
    try (Connection connection = dataSource.getConnection();
        Statement stmt = connection.createStatement()) {
      stmt.execute(sql);
    }
  }

  public void deleteTableData() throws ExecutionException {
    for (int i = 0; i < metadataList.size(); i++) {
      jdbcAdmin.truncateTable(schemaList.get(i), tableList.get(i));
    }
  }

  public void deleteTables() throws Exception {
    for (int i = 0; i < metadataList.size(); i++) {
      jdbcAdmin.dropTable(schemaList.get(i), tableList.get(i));
    }
  }

  public JdbcConfig getJdbcConfig() {
    return config;
  }

  @Override
  public void close() throws IOException {
    try {
      dataSource.close();
      jdbcAdmin.close();
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }
}
