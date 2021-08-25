package com.scalar.db.storage.jdbc.test;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.storage.jdbc.JdbcConfig;
import com.scalar.db.storage.jdbc.JdbcDatabaseAdmin;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

@SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
public class TestEnv implements Closeable {

  private static final String PROP_JDBC_URL = "scalardb.jdbc.url";
  private static final String PROP_JDBC_USERNAME = "scalardb.jdbc.username";
  private static final String PROP_JDBC_PASSWORD = "scalardb.jdbc.password";
  private static final String PROP_NAMESPACE_PREFIX = "scalardb.namespace_prefix";
  private final JdbcConfig config;

  private final List<String> schemaList;
  private final List<String> tableList;

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
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, jdbcUrl);
    props.setProperty(DatabaseConfig.USERNAME, username);
    props.setProperty(DatabaseConfig.PASSWORD, password);
    props.setProperty(DatabaseConfig.STORAGE, "jdbc");
    namespacePrefix.ifPresent(s -> props.setProperty(DatabaseConfig.NAMESPACE_PREFIX, s));
    config = new JdbcConfig(props);

    schemaList = new ArrayList<>();
    tableList = new ArrayList<>();
    jdbcAdmin = new JdbcDatabaseAdmin(config);
  }

  public void createTable(String schema, String table, TableMetadata metadata)
      throws ExecutionException {
    schemaList.add(schema);
    tableList.add(table);
    jdbcAdmin.createNamespace(schema);
    jdbcAdmin.createTable(schema, table, metadata);
  }

  public void deleteTableData() throws ExecutionException {
    for (int i = 0; i < tableList.size(); i++) {
      jdbcAdmin.truncateTable(schemaList.get(i), tableList.get(i));
    }
  }

  public void deleteTables() throws Exception {
    for (int i = 0; i < tableList.size(); i++) {
      jdbcAdmin.dropTable(schemaList.get(i), tableList.get(i));
    }
    for (String namespace : new HashSet<>(schemaList)) {
      jdbcAdmin.dropNamespace(namespace);
    }
  }

  public JdbcConfig getJdbcConfig() {
    return config;
  }

  @Override
  public void close() throws IOException {
    jdbcAdmin.close();
  }
}
