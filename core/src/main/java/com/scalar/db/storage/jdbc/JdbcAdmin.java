package com.scalar.db.storage.jdbc;

import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.dbcp2.BasicDataSource;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Map;
import java.util.Set;

@ThreadSafe
public class JdbcAdmin implements DistributedStorageAdmin {
  public static final String METADATA_SCHEMA = "scalardb";
  public static final String METADATA_TABLE = "metadata";

  private final RdbEngineStrategy rdbEngine;

  @Inject
  public JdbcAdmin(DatabaseConfig databaseConfig) {
    rdbEngine = RdbEngineStrategy.create(databaseConfig);
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public JdbcAdmin(BasicDataSource dataSource, JdbcConfig config) {
    rdbEngine = RdbEngineStrategy.create(dataSource, config);
  }

  @Override
  public void createNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    rdbEngine.createNamespace(namespace, options);
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    rdbEngine.createTable(namespace, table, metadata, options);
  }

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    rdbEngine.dropTable(namespace, table);
  }

  @Override
  public void dropNamespace(String namespace) throws ExecutionException {
    rdbEngine.dropNamespace(namespace);
  }

  @Override
  public void truncateTable(String namespace, String table) throws ExecutionException {
    rdbEngine.truncateTable(namespace, table);
  }

  @Override
  public TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException {
    return rdbEngine.getTableMetadata(namespace, table);
  }

  @Override
  public Set<String> getNamespaceTableNames(String namespace) throws ExecutionException {
    return rdbEngine.getNamespaceTableNames(namespace);
  }

  @Override
  public boolean namespaceExists(String namespace) throws ExecutionException {
    return rdbEngine.namespaceExists(namespace);
  }

  @Override
  public void close() {
    rdbEngine.close();

  }

  @Override
  public void createIndex(
      String namespace, String table, String columnName, Map<String, String> options)
      throws ExecutionException {
    rdbEngine.createIndex(namespace, table, columnName, options);
  }


  @Override
  public void dropIndex(String namespace, String table, String columnName)
      throws ExecutionException {
    rdbEngine.dropIndex(namespace, table, columnName);
  }


  @Override
  public void repairTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    rdbEngine.repairTable(namespace, table, metadata, options);
  }

  @Override
  public void addNewColumnToTable(
      String namespace, String table, String columnName, DataType columnType)
      throws ExecutionException {
    rdbEngine.addNewColumnToTable(namespace, table, columnName, columnType);
  }
}