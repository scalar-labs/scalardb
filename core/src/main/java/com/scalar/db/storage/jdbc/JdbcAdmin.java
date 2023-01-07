package com.scalar.db.storage.jdbc;

import com.google.common.annotations.VisibleForTesting;
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

@SuppressFBWarnings({"OBL_UNSATISFIED_OBLIGATION", "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE"})
@ThreadSafe
public class JdbcAdmin implements DistributedStorageAdmin {
  public static final String METADATA_SCHEMA = "scalardb";
  public static final String METADATA_TABLE = "metadata";

  private final RdbEngineStrategy rdbEngineSt;

  @Inject
  public JdbcAdmin(DatabaseConfig databaseConfig) {
    rdbEngineSt = RdbEngineStrategy.create(databaseConfig);
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public JdbcAdmin(BasicDataSource dataSource, JdbcConfig config) {
    rdbEngineSt = RdbEngineStrategy.create(dataSource, config);
  }

  // TODO remove
  @SuppressFBWarnings("EI_EXPOSE_REP2")
  @VisibleForTesting
  JdbcAdmin(BasicDataSource dataSource, RdbEngine rdbEngine, JdbcConfig config) {
    rdbEngineSt = RdbEngineStrategy.create(dataSource, config);
  }

  @Override
  public void createNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    rdbEngineSt.createNamespace(namespace, options);
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    rdbEngineSt.createTable(namespace, table, metadata, options);
  }

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    rdbEngineSt.dropTable(namespace, table);
  }

  @Override
  public void dropNamespace(String namespace) throws ExecutionException {
    rdbEngineSt.dropNamespace(namespace);
  }

  @Override
  public void truncateTable(String namespace, String table) throws ExecutionException {
    rdbEngineSt.truncateTable(namespace, table);
  }

  @Override
  public TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException {
    return rdbEngineSt.getTableMetadata(namespace, table);
  }

  @Override
  public Set<String> getNamespaceTableNames(String namespace) throws ExecutionException {
    return rdbEngineSt.getNamespaceTableNames(namespace);
  }

  @Override
  public boolean namespaceExists(String namespace) throws ExecutionException {
    return rdbEngineSt.namespaceExists(namespace);
  }

  @Override
  public void close() {
    rdbEngineSt.close();

  }

  @Override
  public void createIndex(
      String namespace, String table, String columnName, Map<String, String> options)
      throws ExecutionException {
    rdbEngineSt.createIndex(namespace, table, columnName, options);
  }


  @Override
  public void dropIndex(String namespace, String table, String columnName)
      throws ExecutionException {
    rdbEngineSt.dropIndex(namespace, table, columnName);
  }


  @Override
  public void repairTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    rdbEngineSt.repairTable(namespace, table, metadata, options);
  }

  @Override
  public void addNewColumnToTable(
      String namespace, String table, String columnName, DataType columnType)
      throws ExecutionException {
    rdbEngineSt.addNewColumnToTable(namespace, table, columnName, columnType);
  }
}