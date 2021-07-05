package com.scalar.db.service;

import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.Map;

public class AdminService implements DistributedStorageAdmin {

  private final DistributedStorageAdmin admin;

  @Inject
  public AdminService(DistributedStorageAdmin admin) {
    this.admin = admin;
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    admin.createTable(namespace, table, metadata, options);
  }

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    admin.dropTable(namespace, table);
  }

  @Override
  public void truncateTable(String namespace, String table) throws ExecutionException {
    admin.truncateTable(namespace, table);
  }

  @Override
  public TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException {
    return admin.getTableMetadata(namespace, table);
  }

  @Override
  public void close() {
    admin.close();
  }
}
