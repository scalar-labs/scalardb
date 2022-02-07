package com.scalar.db.storage.common;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.util.ScalarDbUtils;
import java.util.List;
import java.util.Optional;

public abstract class AbstractDistributedStorage implements DistributedStorage {

  private final boolean needOperationCopy;
  private Optional<String> namespace;
  private Optional<String> tableName;

  public AbstractDistributedStorage(DatabaseConfig config) {
    needOperationCopy = config.needOperationCopy();
    namespace = Optional.empty();
    tableName = Optional.empty();
  }

  @Override
  public void with(String namespace, String tableName) {
    this.namespace = Optional.ofNullable(namespace);
    this.tableName = Optional.ofNullable(tableName);
  }

  @Override
  public void withNamespace(String namespace) {
    this.namespace = Optional.ofNullable(namespace);
  }

  @Override
  public Optional<String> getNamespace() {
    return namespace;
  }

  @Override
  public void withTable(String tableName) {
    this.tableName = Optional.ofNullable(tableName);
  }

  @Override
  public Optional<String> getTable() {
    return tableName;
  }

  protected <T extends Mutation> List<T> setTargetToIfNot(List<T> mutations) {
    return ScalarDbUtils.setTargetToIfNot(mutations, namespace, tableName, needOperationCopy);
  }

  protected Get setTargetToIfNot(Get get) {
    return ScalarDbUtils.setTargetToIfNot(get, namespace, tableName, needOperationCopy);
  }

  protected Scan setTargetToIfNot(Scan scan) {
    return ScalarDbUtils.setTargetToIfNot(scan, namespace, tableName, needOperationCopy);
  }

  protected Put setTargetToIfNot(Put put) {
    return ScalarDbUtils.setTargetToIfNot(put, namespace, tableName, needOperationCopy);
  }

  protected Delete setTargetToIfNot(Delete delete) {
    return ScalarDbUtils.setTargetToIfNot(delete, namespace, tableName, needOperationCopy);
  }
}
