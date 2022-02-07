package com.scalar.db.transaction.common;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TwoPhaseCommitTransaction;
import com.scalar.db.util.ScalarDbUtils;
import java.util.List;
import java.util.Optional;

public abstract class AbstractTwoPhaseCommitTransaction implements TwoPhaseCommitTransaction {

  private Optional<String> namespace;
  private Optional<String> tableName;

  public AbstractTwoPhaseCommitTransaction() {
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
    return ScalarDbUtils.setTargetToIfNot(mutations, namespace, tableName, true);
  }

  protected Get setTargetToIfNot(Get get) {
    return ScalarDbUtils.setTargetToIfNot(get, namespace, tableName, true);
  }

  protected Scan setTargetToIfNot(Scan scan) {
    return ScalarDbUtils.setTargetToIfNot(scan, namespace, tableName, true);
  }

  protected Put setTargetToIfNot(Put put) {
    return ScalarDbUtils.setTargetToIfNot(put, namespace, tableName, true);
  }

  protected Delete setTargetToIfNot(Delete delete) {
    return ScalarDbUtils.setTargetToIfNot(delete, namespace, tableName, true);
  }
}
