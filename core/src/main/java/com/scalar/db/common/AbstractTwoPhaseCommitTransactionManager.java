package com.scalar.db.common;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.util.ScalarDbUtils;
import java.util.List;
import java.util.Optional;

public abstract class AbstractTwoPhaseCommitTransactionManager
    implements TwoPhaseCommitTransactionManager {

  private Optional<String> namespace;
  private Optional<String> tableName;

  public AbstractTwoPhaseCommitTransactionManager(DatabaseConfig config) {
    namespace = config.getDefaultNamespaceName();
    tableName = Optional.empty();
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public void with(String namespace, String tableName) {
    this.namespace = Optional.ofNullable(namespace);
    this.tableName = Optional.ofNullable(tableName);
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public void withNamespace(String namespace) {
    this.namespace = Optional.ofNullable(namespace);
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public Optional<String> getNamespace() {
    return namespace;
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public void withTable(String tableName) {
    this.tableName = Optional.ofNullable(tableName);
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public Optional<String> getTable() {
    return tableName;
  }

  protected <T extends Mutation> List<T> copyAndSetTargetToIfNot(List<T> mutations) {
    return ScalarDbUtils.copyAndSetTargetToIfNot(mutations, namespace, tableName);
  }

  protected Get copyAndSetTargetToIfNot(Get get) {
    return ScalarDbUtils.copyAndSetTargetToIfNot(get, namespace, tableName);
  }

  protected Scan copyAndSetTargetToIfNot(Scan scan) {
    return ScalarDbUtils.copyAndSetTargetToIfNot(scan, namespace, tableName);
  }

  protected Put copyAndSetTargetToIfNot(Put put) {
    return ScalarDbUtils.copyAndSetTargetToIfNot(put, namespace, tableName);
  }

  protected Delete copyAndSetTargetToIfNot(Delete delete) {
    return ScalarDbUtils.copyAndSetTargetToIfNot(delete, namespace, tableName);
  }

  protected Insert copyAndSetTargetToIfNot(Insert insert) {
    return ScalarDbUtils.copyAndSetTargetToIfNot(insert, namespace, tableName);
  }

  protected Upsert copyAndSetTargetToIfNot(Upsert upsert) {
    return ScalarDbUtils.copyAndSetTargetToIfNot(upsert, namespace, tableName);
  }

  protected Update copyAndSetTargetToIfNot(Update update) {
    return ScalarDbUtils.copyAndSetTargetToIfNot(update, namespace, tableName);
  }
}
