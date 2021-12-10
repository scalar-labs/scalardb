package com.scalar.db.transaction.rpc;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.util.ScalarDbUtils;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class GrpcTransaction implements DistributedTransaction {

  private final String txId;
  private final GrpcTransactionOnBidirectionalStream stream;

  private Optional<String> namespace;
  private Optional<String> tableName;

  public GrpcTransaction(
      String txId,
      GrpcTransactionOnBidirectionalStream stream,
      Optional<String> namespace,
      Optional<String> tableName) {
    this.txId = txId;
    this.stream = stream;
    this.namespace = namespace;
    this.tableName = tableName;
  }

  @Override
  public String getId() {
    return txId;
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

  @Override
  public Optional<Result> get(Get get) throws CrudException {
    ScalarDbUtils.setTargetToIfNot(get, namespace, tableName);
    return stream.get(get);
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException {
    ScalarDbUtils.setTargetToIfNot(scan, namespace, tableName);
    return stream.scan(scan);
  }

  @Override
  public void put(Put put) throws CrudException {
    ScalarDbUtils.setTargetToIfNot(put, namespace, tableName);
    stream.mutate(put);
  }

  @Override
  public void put(List<Put> puts) throws CrudException {
    mutate(puts);
  }

  @Override
  public void delete(Delete delete) throws CrudException {
    ScalarDbUtils.setTargetToIfNot(delete, namespace, tableName);
    stream.mutate(delete);
  }

  @Override
  public void delete(List<Delete> deletes) throws CrudException {
    mutate(deletes);
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws CrudException {
    ScalarDbUtils.setTargetToIfNot(mutations, namespace, tableName);
    stream.mutate(mutations);
  }

  @Override
  public void commit() throws CommitException, UnknownTransactionStatusException {
    stream.commit();
  }

  @Override
  public void abort() throws AbortException {
    stream.abort();
  }
}
