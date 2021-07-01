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
import com.scalar.db.util.Utility;
import java.util.List;
import java.util.Optional;

public class GrpcTransaction implements DistributedTransaction {

  private final String txId;
  private final GrpcTransactionService service;

  private Optional<String> namespace;
  private Optional<String> tableName;

  public GrpcTransaction(
      String txId,
      GrpcTransactionService service,
      Optional<String> namespace,
      Optional<String> tableName) {
    this.txId = txId;
    this.service = service;
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
    Utility.setTargetToIfNot(get, namespace, tableName);
    return service.get(get);
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException {
    Utility.setTargetToIfNot(scan, namespace, tableName);
    return service.scan(scan);
  }

  @Override
  public void put(Put put) throws CrudException {
    Utility.setTargetToIfNot(put, namespace, tableName);
    service.mutate(put);
  }

  @Override
  public void put(List<Put> puts) throws CrudException {
    mutate(puts);
  }

  @Override
  public void delete(Delete delete) throws CrudException {
    Utility.setTargetToIfNot(delete, namespace, tableName);
    service.mutate(delete);
  }

  @Override
  public void delete(List<Delete> deletes) throws CrudException {
    mutate(deletes);
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws CrudException {
    Utility.setTargetToIfNot(mutations, namespace, tableName);
    service.mutate(mutations);
  }

  @Override
  public void commit() throws CommitException, UnknownTransactionStatusException {
    service.commit();
  }

  @Override
  public void abort() throws AbortException {
    service.abort();
  }
}
