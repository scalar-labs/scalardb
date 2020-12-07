package com.scalar.db.api;

import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import java.util.List;
import java.util.Optional;

public interface DistributedTransaction {

  String getId();

  void with(String namespace, String tableName);

  void withNamespace(String namespace);

  Optional<String> getNamespace();

  void withTable(String tableName);

  Optional<String> getTable();

  Optional<Result> get(Get get) throws CrudException;

  List<Result> scan(Scan scan) throws CrudException;

  void put(Put put) throws CrudException;

  void put(List<Put> puts) throws CrudException;

  void delete(Delete delete) throws CrudException;

  void delete(List<Delete> deletes) throws CrudException;

  void mutate(List<? extends Mutation> mutations) throws CrudException;

  void commit() throws CommitException, UnknownTransactionStatusException;

  void abort() throws AbortException;
}
