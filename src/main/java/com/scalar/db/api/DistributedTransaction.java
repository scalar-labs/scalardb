package com.scalar.db.api;

import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import java.util.List;
import java.util.Optional;

public interface DistributedTransaction {

  String getId();

  void with(String namespace, String tableName);

  Optional<Result> get(Get get) throws CrudException;

  List<Result> scan(Scan scan) throws CrudException;

  void put(Put put);

  void put(List<Put> puts);

  void delete(Delete delete);

  void delete(List<Delete> deletes);

  void mutate(List<? extends Mutation> mutations);

  void commit() throws CommitException, UnknownTransactionStatusException;

  // TODO : maybe not needed
  void abort();
}
