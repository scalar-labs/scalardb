package com.scalar.db.transaction.rpc;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.rpc.TransactionRequest;
import com.scalar.db.rpc.TransactionRequest.Command;
import com.scalar.db.rpc.TransactionRequest.Mutations;
import com.scalar.db.rpc.TransactionResponse;
import com.scalar.db.rpc.TransactionResponse.Error.ErrorCode;
import com.scalar.db.storage.rpc.GrpcTableMetadataManager;
import com.scalar.db.util.ProtoUtil;
import com.scalar.db.util.ThrowableSupplier;
import com.scalar.db.util.Utility;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class GrpcTransaction implements DistributedTransaction {

  private final String txId;
  private final StreamObserver<TransactionRequest> transaction;
  private final GrpcTransactionStreamObserver observer;
  private final GrpcTableMetadataManager metadataManager;

  private Optional<String> namespace;
  private Optional<String> tableName;

  public GrpcTransaction(
      String txId,
      StreamObserver<TransactionRequest> transaction,
      GrpcTransactionStreamObserver observer,
      GrpcTableMetadataManager metadataManager,
      Optional<String> namespace,
      Optional<String> tableName) {
    this.txId = txId;
    this.transaction = transaction;
    this.observer = observer;
    this.metadataManager = metadataManager;
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
    return executeCrud(
        () -> {
          Utility.setTargetToIfNot(get, namespace, tableName);

          TransactionResponse response =
              sendRequest(
                  TransactionRequest.newBuilder()
                      .setCommand(Command.GET)
                      .setGet(ProtoUtil.toGet(get))
                      .build());
          throwIfErrorCrud(response);

          if (response.hasResults() && !response.getResults().getResultList().isEmpty()) {
            TableMetadata tableMetadata = metadataManager.getTableMetadata(get);
            return Optional.of(
                ProtoUtil.toResult(response.getResults().getResultList().get(0), tableMetadata));
          }

          return Optional.empty();
        });
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException {
    return executeCrud(
        () -> {
          Utility.setTargetToIfNot(scan, namespace, tableName);

          TransactionResponse response =
              sendRequest(
                  TransactionRequest.newBuilder()
                      .setCommand(Command.SCAN)
                      .setScan(ProtoUtil.toScan(scan))
                      .build());
          throwIfErrorCrud(response);

          if (!response.hasResults()) {
            return Collections.emptyList();
          }

          TableMetadata tableMetadata = metadataManager.getTableMetadata(scan);
          return response.getResults().getResultList().stream()
              .map(r -> ProtoUtil.toResult(r, tableMetadata))
              .collect(Collectors.toList());
        });
  }

  @Override
  public void put(Put put) throws CrudException {
    mutate(put);
  }

  @Override
  public void put(List<Put> puts) throws CrudException {
    mutate(puts);
  }

  @Override
  public void delete(Delete delete) throws CrudException {
    mutate(delete);
  }

  @Override
  public void delete(List<Delete> deletes) throws CrudException {
    mutate(deletes);
  }

  private void mutate(Mutation mutation) throws CrudException {
    executeCrud(
        () -> {
          Utility.setTargetToIfNot(mutation, namespace, tableName);

          TransactionResponse response =
              sendRequest(
                  TransactionRequest.newBuilder()
                      .setCommand(Command.MUTATION)
                      .setMutations(
                          Mutations.newBuilder()
                              .addMutation(ProtoUtil.toMutation(mutation))
                              .build())
                      .build());
          throwIfErrorCrud(response);
          return null;
        });
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws CrudException {
    executeCrud(
        () -> {
          Utility.setTargetToIfNot(mutations, namespace, tableName);

          TransactionRequest.Mutations.Builder mutationsBuilder =
              TransactionRequest.Mutations.newBuilder();
          mutations.forEach(m -> mutationsBuilder.addMutation(ProtoUtil.toMutation(m)));
          TransactionResponse response =
              sendRequest(
                  TransactionRequest.newBuilder()
                      .setCommand(Command.MUTATION)
                      .setMutations(mutationsBuilder.build())
                      .build());
          throwIfErrorCrud(response);
          return null;
        });
  }

  private static void throwIfErrorCrud(TransactionResponse response) throws CrudException {
    if (response.hasError()) {
      switch (response.getError().getErrorCode()) {
        case INVALID_ARGUMENT:
          throw new IllegalArgumentException(response.getError().getMessage());
        case CONFLICT:
          throw new CrudConflictException(response.getError().getMessage());
        default:
          throw new CrudException(response.getError().getMessage());
      }
    }
  }

  private static <T> T executeCrud(ThrowableSupplier<T, Throwable> throwableSupplier)
      throws CrudException {
    try {
      return throwableSupplier.get();
    } catch (CrudException | IllegalArgumentException e) {
      throw e;
    } catch (Throwable t) {
      if (t instanceof Error) {
        throw (Error) t;
      }
      throw new CrudException(t.getMessage(), t);
    }
  }

  @Override
  public void commit() throws CommitException, UnknownTransactionStatusException {
    try {
      TransactionResponse response =
          sendRequest(TransactionRequest.newBuilder().setCommand(Command.COMMIT).build());
      if (response.hasError()) {
        switch (response.getError().getErrorCode()) {
          case INVALID_ARGUMENT:
            throw new IllegalArgumentException(response.getError().getMessage());
          case CONFLICT:
            throw new CommitConflictException(response.getError().getMessage());
          case UNKNOWN_TRANSACTION:
            throw new UnknownTransactionStatusException(response.getError().getMessage());
          default:
            throw new CommitException(response.getError().getMessage());
        }
      }
    } catch (CommitException | UnknownTransactionStatusException | IllegalArgumentException e) {
      throw e;
    } catch (Throwable t) {
      if (t instanceof Error) {
        throw (Error) t;
      }
      throw new CommitException(t.getMessage(), t);
    }
  }

  @Override
  public void abort() throws AbortException {
    try {
      TransactionResponse response =
          sendRequest(TransactionRequest.newBuilder().setCommand(Command.ABORT).build());
      if (response.hasError()) {
        if (response.getError().getErrorCode() == ErrorCode.INVALID_ARGUMENT) {
          throw new IllegalArgumentException(response.getError().getMessage());
        }
        throw new AbortException(response.getError().getMessage());
      }
    } catch (AbortException | IllegalArgumentException e) {
      throw e;
    } catch (Throwable t) {
      if (t instanceof Error) {
        throw (Error) t;
      }
      throw new AbortException(t.getMessage(), t);
    }
  }

  private TransactionResponse sendRequest(TransactionRequest request) throws Throwable {
    observer.init();
    transaction.onNext(request);
    observer.await();
    if (observer.hasError()) {
      throw observer.getError();
    }
    return observer.getResponse();
  }
}
