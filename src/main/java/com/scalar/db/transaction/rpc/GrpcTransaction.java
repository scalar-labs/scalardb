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
import com.scalar.db.rpc.AbortRequest;
import com.scalar.db.rpc.CommitRequest;
import com.scalar.db.rpc.DistributedTransactionGrpc;
import com.scalar.db.rpc.TransactionalGetRequest;
import com.scalar.db.rpc.TransactionalGetResponse;
import com.scalar.db.rpc.TransactionalMutateRequest;
import com.scalar.db.rpc.TransactionalScanRequest;
import com.scalar.db.rpc.TransactionalScanResponse;
import com.scalar.db.rpc.util.ProtoUtil;
import com.scalar.db.storage.common.util.Utility;
import com.scalar.db.storage.rpc.GrpcTableMetadataManager;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class GrpcTransaction implements DistributedTransaction {

  private final String txId;
  private final DistributedTransactionGrpc.DistributedTransactionBlockingStub stub;
  private final GrpcTableMetadataManager metadataManager;

  private Optional<String> namespace;
  private Optional<String> tableName;

  public GrpcTransaction(
      String txId,
      DistributedTransactionGrpc.DistributedTransactionBlockingStub stub,
      GrpcTableMetadataManager metadataManager,
      Optional<String> namespace,
      Optional<String> tableName) {
    this.txId = txId;
    this.stub = stub;
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

          TransactionalGetResponse response =
              stub.get(
                  TransactionalGetRequest.newBuilder()
                      .setTransactionId(txId)
                      .setGet(ProtoUtil.toGet(get))
                      .build());
          if (response.hasResult()) {
            TableMetadata tableMetadata = metadataManager.getTableMetadata(get);
            return Optional.of(ProtoUtil.toResult(response.getResult(), tableMetadata));
          }
          return Optional.empty();
        });
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException {
    return executeCrud(
        () -> {
          Utility.setTargetToIfNot(scan, namespace, tableName);

          TransactionalScanResponse response =
              stub.scan(
                  TransactionalScanRequest.newBuilder()
                      .setTransactionId(txId)
                      .setScan(ProtoUtil.toScan(scan))
                      .build());
          TableMetadata tableMetadata = metadataManager.getTableMetadata(scan);
          return response.getResultList().stream()
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

          stub.mutate(
              TransactionalMutateRequest.newBuilder()
                  .setTransactionId(txId)
                  .addMutations(ProtoUtil.toMutation(mutation))
                  .build());
          return null;
        });
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws CrudException {
    executeCrud(
        () -> {
          Utility.setTargetToIfNot(mutations, namespace, tableName);

          TransactionalMutateRequest.Builder builder =
              TransactionalMutateRequest.newBuilder().setTransactionId(txId);
          mutations.forEach(m -> builder.addMutations(ProtoUtil.toMutation(m)));
          stub.mutate(builder.build());
          return null;
        });
  }

  private static <T> T executeCrud(Supplier<T> supplier) throws CrudException {
    try {
      return supplier.get();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Code.INVALID_ARGUMENT) {
        throw new IllegalArgumentException(e.getMessage());
      } else if (e.getStatus().getCode() == Code.FAILED_PRECONDITION) {
        throw new CrudConflictException(e.getMessage());
      }
      throw new CrudException(e.getMessage());
    }
  }

  @Override
  public void commit() throws CommitException, UnknownTransactionStatusException {
    try {
      stub.commit(CommitRequest.newBuilder().setTransactionId(txId).build());
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Code.FAILED_PRECONDITION) {
        throw new CommitConflictException(e.getMessage());
      } else if (e.getStatus().getCode() == Code.UNKNOWN) {
        throw new UnknownTransactionStatusException(e.getMessage());
      }
      throw new CommitException(e.getMessage());
    }
  }

  @Override
  public void abort() throws AbortException {
    try {
      stub.abort(AbortRequest.newBuilder().setTransactionId(txId).build());
    } catch (StatusRuntimeException e) {
      throw new AbortException(e.getMessage());
    }
  }
}
