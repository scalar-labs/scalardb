package com.scalar.db.storage.rpc;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.scalar.db.api.Operation;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.StorageRuntimeException;
import com.scalar.db.rpc.DistributedStorageAdminGrpc;
import com.scalar.db.rpc.GetTableMetadataRequest;
import com.scalar.db.rpc.GetTableMetadataResponse;
import com.scalar.db.storage.common.TableMetadataManager;
import com.scalar.db.util.ProtoUtil;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class GrpcTableMetadataManager implements TableMetadataManager {

  private final LoadingCache<TableName, Optional<TableMetadata>> tableMetadataCache;

  public GrpcTableMetadataManager(
      GrpcConfig config, DistributedStorageAdminGrpc.DistributedStorageAdminBlockingStub stub) {
    Objects.requireNonNull(stub);

    // TODO Need to add an expiration to handle the case of altering table
    tableMetadataCache =
        CacheBuilder.newBuilder()
            .build(
                new CacheLoader<TableName, Optional<TableMetadata>>() {
                  @Override
                  public Optional<TableMetadata> load(@Nonnull TableName tableName) {
                    GetTableMetadataResponse response =
                        stub.withDeadlineAfter(
                                config.getDeadlineDurationMillis(), TimeUnit.MILLISECONDS)
                            .getTableMetadata(
                                GetTableMetadataRequest.newBuilder()
                                    .setNamespace(tableName.namespace)
                                    .setTable(tableName.table)
                                    .build());
                    if (!response.hasTableMetadata()) {
                      return Optional.empty();
                    }
                    return Optional.of(ProtoUtil.toTableMetadata(response.getTableMetadata()));
                  }
                });
  }

  @Override
  public TableMetadata getTableMetadata(Operation operation) {
    if (!operation.forNamespace().isPresent() || !operation.forTable().isPresent()) {
      throw new IllegalArgumentException("operation has no target namespace and table name");
    }
    return getTableMetadata(operation.forFullNamespace().get(), operation.forTable().get());
  }

  @Override
  public TableMetadata getTableMetadata(String namespace, String table) {
    try {
      return tableMetadataCache.get(new TableName(namespace, table)).orElse(null);
    } catch (ExecutionException e) {
      throw new StorageRuntimeException("Failed to read the table metadata", e);
    } catch (UncheckedExecutionException e) {
      if (e.getCause() instanceof StatusRuntimeException) {
        StatusRuntimeException statusRuntimeException = (StatusRuntimeException) e.getCause();
        if (statusRuntimeException.getStatus().getCode() == Code.INVALID_ARGUMENT) {
          throw new IllegalArgumentException(e.getMessage(), statusRuntimeException);
        }
      }
      throw new StorageRuntimeException("Failed to read the table metadata", e);
    }
  }

  private static class TableName {
    public final String namespace;
    public final String table;

    public TableName(String namespace, String table) {
      this.namespace = Objects.requireNonNull(namespace);
      this.table = Objects.requireNonNull(table);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TableName tableName = (TableName) o;
      return Objects.equals(namespace, tableName.namespace)
          && Objects.equals(table, tableName.table);
    }

    @Override
    public int hashCode() {
      return Objects.hash(namespace, table);
    }
  }

  @Override
  public void deleteTableMetadata(String namespace, String table) {
    //TODO To implement
  }

  @Override
  public void addTableMetadata(TableMetadata metadata) {
    //TODO To implement
  }
}
