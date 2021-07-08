package com.scalar.db.storage.rpc;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.rpc.CreateTableRequest;
import com.scalar.db.rpc.DistributedStorageAdminGrpc;
import com.scalar.db.rpc.DropTableRequest;
import com.scalar.db.rpc.TruncateTableRequest;
import com.scalar.db.util.ProtoUtil;
import io.grpc.ManagedChannel;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class GrpcAdmin implements DistributedStorageAdmin {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcAdmin.class);

  private final ManagedChannel channel;
  private final DistributedStorageAdminGrpc.DistributedStorageAdminBlockingStub stub;
  private final GrpcTableMetadataManager metadataManager;

  @Inject
  public GrpcAdmin(DatabaseConfig config) {
    channel =
        NettyChannelBuilder.forAddress(config.getContactPoints().get(0), config.getContactPort())
            .usePlaintext()
            .build();
    stub = DistributedStorageAdminGrpc.newBlockingStub(channel);
    metadataManager = new GrpcTableMetadataManager(stub);
  }

  @VisibleForTesting
  GrpcAdmin(
      DistributedStorageAdminGrpc.DistributedStorageAdminBlockingStub stub,
      GrpcTableMetadataManager metadataManager) {
    channel = null;
    this.stub = stub;
    this.metadataManager = metadataManager;
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    execute(
        () ->
            stub.createTable(
                CreateTableRequest.newBuilder()
                    .setNamespace(namespace)
                    .setTable(table)
                    .setTableMetadata(ProtoUtil.toTableMetadata(metadata))
                    .putAllOptions(options)
                    .build()));
  }

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    execute(
        () ->
            stub.dropTable(
                DropTableRequest.newBuilder().setNamespace(namespace).setTable(table).build()));
  }

  @Override
  public void truncateTable(String namespace, String table) throws ExecutionException {
    execute(
        () ->
            stub.truncateTable(
                TruncateTableRequest.newBuilder().setNamespace(namespace).setTable(table).build()));
  }

  @Override
  public TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException {
    return metadataManager.getTableMetadata(namespace, table);
  }

  private static void execute(Runnable supplier) throws ExecutionException {
    try {
      supplier.run();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Code.INVALID_ARGUMENT) {
        throw new IllegalArgumentException(e.getMessage());
      }
      throw new ExecutionException(e.getMessage());
    }
  }

  @Override
  public void close() {
    try {
      channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOGGER.warn("failed to shutdown the channel", e);
    }
  }
}
