package com.scalar.db.storage.cosmos;

import static com.google.common.base.Preconditions.checkNotNull;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosContainer;
import com.scalar.db.api.Operation;
import com.scalar.db.common.TableMetadataManager;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

/** A handler class for statements */
@ThreadSafe
public abstract class StatementHandler {
  protected final CosmosClient client;
  protected final TableMetadataManager metadataManager;

  /**
   * Constructs a {@code StatementHandler} with the specified {@link CosmosClient}
   *
   * @param client {@code CosmosClient}
   * @param metadataManager {@code TableMetadataManager}
   */
  protected StatementHandler(CosmosClient client, TableMetadataManager metadataManager) {
    this.client = checkNotNull(client);
    this.metadataManager = checkNotNull(metadataManager);
  }

  @Nonnull
  protected CosmosContainer getContainer(Operation operation) {
    return client
        .getDatabase(operation.forNamespace().get())
        .getContainer(operation.forTable().get());
  }
}
