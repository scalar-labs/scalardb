package com.scalar.db.dataloader.core.tablemetadata;

import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import lombok.RequiredArgsConstructor;

/**
 * Implementation of {@link TableMetadataService} that retrieves table metadata using {@link
 * DistributedTransactionAdmin}.
 */
@RequiredArgsConstructor
public class TableMetadataTransactionService extends TableMetadataService {

  private final DistributedTransactionAdmin transactionAdmin;

  /**
   * Retrieves the {@link TableMetadata} for a given namespace and table using the {@link
   * DistributedTransactionAdmin}.
   *
   * @param namespace The namespace of the table.
   * @param tableName The name of the table.
   * @return The {@link TableMetadata} for the specified table, or null if not found.
   * @throws TableMetadataException If an error occurs while fetching metadata.
   */
  @Override
  protected TableMetadata getTableMetadataInternal(String namespace, String tableName)
      throws TableMetadataException {
    try {
      return transactionAdmin.getTableMetadata(namespace, tableName);
    } catch (ExecutionException e) {
      throw new TableMetadataException(
          CoreError.DATA_LOADER_TABLE_METADATA_RETRIEVAL_FAILED.buildMessage(e.getMessage()), e);
    }
  }
}
