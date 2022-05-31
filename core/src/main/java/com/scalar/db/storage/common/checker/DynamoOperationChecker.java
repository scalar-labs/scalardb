package com.scalar.db.storage.common.checker;

import com.scalar.db.api.Put;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.TableMetadataManager;

public class DynamoOperationChecker extends OperationChecker {
  public DynamoOperationChecker(TableMetadataManager metadataManager) {
    super(metadataManager);
  }

  @Override
  void checkColumnsInPut(Put put, TableMetadata metadata) {
    super.checkColumnsInPut(put, metadata);

    put.getColumns().values().stream()
        .filter(column -> metadata.getSecondaryIndexNames().contains(column.getName()))
        .forEach(
            column -> {
              if (!new ColumnChecker(metadata, true, false, false, false).check(column)) {
                throw new IllegalArgumentException(
                    "A secondary index column cannot be set to null. Operation: " + put);
              }
            });
  }
}
