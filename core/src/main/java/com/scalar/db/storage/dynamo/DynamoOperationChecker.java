package com.scalar.db.storage.dynamo;

import com.scalar.db.api.Put;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.storage.common.checker.ColumnChecker;
import com.scalar.db.storage.common.checker.OperationChecker;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class DynamoOperationChecker extends OperationChecker {
  public DynamoOperationChecker(TableMetadataManager metadataManager) {
    super(metadataManager);
  }

  @Override
  protected void checkColumnsInPut(Put put, TableMetadata metadata) {
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
