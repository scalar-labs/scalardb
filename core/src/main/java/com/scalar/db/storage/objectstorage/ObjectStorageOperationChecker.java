package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.common.CoreError;
import com.scalar.db.common.StorageInfoProvider;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.common.checker.OperationChecker;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.ColumnVisitor;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import java.nio.ByteBuffer;

public class ObjectStorageOperationChecker extends OperationChecker {
  private static final char[] ILLEGAL_CHARACTERS_IN_PRIMARY_KEY = {
    ObjectStorageUtils.OBJECT_KEY_DELIMITER, ObjectStorageUtils.CONCATENATED_KEY_DELIMITER,
  };

  private static final ColumnVisitor COMMON_COLUMN_CHECKER =
      new ColumnVisitor() {
        @Override
        public void visit(BooleanColumn column) {}

        @Override
        public void visit(IntColumn column) {}

        @Override
        public void visit(BigIntColumn column) {}

        @Override
        public void visit(FloatColumn column) {}

        @Override
        public void visit(DoubleColumn column) {}

        @Override
        public void visit(TextColumn column) {}

        @Override
        public void visit(BlobColumn column) {
          ByteBuffer buffer = column.getBlobValue();
          if (buffer == null) {
            return;
          }
          // Calculate the maximum allowed blob length after Base64 encoding.
          long allowedLength = (long) Serializer.MAX_STRING_LENGTH_ALLOWED / 4 * 3;
          if (buffer.remaining() > allowedLength) {
            throw new IllegalArgumentException(
                CoreError.OBJECT_STORAGE_BLOB_EXCEEDS_MAX_LENGTH_ALLOWED.buildMessage(
                    Serializer.MAX_STRING_LENGTH_ALLOWED, column.getName(), buffer.remaining()));
          }
        }

        @Override
        public void visit(DateColumn column) {}

        @Override
        public void visit(TimeColumn column) {}

        @Override
        public void visit(TimestampColumn column) {}

        @Override
        public void visit(TimestampTZColumn column) {}
      };

  private static final ColumnVisitor PRIMARY_KEY_COLUMN_CHECKER =
      new ColumnVisitor() {
        @Override
        public void visit(BooleanColumn column) {}

        @Override
        public void visit(IntColumn column) {}

        @Override
        public void visit(BigIntColumn column) {}

        @Override
        public void visit(FloatColumn column) {}

        @Override
        public void visit(DoubleColumn column) {}

        @Override
        public void visit(TextColumn column) {
          String value = column.getTextValue();
          assert value != null;

          for (char illegalCharacter : ILLEGAL_CHARACTERS_IN_PRIMARY_KEY) {
            if (value.indexOf(illegalCharacter) != -1) {
              throw new IllegalArgumentException(
                  CoreError.OBJECT_STORAGE_PRIMARY_KEY_CONTAINS_ILLEGAL_CHARACTER.buildMessage(
                      column.getName(), value));
            }
          }
        }

        @Override
        public void visit(BlobColumn column) {}

        @Override
        public void visit(DateColumn column) {}

        @Override
        public void visit(TimeColumn column) {}

        @Override
        public void visit(TimestampColumn column) {}

        @Override
        public void visit(TimestampTZColumn column) {}
      };

  public ObjectStorageOperationChecker(
      DatabaseConfig databaseConfig,
      TableMetadataManager metadataManager,
      StorageInfoProvider storageInfoProvider) {
    super(databaseConfig, metadataManager, storageInfoProvider);
  }

  @Override
  public void check(Get get) throws ExecutionException {
    super.check(get);
    checkPrimaryKey(get);
  }

  @Override
  public void check(Scan scan) throws ExecutionException {
    super.check(scan);
    checkPrimaryKey(scan);
    scan.getStartClusteringKey()
        .ifPresent(
            c -> c.getColumns().forEach(column -> column.accept(PRIMARY_KEY_COLUMN_CHECKER)));
    scan.getEndClusteringKey()
        .ifPresent(
            c -> c.getColumns().forEach(column -> column.accept(PRIMARY_KEY_COLUMN_CHECKER)));
  }

  @Override
  public void check(Put put) throws ExecutionException {
    super.check(put);
    put.getColumns().values().forEach(column -> column.accept(COMMON_COLUMN_CHECKER));
    checkPrimaryKey(put);
  }

  @Override
  public void check(Delete delete) throws ExecutionException {
    super.check(delete);
    checkPrimaryKey(delete);
  }

  private void checkPrimaryKey(Operation operation) {
    operation
        .getPartitionKey()
        .getColumns()
        .forEach(column -> column.accept(PRIMARY_KEY_COLUMN_CHECKER));
    operation
        .getClusteringKey()
        .ifPresent(
            c -> c.getColumns().forEach(column -> column.accept(PRIMARY_KEY_COLUMN_CHECKER)));
  }
}
