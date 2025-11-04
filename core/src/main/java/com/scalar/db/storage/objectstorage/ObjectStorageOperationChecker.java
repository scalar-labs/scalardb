package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
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
import com.scalar.db.io.DataType;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;

public class ObjectStorageOperationChecker extends OperationChecker {
  private static final char[] ILLEGAL_CHARACTERS_IN_PRIMARY_KEY = {
    ObjectStorageUtils.OBJECT_KEY_DELIMITER, ObjectStorageUtils.CONCATENATED_KEY_DELIMITER,
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
    checkPrimaryKey(put);

    TableMetadata metadata = getTableMetadata(put);
    checkCondition(put, metadata);
  }

  @Override
  public void check(Delete delete) throws ExecutionException {
    super.check(delete);
    checkPrimaryKey(delete);

    TableMetadata metadata = getTableMetadata(delete);
    checkCondition(delete, metadata);
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

  private void checkCondition(Mutation mutation, TableMetadata metadata) {
    if (!mutation.getCondition().isPresent()) {
      return;
    }
    for (ConditionalExpression expression : mutation.getCondition().get().getExpressions()) {
      if (metadata.getColumnDataType(expression.getColumn().getName()) == DataType.BLOB) {
        if (expression.getOperator() != ConditionalExpression.Operator.EQ
            && expression.getOperator() != ConditionalExpression.Operator.NE
            && expression.getOperator() != ConditionalExpression.Operator.IS_NULL
            && expression.getOperator() != ConditionalExpression.Operator.IS_NOT_NULL) {
          throw new IllegalArgumentException(
              CoreError.OBJECT_STORAGE_CONDITION_OPERATION_NOT_SUPPORTED_FOR_BLOB_TYPE.buildMessage(
                  mutation));
        }
      }
    }
  }
}
