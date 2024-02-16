package com.scalar.db.storage.dynamo;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.common.checker.ColumnChecker;
import com.scalar.db.common.checker.OperationChecker;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class DynamoOperationChecker extends OperationChecker {
  public DynamoOperationChecker(
      DatabaseConfig databaseConfig, TableMetadataManager metadataManager) {
    super(databaseConfig, metadataManager);
  }

  @Override
  public void check(Put put) throws ExecutionException {
    super.check(put);
    TableMetadata metadata = getTableMetadata(put);
    put.getColumns().values().stream()
        .filter(column -> metadata.getSecondaryIndexNames().contains(column.getName()))
        .forEach(
            column -> {
              if (!new ColumnChecker(metadata, true, false, true, false).check(column)) {
                throw new IllegalArgumentException(
                    CoreError.DYNAMO_INDEX_COLUMN_CANNOT_BE_SET_TO_NULL_OR_EMPTY.buildMessage(put));
              }
            });
    checkCondition(put, metadata);
  }

  @Override
  public void check(Delete delete) throws ExecutionException {
    super.check(delete);
    TableMetadata metadata = getTableMetadata(delete);
    checkCondition(delete, metadata);
  }

  private void checkCondition(Mutation mutation, TableMetadata metadata) {
    if (!mutation.getCondition().isPresent()) {
      return;
    }
    for (ConditionalExpression expression : mutation.getCondition().get().getExpressions()) {
      if (metadata.getColumnDataType(expression.getColumn().getName()) == DataType.BOOLEAN) {
        if (expression.getOperator() != ConditionalExpression.Operator.EQ
            && expression.getOperator() != ConditionalExpression.Operator.NE
            && expression.getOperator() != ConditionalExpression.Operator.IS_NULL
            && expression.getOperator() != ConditionalExpression.Operator.IS_NOT_NULL) {
          throw new IllegalArgumentException(
              CoreError.DYNAMO_CONDITION_OPERATION_NOT_SUPPORTED_FOR_BOOLEAN_TYPE.buildMessage(
                  mutation));
        }
      }
    }
  }
}
