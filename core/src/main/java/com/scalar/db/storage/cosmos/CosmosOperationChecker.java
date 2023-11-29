package com.scalar.db.storage.cosmos;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.common.checker.OperationChecker;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;

public class CosmosOperationChecker extends OperationChecker {
  public CosmosOperationChecker(
      DatabaseConfig databaseConfig, TableMetadataManager metadataManager) {
    super(databaseConfig, metadataManager);
  }

  @Override
  public void check(Put put) throws ExecutionException {
    super.check(put);
    TableMetadata metadata = getTableMetadata(put);
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
      if (metadata.getColumnDataType(expression.getColumn().getName()) == DataType.BLOB) {
        if (expression.getOperator() != ConditionalExpression.Operator.EQ
            && expression.getOperator() != ConditionalExpression.Operator.NE
            && expression.getOperator() != ConditionalExpression.Operator.IS_NULL
            && expression.getOperator() != ConditionalExpression.Operator.IS_NOT_NULL) {
          throw new IllegalArgumentException(
              "Cosmos DB only supports EQ, NE, IS_NULL, and IS_NOT_NULL operations for BLOB type in conditions");
        }
      }
    }
  }
}
