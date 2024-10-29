package com.scalar.db.storage.cosmos;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.common.checker.OperationChecker;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.ColumnVisitor;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.TextColumn;

public class CosmosOperationChecker extends OperationChecker {

  private static final char[] ILLEGAL_CHARACTERS_IN_PRIMARY_KEY = {
    // Colons are not allowed in primary key columns due to the `ConcatenationVisitor` limitation
    ':',

    // The following characters are not allowed in primary key columns because they are restricted
    // and cannot be used in the `Id` property of a Cosmos DB document. See the following link for
    // more information:
    // https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.databaseproperties.id?view=azure-dotnet#remarks
    '/',
    '\\',
    '#',
    '?'
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
                  CoreError.COSMOS_PRIMARY_KEY_CONTAINS_ILLEGAL_CHARACTER.buildMessage(
                      column.getName(), value));
            }
          }
        }

        @Override
        public void visit(BlobColumn column) {}
      };

  public CosmosOperationChecker(
      DatabaseConfig databaseConfig, TableMetadataManager metadataManager) {
    super(databaseConfig, metadataManager);
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
              CoreError.COSMOS_CONDITION_OPERATION_NOT_SUPPORTED_FOR_BLOB_TYPE.buildMessage(
                  mutation));
        }
      }
    }
  }
}
