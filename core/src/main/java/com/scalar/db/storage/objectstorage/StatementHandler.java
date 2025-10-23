package com.scalar.db.storage.objectstorage;

import com.google.common.collect.Ordering;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Operation;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Column;
import java.util.List;
import javax.annotation.Nonnull;

public class StatementHandler {
  protected final ObjectStorageWrapper wrapper;
  protected final TableMetadataManager metadataManager;

  public StatementHandler(ObjectStorageWrapper wrapper, TableMetadataManager metadataManager) {
    this.wrapper = wrapper;
    this.metadataManager = metadataManager;
  }

  @Nonnull
  protected String getNamespace(Operation operation) {
    assert operation.forNamespace().isPresent();
    return operation.forNamespace().get();
  }

  @Nonnull
  protected String getTable(Operation operation) {
    assert operation.forTable().isPresent();
    return operation.forTable().get();
  }

  protected void validateConditions(
      ObjectStorageRecord record, List<ConditionalExpression> expressions, TableMetadata metadata)
      throws ExecutionException {
    for (ConditionalExpression expression : expressions) {
      Column<?> expectedColumn = expression.getColumn();
      Column<?> actualColumn =
          ColumnValueMapper.convert(
              record.getValues().get(expectedColumn.getName()),
              expectedColumn.getName(),
              metadata.getColumnDataType(expectedColumn.getName()));
      boolean validationFailed = false;
      switch (expression.getOperator()) {
        case EQ:
          if (actualColumn.hasNullValue()) {
            validationFailed = true;
            break;
          }
          if (Ordering.natural().compare(actualColumn, expectedColumn) != 0) {
            validationFailed = true;
            break;
          }
          break;
        case NE:
          if (actualColumn.hasNullValue()) {
            validationFailed = true;
            break;
          }
          if (Ordering.natural().compare(actualColumn, expectedColumn) == 0) {
            validationFailed = true;
            break;
          }
          break;
        case GT:
          if (actualColumn.hasNullValue()) {
            validationFailed = true;
            break;
          }
          if (Ordering.natural().compare(actualColumn, expectedColumn) <= 0) {
            validationFailed = true;
            break;
          }
          break;
        case GTE:
          if (actualColumn.hasNullValue()) {
            validationFailed = true;
            break;
          }
          if (Ordering.natural().compare(actualColumn, expectedColumn) < 0) {
            validationFailed = true;
            break;
          }
          break;
        case LT:
          if (actualColumn.hasNullValue()) {
            validationFailed = true;
            break;
          }
          if (Ordering.natural().compare(actualColumn, expectedColumn) >= 0) {
            validationFailed = true;
            break;
          }
          break;
        case LTE:
          if (actualColumn.hasNullValue()) {
            validationFailed = true;
            break;
          }
          if (Ordering.natural().compare(actualColumn, expectedColumn) > 0) {
            validationFailed = true;
            break;
          }
          break;
        case IS_NULL:
          if (!actualColumn.hasNullValue()) {
            validationFailed = true;
            break;
          }
          break;
        case IS_NOT_NULL:
          if (actualColumn.hasNullValue()) {
            validationFailed = true;
            break;
          }
          break;
        case LIKE:
        case NOT_LIKE:
        default:
          throw new AssertionError("Unsupported operator");
      }
      if (validationFailed) {
        throw new ExecutionException(
            String.format(
                "A condition failed. ConditionalExpression: %s, Column: %s",
                expectedColumn, actualColumn));
      }
    }
  }
}
