package com.scalar.db.storage.dynamo;

import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.storage.RetriableExecutionException;
import com.scalar.db.storage.common.TableMetadataManager;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.TransactionConflictException;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

/**
 * A handler class for put statements
 *
 * @author Yuji Ito
 */
@ThreadSafe
public class PutStatementHandler extends StatementHandler {

  public PutStatementHandler(DynamoDbClient client, TableMetadataManager metadataManager) {
    super(client, metadataManager);
  }

  @Nonnull
  @Override
  public List<Map<String, AttributeValue>> handle(Operation operation) throws ExecutionException {
    checkArgument(operation, Put.class);
    Put put = (Put) operation;

    TableMetadata tableMetadata = metadataManager.getTableMetadata(operation);
    try {
      execute(put, tableMetadata);
    } catch (ConditionalCheckFailedException e) {
      throw new NoMutationException("no mutation was applied.", e);
    } catch (TransactionConflictException e) {
      throw new RetriableExecutionException(e.getMessage(), e);
    } catch (DynamoDbException e) {
      throw new ExecutionException(e.getMessage(), e);
    }

    return Collections.emptyList();
  }

  private void execute(Put put, TableMetadata tableMetadata) {
    DynamoMutation dynamoMutation = new DynamoMutation(put, tableMetadata);
    String expression;
    String condition = null;
    Map<String, String> expressionAttributeNameMap = new HashMap<>();
    Map<String, AttributeValue> bindMap;

    if (!put.getCondition().isPresent()) {
      expression = dynamoMutation.getUpdateExpressionWithKey();
      expressionAttributeNameMap.putAll(dynamoMutation.getColumnMapWithKey());
      bindMap = dynamoMutation.getValueBindMapWithKey();
    } else if (put.getCondition().get() instanceof PutIfNotExists) {
      expression = dynamoMutation.getUpdateExpressionWithKey();
      expressionAttributeNameMap.putAll(dynamoMutation.getColumnMapWithKey());
      bindMap = dynamoMutation.getValueBindMapWithKey();
      condition = dynamoMutation.getIfNotExistsCondition();
    } else if (put.getCondition().get() instanceof PutIfExists) {
      expression = dynamoMutation.getUpdateExpression();
      condition = dynamoMutation.getIfExistsCondition();
      expressionAttributeNameMap.putAll(dynamoMutation.getColumnMap());
      bindMap = dynamoMutation.getValueBindMap();
    } else {
      expression = dynamoMutation.getUpdateExpression();
      condition = dynamoMutation.getCondition();
      expressionAttributeNameMap.putAll(dynamoMutation.getColumnMap());
      bindMap = dynamoMutation.getConditionBindMap();
      bindMap.putAll(dynamoMutation.getValueBindMap());
    }

    UpdateItemRequest.Builder requestBuilder =
        UpdateItemRequest.builder()
            .tableName(dynamoMutation.getTableName())
            .key(dynamoMutation.getKeyMap())
            .updateExpression(expression)
            .conditionExpression(condition)
            .expressionAttributeValues(bindMap);

    Map<String, String> conditionAttributeNameMap = dynamoMutation.getConditionAttributeNameMap();
    if (!conditionAttributeNameMap.isEmpty()) {
      expressionAttributeNameMap.putAll(dynamoMutation.getConditionAttributeNameMap());
    }

    requestBuilder.expressionAttributeNames(expressionAttributeNameMap);

    client.updateItem(requestBuilder.build());
  }
}
