package com.scalar.db.storage.dynamo;

import static com.google.common.base.Preconditions.checkNotNull;

import com.scalar.db.api.Put;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.storage.RetriableExecutionException;
import com.scalar.db.util.TableMetadataManager;
import java.util.Map;
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
public class PutStatementHandler {
  private final DynamoDbClient client;
  private final TableMetadataManager metadataManager;

  public PutStatementHandler(DynamoDbClient client, TableMetadataManager metadataManager) {
    this.client = checkNotNull(client);
    this.metadataManager = checkNotNull(metadataManager);
  }

  public void handle(Put put) throws ExecutionException {
    TableMetadata tableMetadata = metadataManager.getTableMetadata(put);
    try {
      execute(put, tableMetadata);
    } catch (ConditionalCheckFailedException e) {
      throw new NoMutationException("no mutation was applied.", e);
    } catch (TransactionConflictException e) {
      throw new RetriableExecutionException(e.getMessage(), e);
    } catch (DynamoDbException e) {
      throw new ExecutionException(e.getMessage(), e);
    }
  }

  private void execute(Put put, TableMetadata tableMetadata) {
    DynamoMutation dynamoMutation = new DynamoMutation(put, tableMetadata);
    String expression;
    String condition = null;
    Map<String, String> expressionAttributeNameMap;
    Map<String, AttributeValue> bindMap;

    if (!put.getCondition().isPresent()) {
      expression = dynamoMutation.getUpdateExpressionWithKey();
      expressionAttributeNameMap = dynamoMutation.getColumnMapWithKey();
      bindMap = dynamoMutation.getValueBindMapWithKey();
    } else if (put.getCondition().get() instanceof PutIfNotExists) {
      expression = dynamoMutation.getUpdateExpressionWithKey();
      expressionAttributeNameMap = dynamoMutation.getColumnMapWithKey();
      bindMap = dynamoMutation.getValueBindMapWithKey();
      condition = dynamoMutation.getIfNotExistsCondition();
    } else if (put.getCondition().get() instanceof PutIfExists) {
      expression = dynamoMutation.getUpdateExpression();
      condition = dynamoMutation.getIfExistsCondition();
      expressionAttributeNameMap = dynamoMutation.getColumnMap();
      bindMap = dynamoMutation.getValueBindMap();
    } else {
      expression = dynamoMutation.getUpdateExpression();
      condition = dynamoMutation.getIfExistsCondition() + " AND " + dynamoMutation.getCondition();
      expressionAttributeNameMap = dynamoMutation.getColumnMap();
      expressionAttributeNameMap.putAll(dynamoMutation.getConditionColumnMap());
      bindMap = dynamoMutation.getConditionBindMap();
      bindMap.putAll(dynamoMutation.getValueBindMap());
    }

    client.updateItem(
        UpdateItemRequest.builder()
            .tableName(dynamoMutation.getTableName())
            .key(dynamoMutation.getKeyMap())
            .updateExpression(expression)
            .conditionExpression(condition)
            .expressionAttributeValues(bindMap)
            .expressionAttributeNames(expressionAttributeNameMap)
            .build());
  }
}
