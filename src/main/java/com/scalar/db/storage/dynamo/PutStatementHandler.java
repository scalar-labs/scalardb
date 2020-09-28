package com.scalar.db.storage.dynamo;

import com.scalar.db.api.MutationCondition;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
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

  @Override
  public List<Map<String, AttributeValue>> handle(Operation operation) throws ExecutionException {
    checkArgument(operation, Put.class);
    Put put = (Put) operation;
    MutationCondition condition = put.getCondition().orElse(null);

    try {
      if (condition != null && (condition instanceof PutIf || condition instanceof PutIfExists)) {
        update(put);
      } else {
        insert(put);
      }
    } catch (ConditionalCheckFailedException e) {
      throw new NoMutationException("no mutation was applied.", e);
    } catch (DynamoDbException e) {
      throw new ExecutionException(e.getMessage(), e);
    }

    return Collections.emptyList();
  }

  private void insert(Put put) {
    DynamoMutation dynamoMutation = new DynamoMutation(put, metadataManager);

    PutItemRequest.Builder builder =
        PutItemRequest.builder()
            .tableName(dynamoMutation.getTableName())
            .item(dynamoMutation.getValueMapWithKey());

    if (put.getCondition().isPresent() && put.getCondition().get() instanceof PutIfNotExists) {
      String condition = dynamoMutation.getIfNotExistsCondition();
      builder.conditionExpression(condition);
    }

    client.putItem(builder.build());
  }

  private void update(Put put) {
    DynamoMutation dynamoMutation = new DynamoMutation(put, metadataManager);
    String condition;
    Map<String, AttributeValue> bindMap;
    if (put.getCondition().get() instanceof PutIfExists) {
      condition = dynamoMutation.getIfExistsCondition();
      bindMap = dynamoMutation.getValueBindMap();
    } else {
      condition = dynamoMutation.getCondition();
      bindMap = dynamoMutation.getConditionBindMap();
      bindMap.putAll(dynamoMutation.getValueBindMap());
    }

    UpdateItemRequest request =
        UpdateItemRequest.builder()
            .tableName(dynamoMutation.getTableName())
            .key(dynamoMutation.getKeyMap())
            .updateExpression(dynamoMutation.getUpdateExpression())
            .conditionExpression(condition)
            .expressionAttributeValues(bindMap)
            .build();

    client.updateItem(request);
  }
}
