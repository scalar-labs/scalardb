package com.scalar.db.storage.dynamo;

import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.storage.RetriableExecutionException;
import com.scalar.db.util.TableMetadataManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CancellationReason;
import software.amazon.awssdk.services.dynamodb.model.Delete;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.Update;

/**
 * A handler for a batch
 *
 * @author Yuji Ito
 */
@ThreadSafe
public class BatchHandler {
  private final DynamoDbClient client;
  private final TableMetadataManager metadataManager;

  /**
   * Constructs a {@code BatchHandler} with the specified {@link DynamoDbClient} and {@link
   * TableMetadataManager}
   *
   * @param client {@code DynamoDbClient} to create a statement with
   * @param metadataManager {@code TableMetadataManager}
   */
  public BatchHandler(DynamoDbClient client, TableMetadataManager metadataManager) {
    this.client = client;
    this.metadataManager = metadataManager;
  }

  /**
   * Execute the specified list of {@link Mutation}s in batch. All the {@link Mutation}s in the list
   * must be for the same partition.
   *
   * @param mutations a list of {@code Mutation}s to execute
   * @throws NoMutationException if at least one of conditional {@code Mutation}s failed because it
   *     didn't meet the condition
   */
  public void handle(List<? extends Mutation> mutations) throws ExecutionException {
    if (mutations.size() > 25) {
      throw new IllegalArgumentException("DynamoDB cannot batch more than 25 mutations at once.");
    }

    TableMetadata tableMetadata = metadataManager.getTableMetadata(mutations.get(0));

    TransactWriteItemsRequest.Builder builder = TransactWriteItemsRequest.builder();
    List<TransactWriteItem> transactItems = new ArrayList<>();
    mutations.forEach(m -> transactItems.add(makeWriteItem(m, tableMetadata)));
    builder.transactItems(transactItems);

    try {
      client.transactWriteItems(builder.build());
    } catch (TransactionCanceledException e) {
      boolean allReasonsAreTransactionConflicts = true;
      for (CancellationReason reason : e.cancellationReasons()) {
        if (reason.code().equals("ConditionalCheckFailed")) {
          throw new NoMutationException("no mutation was applied.", e);
        }
        if (!reason.code().equals("TransactionConflict") && !reason.code().equals("None")) {
          allReasonsAreTransactionConflicts = false;
        }
      }
      if (allReasonsAreTransactionConflicts) {
        // If all the reasons of the cancellation are "TransactionConflict", throw
        // RetriableExecutionException
        throw new RetriableExecutionException(e.getMessage(), e);
      }
      throw new ExecutionException(e.getMessage(), e);
    } catch (DynamoDbException e) {
      throw new ExecutionException(e.getMessage(), e);
    }
  }

  private TransactWriteItem makeWriteItem(Mutation mutation, TableMetadata tableMetadata) {
    TransactWriteItem.Builder itemBuilder = TransactWriteItem.builder();

    if (mutation instanceof com.scalar.db.api.Put) {
      itemBuilder.update(makeUpdate((com.scalar.db.api.Put) mutation, tableMetadata));
    } else {
      itemBuilder.delete(makeDelete((com.scalar.db.api.Delete) mutation, tableMetadata));
    }

    return itemBuilder.build();
  }

  private Update makeUpdate(com.scalar.db.api.Put put, TableMetadata tableMetadata) {
    DynamoMutation dynamoMutation = new DynamoMutation(put, tableMetadata);
    Update.Builder updateBuilder = Update.builder();
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

    return updateBuilder
        .tableName(dynamoMutation.getTableName())
        .key(dynamoMutation.getKeyMap())
        .updateExpression(expression)
        .conditionExpression(condition)
        .expressionAttributeValues(bindMap)
        .expressionAttributeNames(expressionAttributeNameMap)
        .build();
  }

  private Delete makeDelete(com.scalar.db.api.Delete delete, TableMetadata tableMetadata) {
    DynamoMutation dynamoMutation = new DynamoMutation(delete, tableMetadata);
    Delete.Builder deleteBuilder =
        Delete.builder().tableName(dynamoMutation.getTableName()).key(dynamoMutation.getKeyMap());

    if (delete.getCondition().isPresent()) {
      String condition;

      if (delete.getCondition().get() instanceof DeleteIfExists) {
        condition = dynamoMutation.getIfExistsCondition();
      } else {
        condition = dynamoMutation.getCondition();
        deleteBuilder.expressionAttributeNames(dynamoMutation.getConditionColumnMap());
        Map<String, AttributeValue> bindMap = dynamoMutation.getConditionBindMap();
        deleteBuilder.expressionAttributeValues(bindMap);
      }

      deleteBuilder.conditionExpression(condition);
    }

    return deleteBuilder.build();
  }
}
