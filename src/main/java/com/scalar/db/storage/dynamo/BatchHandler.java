package com.scalar.db.storage.dynamo;

import static com.google.common.base.Preconditions.checkNotNull;

import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.MultiPartitionException;
import com.scalar.db.exception.storage.NoMutationException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
  static final Logger LOGGER = LoggerFactory.getLogger(BatchHandler.class);
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
   * must be for the same partition. Otherwise, it throws an {@link MultiPartitionException}.
   *
   * @param mutations a list of {@code Mutation}s to execute
   * @throws NoMutationException if at least one of conditional {@code Mutation}s failed because it
   *     didn't meet the condition
   */
  public void handle(List<? extends Mutation> mutations) throws ExecutionException {
    checkNotNull(mutations);
    if (mutations.size() < 1) {
      throw new IllegalArgumentException("please specify at least one mutation.");
    } else if (mutations.size() > 25) {
      throw new IllegalArgumentException("DynamoDB cannot batch more than 25 mutations at once.");
    }

    TransactWriteItemsRequest.Builder builder = TransactWriteItemsRequest.builder();
    List<TransactWriteItem> transactItems = new ArrayList<>();
    mutations.forEach(m -> transactItems.add(makeWriteItem(m)));
    builder.transactItems(transactItems);

    try {
      client.transactWriteItems(builder.build());
    } catch (TransactionCanceledException e) {
      for (CancellationReason reason : e.cancellationReasons()) {
        if (reason.code().equals("ConditionalCheckFailed")) {
          throw new NoMutationException("no mutation was applied.", e);
        }
      }
      throw new ExecutionException(e.getMessage(), e);
    } catch (DynamoDbException e) {
      throw new ExecutionException(e.getMessage(), e);
    }
  }

  private TransactWriteItem makeWriteItem(Mutation mutation) {
    TransactWriteItem.Builder itemBuilder = TransactWriteItem.builder();

    if (mutation instanceof com.scalar.db.api.Put) {
      itemBuilder.update(makeUpdate((com.scalar.db.api.Put) mutation));
    } else {
      itemBuilder.delete(makeDelete((com.scalar.db.api.Delete) mutation));
    }

    return itemBuilder.build();
  }

  private Update makeUpdate(com.scalar.db.api.Put put) {
    DynamoMutation dynamoMutation = new DynamoMutation(put, metadataManager);
    Update.Builder updateBuilder = Update.builder();
    String expression;
    String condition = null;
    Map<String, String> columnMap;
    Map<String, AttributeValue> bindMap;

    if (!put.getCondition().isPresent()) {
      expression = dynamoMutation.getUpdateExpressionWithKey();
      columnMap = dynamoMutation.getColumnMapWithKey();
      bindMap = dynamoMutation.getValueBindMapWithKey();
    } else if (put.getCondition().get() instanceof PutIfNotExists) {
      expression = dynamoMutation.getUpdateExpressionWithKey();
      columnMap = dynamoMutation.getColumnMapWithKey();
      bindMap = dynamoMutation.getValueBindMapWithKey();
      condition = dynamoMutation.getIfNotExistsCondition();
    } else if (put.getCondition().get() instanceof PutIfExists) {
      expression = dynamoMutation.getUpdateExpression();
      condition = dynamoMutation.getIfExistsCondition();
      columnMap = dynamoMutation.getColumnMap();
      bindMap = dynamoMutation.getValueBindMap();
    } else {
      expression = dynamoMutation.getUpdateExpression();
      condition = dynamoMutation.getIfExistsCondition() + " AND " + dynamoMutation.getCondition();
      columnMap = dynamoMutation.getColumnMap();
      bindMap = dynamoMutation.getConditionBindMap();
      bindMap.putAll(dynamoMutation.getValueBindMap());
    }

    updateBuilder
        .tableName(dynamoMutation.getTableName())
        .key(dynamoMutation.getKeyMap())
        .updateExpression(expression)
        .conditionExpression(condition)
        .expressionAttributeNames(columnMap)
        .expressionAttributeValues(bindMap)
        .build();

    return updateBuilder.build();
  }

  private Delete makeDelete(com.scalar.db.api.Delete delete) {
    DynamoMutation dynamoMutation = new DynamoMutation(delete, metadataManager);
    Delete.Builder deleteBuilder =
        Delete.builder().tableName(dynamoMutation.getTableName()).key(dynamoMutation.getKeyMap());

    if (delete.getCondition().isPresent()) {
      String condition;

      if (delete.getCondition().get() instanceof DeleteIfExists) {
        condition = dynamoMutation.getIfExistsCondition();
      } else {
        condition = dynamoMutation.getCondition();
        Map<String, AttributeValue> bindMap = dynamoMutation.getConditionBindMap();

        deleteBuilder.expressionAttributeValues(bindMap);
      }

      deleteBuilder.conditionExpression(condition);
    }

    return deleteBuilder.build();
  }
}
