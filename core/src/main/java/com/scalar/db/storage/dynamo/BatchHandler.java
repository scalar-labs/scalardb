package com.scalar.db.storage.dynamo;

import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.storage.RetriableExecutionException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
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
  private final String namespacePrefix;

  /**
   * Constructs a {@code BatchHandler} with the specified {@link DynamoDbClient} and {@link
   * TableMetadataManager}
   *
   * @param client {@code DynamoDbClient} to create a statement with
   * @param metadataManager {@code TableMetadataManager}
   * @param namespacePrefix a namespace prefix
   */
  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public BatchHandler(
      DynamoDbClient client,
      TableMetadataManager metadataManager,
      Optional<String> namespacePrefix) {
    this.client = client;
    this.metadataManager = metadataManager;
    this.namespacePrefix = namespacePrefix.orElse("");
  }

  /**
   * Executes the specified list of {@link Mutation}s in batch. All the {@link Mutation}s in the
   * list must be for the same partition.
   *
   * @param mutations a list of {@code Mutation}s to execute
   * @throws NoMutationException if at least one of conditional {@code Mutation}s fails because it
   *     didn't meet the condition
   */
  public void handle(List<? extends Mutation> mutations) throws ExecutionException {
    if (mutations.size() > 100) {
      throw new IllegalArgumentException("DynamoDB cannot batch more than 100 mutations at once");
    }

    TableMetadata tableMetadata = metadataManager.getTableMetadata(mutations.get(0));
    mutations = copyAndAppendNamespacePrefix(mutations);

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
          throw new NoMutationException(CoreError.NO_MUTATION_APPLIED.buildMessage(), e);
        }
        if (!reason.code().equals("TransactionConflict") && !reason.code().equals("None")) {
          allReasonsAreTransactionConflicts = false;
        }
      }
      if (allReasonsAreTransactionConflicts) {
        // If all the reasons of the cancellation are "TransactionConflict", throw
        // RetriableExecutionException
        throw new RetriableExecutionException(
            CoreError.DYNAMO_TRANSACTION_CONFLICT_OCCURRED_IN_MUTATION.buildMessage(
                e.getMessage(), e),
            e);
      }

      throw new ExecutionException(
          CoreError.DYNAMO_ERROR_OCCURRED_IN_MUTATION.buildMessage(e.getMessage()), e);
    } catch (DynamoDbException e) {
      throw new ExecutionException(
          CoreError.DYNAMO_ERROR_OCCURRED_IN_MUTATION.buildMessage(e.getMessage()), e);
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
        condition = dynamoMutation.getIfExistsCondition() + " AND " + dynamoMutation.getCondition();
        deleteBuilder.expressionAttributeNames(dynamoMutation.getConditionColumnMap());
        Map<String, AttributeValue> bindMap = dynamoMutation.getConditionBindMap();
        deleteBuilder.expressionAttributeValues(bindMap);
      }

      deleteBuilder.conditionExpression(condition);
    }

    return deleteBuilder.build();
  }

  private List<? extends Mutation> copyAndAppendNamespacePrefix(
      List<? extends Mutation> mutations) {
    return mutations.stream()
        .map(
            m -> {
              if (m instanceof Put) {
                return copyAndAppendNamespacePrefix((Put) m);
              } else if (m instanceof com.scalar.db.api.Delete) {
                return copyAndAppendNamespacePrefix((com.scalar.db.api.Delete) m);
              } else {
                throw new AssertionError("Unexpected mutation type: " + m.getClass().getName());
              }
            })
        .collect(Collectors.toList());
  }

  private Put copyAndAppendNamespacePrefix(Put put) {
    assert put.forNamespace().isPresent();
    return Put.newBuilder(put).namespace(namespacePrefix + put.forNamespace().get()).build();
  }

  private com.scalar.db.api.Delete copyAndAppendNamespacePrefix(com.scalar.db.api.Delete delete) {
    assert delete.forNamespace().isPresent();
    return com.scalar.db.api.Delete.newBuilder(delete)
        .namespace(namespacePrefix + delete.forNamespace().get())
        .build();
  }
}
