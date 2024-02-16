package com.scalar.db.storage.dynamo;

import static com.google.common.base.Preconditions.checkNotNull;

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
import java.util.Map;
import java.util.Optional;
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
  private final String namespacePrefix;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public PutStatementHandler(
      DynamoDbClient client,
      TableMetadataManager metadataManager,
      Optional<String> namespacePrefix) {
    this.client = checkNotNull(client);
    this.metadataManager = checkNotNull(metadataManager);
    this.namespacePrefix = namespacePrefix.orElse("");
  }

  public void handle(Put put) throws ExecutionException {
    TableMetadata tableMetadata = metadataManager.getTableMetadata(put);
    put = copyAndAppendNamespacePrefix(put);

    try {
      execute(put, tableMetadata);
    } catch (ConditionalCheckFailedException e) {
      throw new NoMutationException(CoreError.NO_MUTATION_APPLIED.buildMessage(), e);
    } catch (TransactionConflictException e) {
      throw new RetriableExecutionException(
          CoreError.DYNAMO_TRANSACTION_CONFLICT_OCCURRED_IN_MUTATION.buildMessage(
              e.getMessage(), e),
          e);
    } catch (DynamoDbException e) {
      throw new ExecutionException(
          CoreError.DYNAMO_ERROR_OCCURRED_IN_MUTATION.buildMessage(e.getMessage()), e);
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

  private Put copyAndAppendNamespacePrefix(Put put) {
    assert put.forNamespace().isPresent();
    return Put.newBuilder(put).namespace(namespacePrefix + put.forNamespace().get()).build();
  }
}
