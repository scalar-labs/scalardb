package com.scalar.db.storage.dynamo;

import static com.google.common.base.Preconditions.checkNotNull;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIfExists;
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
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.TransactionConflictException;

/**
 * A handler class for delete statements
 *
 * @author Yuji Ito
 */
@ThreadSafe
public class DeleteStatementHandler {
  private final DynamoDbClient client;
  private final TableMetadataManager metadataManager;
  private final String namespacePrefix;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public DeleteStatementHandler(
      DynamoDbClient client,
      TableMetadataManager metadataManager,
      Optional<String> namespacePrefix) {
    this.client = checkNotNull(client);
    this.metadataManager = checkNotNull(metadataManager);
    this.namespacePrefix = namespacePrefix.orElse("");
  }

  public void handle(Delete delete) throws ExecutionException {
    TableMetadata tableMetadata = metadataManager.getTableMetadata(delete);
    delete = copyAndAppendNamespacePrefix(delete);

    try {
      delete(delete, tableMetadata);
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

  private void delete(Delete delete, TableMetadata tableMetadata) {
    DynamoMutation dynamoMutation = new DynamoMutation(delete, tableMetadata);

    DeleteItemRequest.Builder builder =
        DeleteItemRequest.builder()
            .tableName(dynamoMutation.getTableName())
            .key(dynamoMutation.getKeyMap());

    if (delete.getCondition().isPresent()) {
      String condition;

      if (delete.getCondition().get() instanceof DeleteIfExists) {
        condition = dynamoMutation.getIfExistsCondition();
      } else {
        condition = dynamoMutation.getIfExistsCondition() + " AND " + dynamoMutation.getCondition();
        builder.expressionAttributeNames(dynamoMutation.getConditionColumnMap());
        Map<String, AttributeValue> bindMap = dynamoMutation.getConditionBindMap();
        builder.expressionAttributeValues(bindMap);
      }

      builder.conditionExpression(condition);
    }

    client.deleteItem(builder.build());
  }

  private Delete copyAndAppendNamespacePrefix(Delete delete) {
    assert delete.forNamespace().isPresent();
    return Delete.newBuilder(delete)
        .namespace(namespacePrefix + delete.forNamespace().get())
        .build();
  }
}
