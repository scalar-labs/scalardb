package com.scalar.db.storage.dynamo;

import static com.google.common.base.Preconditions.checkNotNull;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIfExists;
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

  public DeleteStatementHandler(DynamoDbClient client, TableMetadataManager metadataManager) {
    this.client = checkNotNull(client);
    this.metadataManager = checkNotNull(metadataManager);
  }

  public void handle(Delete delete) throws ExecutionException {
    TableMetadata tableMetadata = metadataManager.getTableMetadata(delete);
    try {
      delete(delete, tableMetadata);
    } catch (ConditionalCheckFailedException e) {
      throw new NoMutationException("no mutation was applied.", e);
    } catch (TransactionConflictException e) {
      throw new RetriableExecutionException(e.getMessage(), e);
    } catch (DynamoDbException e) {
      throw new ExecutionException(e.getMessage(), e);
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
        condition = dynamoMutation.getCondition();
        builder.expressionAttributeNames(dynamoMutation.getConditionColumnMap());
        Map<String, AttributeValue> bindMap = dynamoMutation.getConditionBindMap();
        builder.expressionAttributeValues(bindMap);
      }

      builder.conditionExpression(condition);
    }

    client.deleteItem(builder.build());
  }
}
