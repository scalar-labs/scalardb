package com.scalar.db.storage.dynamo;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.Operation;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.storage.RetriableExecutionException;
import com.scalar.db.util.TableMetadataManager;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
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
public class DeleteStatementHandler extends StatementHandler {

  public DeleteStatementHandler(DynamoDbClient client, TableMetadataManager metadataManager) {
    super(client, metadataManager);
  }

  @Nonnull
  @Override
  public List<Map<String, AttributeValue>> handle(Operation operation) throws ExecutionException {
    checkArgument(operation, Delete.class);
    Delete delete = (Delete) operation;

    TableMetadata tableMetadata = metadataManager.getTableMetadata(operation);
    try {
      delete(delete, tableMetadata);
    } catch (ConditionalCheckFailedException e) {
      throw new NoMutationException("no mutation was applied.", e);
    } catch (TransactionConflictException e) {
      throw new RetriableExecutionException(e.getMessage(), e);
    } catch (DynamoDbException e) {
      throw new ExecutionException(e.getMessage(), e);
    }

    return Collections.emptyList();
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
        Map<String, AttributeValue> bindMap = dynamoMutation.getConditionBindMap();

        builder.expressionAttributeValues(bindMap);
      }

      builder.conditionExpression(condition);
    }

    Map<String, String> conditionAttributeNameMap = dynamoMutation.getConditionAttributeNameMap();
    if (!conditionAttributeNameMap.isEmpty()) {
      builder.expressionAttributeNames(dynamoMutation.getConditionAttributeNameMap());
    }

    client.deleteItem(builder.build());
  }
}
