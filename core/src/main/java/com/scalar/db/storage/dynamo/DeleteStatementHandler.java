package com.scalar.db.storage.dynamo;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.Operation;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

/**
 * A handler class for delete statements
 *
 * @author Yuji Ito
 */
@ThreadSafe
public class DeleteStatementHandler extends StatementHandler {

  public DeleteStatementHandler(DynamoDbClient client, DynamoTableMetadataManager metadataManager) {
    super(client, metadataManager);
  }

  @Override
  public List<Map<String, AttributeValue>> handle(Operation operation) throws ExecutionException {
    checkArgument(operation, Delete.class);
    Delete delete = (Delete) operation;

    try {
      delete(delete);
    } catch (ConditionalCheckFailedException e) {
      throw new NoMutationException("no mutation was applied.", e);
    } catch (DynamoDbException e) {
      throw new ExecutionException(e.getMessage(), e);
    }

    return Collections.emptyList();
  }

  private void delete(Delete delete) {
    DynamoMutation dynamoMutation = new DynamoMutation(delete, metadataManager);

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

    client.deleteItem(builder.build());
  }
}
