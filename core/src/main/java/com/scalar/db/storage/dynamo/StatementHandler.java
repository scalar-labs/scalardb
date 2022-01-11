package com.scalar.db.storage.dynamo;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Joiner;
import com.scalar.db.api.Operation;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.util.TableMetadataManager;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/** A handler class for statements */
@ThreadSafe
public abstract class StatementHandler {
  protected final DynamoDbClient client;
  protected final TableMetadataManager metadataManager;

  /**
   * Constructs a {@code StatementHandler} with the specified {@link DynamoDbClient} and a new
   * {@link TableMetadataManager}
   *
   * @param client {@code DynamoDbClient}
   * @param metadataManager {@code TableMetadataManager}
   */
  public StatementHandler(DynamoDbClient client, TableMetadataManager metadataManager) {
    this.client = checkNotNull(client);
    this.metadataManager = checkNotNull(metadataManager);
  }

  /**
   * Executes the specified {@code Operation}
   *
   * @param operation an {@code Operation} to execute
   * @return a list of results
   * @throws ExecutionException if the execution failed
   */
  @Nonnull
  public abstract List<Map<String, AttributeValue>> handle(Operation operation)
      throws ExecutionException;

  @SafeVarargs
  protected final void checkArgument(Operation actual, Class<? extends Operation>... expected) {
    for (Class<? extends Operation> e : expected) {
      if (e.isInstance(actual)) {
        return;
      }
    }
    throw new IllegalArgumentException(
        Joiner.on(" ")
            .join(
                new String[] {
                  actual.getClass().toString(), "is passed where something like",
                  expected[0].toString(), "is expected."
                }));
  }
}
