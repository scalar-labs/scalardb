package com.scalar.db.storage.cosmos;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.io.Value;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An abstraction for handler classes for mutate statements */
@ThreadSafe
public abstract class MutateStatementHandler extends StatementHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(MutateStatementHandler.class);

  public MutateStatementHandler(CosmosClient client, TableMetadataHandler metadataHandler) {
    super(client, metadataHandler);
  }

  /**
   * Returns a {@link CosmosItemRequestOptions} with the consistency level for the specified {@link
   * Operation}
   *
   * @param operation an {@code Operation}
   */
  protected ConsistencyLevel convert(Operation operation) {
    switch (operation.getConsistency()) {
      case SEQUENTIAL:
        return ConsistencyLevel.STRONG;
      case EVENTUAL:
        return ConsistencyLevel.EVENTUAL;
      case LINEARIZABLE:
        return ConsistencyLevel.STRONG;
      default:
        LOGGER.warn("Unsupported consistency is specified. SEQUENTIAL is being used instead.");
        return ConsistencyLevel.STRONG;
    }
  }

  protected Record makeRecord(Mutation mutation) {
    Record record = new Record();
    record.setId(getId(mutation));
    record.setConcatPartitionKey(getConcatPartitionKey(mutation));

    MapVisitor partitionKeyVisitor = new MapVisitor();
    for (Value v : mutation.getPartitionKey()) {
      v.accept(partitionKeyVisitor);
    }
    record.setPartitionKey(partitionKeyVisitor.get());

    mutation
        .getClusteringKey()
        .ifPresent(
            k -> {
              MapVisitor clusteringKeyVisitor = new MapVisitor();
              k.get()
                  .forEach(
                      v -> {
                        v.accept(clusteringKeyVisitor);
                      });
              record.setClusteringKey(clusteringKeyVisitor.get());
            });

    return record;
  }

  protected String makeConditionalQuery(Mutation mutation) {
    ConditionQueryBuilder builder = new ConditionQueryBuilder();

    builder.withPartitionKey(getConcatPartitionKey(mutation));
    mutation
        .getClusteringKey()
        .ifPresent(
            k -> {
              builder.withClusteringKey(k);
            });

    mutation
        .getCondition()
        .ifPresent(
            c -> {
              c.accept(builder);
            });

    return builder.build();
  }
}
