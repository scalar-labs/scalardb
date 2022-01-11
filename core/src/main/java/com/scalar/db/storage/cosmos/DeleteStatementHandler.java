package com.scalar.db.storage.cosmos;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.util.TableMetadataManager;
import java.util.Collections;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A handler class for delete statements
 *
 * @author Yuji Ito
 */
@ThreadSafe
public class DeleteStatementHandler extends MutateStatementHandler {

  public DeleteStatementHandler(CosmosClient client, TableMetadataManager metadataManager) {
    super(client, metadataManager);
  }

  @Override
  protected List<Record> execute(Operation operation) throws CosmosException, ExecutionException {
    Mutation mutation = (Mutation) operation;
    TableMetadata tableMetadata = metadataManager.getTableMetadata(mutation);
    if (mutation.getCondition().isPresent()) {
      executeStoredProcedure(mutation, tableMetadata);
    } else {
      execute(mutation, tableMetadata);
    }

    return Collections.emptyList();
  }

  private void execute(Mutation mutation, TableMetadata tableMetadata) throws CosmosException {
    CosmosMutation cosmosMutation = new CosmosMutation(mutation, tableMetadata);
    cosmosMutation.checkArgument(Delete.class);

    if (cosmosMutation.isPrimaryKeySpecified()) {
      String id = cosmosMutation.getId();
      PartitionKey partitionKey = cosmosMutation.getCosmosPartitionKey();
      CosmosItemRequestOptions options = new CosmosItemRequestOptions();

      getContainer(mutation).deleteItem(id, partitionKey, options);
    } else {
      // clustering key is not fully specified
      executeStoredProcedure(mutation, tableMetadata);
    }
  }
}
