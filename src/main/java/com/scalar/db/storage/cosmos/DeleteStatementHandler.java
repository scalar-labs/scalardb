package com.scalar.db.storage.cosmos;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Operation;
import com.scalar.db.exception.storage.NoMutationException;
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
  private final String DELETE_IF = "deleteIf.js";

  public DeleteStatementHandler(CosmosClient client, TableMetadataManager metadataManager) {
    super(client, metadataManager);
  }

  @Override
  protected List<Record> execute(Operation operation) throws CosmosException {
    checkArgument(operation, Delete.class);
    Delete delete = (Delete) operation;

    if (delete.getCondition().isPresent()) {
      executeStoredProcedure(delete);
    } else {
      execute(delete);
    }

    return Collections.emptyList();
  }

  private void execute(Delete delete) throws CosmosException {
    String id = getId(delete);
    PartitionKey partitionKey = new PartitionKey(getConcatenatedPartitionKey(delete));
    CosmosItemRequestOptions options = new CosmosItemRequestOptions();

    getContainer(delete).deleteItem(id, partitionKey, options);
  }
}
