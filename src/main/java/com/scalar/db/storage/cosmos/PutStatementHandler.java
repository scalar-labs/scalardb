package com.scalar.db.storage.cosmos;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import java.util.Collections;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A handler class for put statements
 *
 * @author Yuji Ito
 */
@ThreadSafe
public class PutStatementHandler extends MutateStatementHandler {

  public PutStatementHandler(CosmosClient client, TableMetadataManager metadataManager) {
    super(client, metadataManager);
  }

  @Override
  protected List<Record> execute(Operation operation) throws CosmosException {
    Mutation mutation = (Mutation) operation;

    if (mutation.getCondition().isPresent()) {
      executeStoredProcedure(mutation);
    } else {
      execute(mutation);
    }

    return Collections.emptyList();
  }

  private void execute(Mutation mutation) throws CosmosException {
    CosmosMutation cosmosMutation = new CosmosMutation(mutation, metadataManager);
    cosmosMutation.checkArgument(Put.class);

    Record record = cosmosMutation.makeRecord();
    CosmosItemRequestOptions options = new CosmosItemRequestOptions();

    getContainer(mutation).upsertItem(record, options);
  }
}
