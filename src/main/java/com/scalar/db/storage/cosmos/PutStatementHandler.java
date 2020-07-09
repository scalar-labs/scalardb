package com.scalar.db.storage.cosmos;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.exception.storage.NoMutationException;
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
    checkArgument(operation, Put.class);
    Put put = (Put) operation;

    if (put.getCondition().isPresent()) {
      executeStoredProcedure(put);
    } else {
      execute(put);
    }

    return Collections.emptyList();
  }

  private void execute(Put put) throws CosmosException {
    Record record = makeRecord(put);
    CosmosItemRequestOptions options = new CosmosItemRequestOptions();

    getContainer(put).upsertItem(record, options);
  }
}
