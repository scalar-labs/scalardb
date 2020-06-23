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

@ThreadSafe
public class PutStatementHandler extends MutateStatementHandler {
  private final String PUT_IF_NOT_EXISTS = "putIfNotExists.js";
  private final String PUT_IF = "putIf.js";

  public PutStatementHandler(CosmosClient client, TableMetadataHandler metadataHandler) {
    super(client, metadataHandler);
  }

  @Override
  protected List<Record> execute(Operation operation) throws CosmosException, NoMutationException {
    checkArgument(operation, Put.class);
    Put put = (Put) operation;

    if (put.getCondition().isPresent()) {
      if (put.getCondition().get() instanceof PutIfNotExists) {
        executeStoredProcedure(PUT_IF_NOT_EXISTS, put);
      } else {
        executeStoredProcedure(PUT_IF, put);
      }
    } else {
      execute(put);
    }

    return Collections.emptyList();
  }

  private void execute(Put put) throws CosmosException {
    Record record = makeRecord(put).get();
    CosmosItemRequestOptions options = new CosmosItemRequestOptions();

    getContainer(put).upsertItem(record, options);
  }
}
