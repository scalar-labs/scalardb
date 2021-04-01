package com.scalar.db.storage.cosmos;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosException;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
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

  public PutStatementHandler(CosmosClient client, CosmosTableMetadataManager metadataManager) {
    super(client, metadataManager);
  }

  @Override
  protected List<Record> execute(Operation operation) throws CosmosException {
    Mutation mutation = (Mutation) operation;

    executeStoredProcedure(mutation);

    return Collections.emptyList();
  }
}
