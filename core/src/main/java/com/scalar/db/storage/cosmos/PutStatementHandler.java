package com.scalar.db.storage.cosmos;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosException;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.exception.storage.ExecutionException;
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
  protected void execute(Mutation mutation) throws CosmosException, ExecutionException {
    TableMetadata tableMetadata = metadataManager.getTableMetadata(mutation);
    executeStoredProcedure(mutation, tableMetadata);
  }
}
