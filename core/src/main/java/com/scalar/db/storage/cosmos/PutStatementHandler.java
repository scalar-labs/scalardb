package com.scalar.db.storage.cosmos;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosException;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.util.TableMetadataManager;
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
  protected List<Record> execute(Operation operation) throws CosmosException, ExecutionException {
    Mutation mutation = (Mutation) operation;
    TableMetadata tableMetadata = metadataManager.getTableMetadata(mutation);
    executeStoredProcedure(mutation, tableMetadata);
    return Collections.emptyList();
  }
}
