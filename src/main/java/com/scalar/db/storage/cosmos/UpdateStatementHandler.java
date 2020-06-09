package com.scalar.db.storage.cosmos;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientException;
import com.azure.cosmos.CosmosStoredProcedure;
import com.azure.cosmos.models.CosmosStoredProcedureRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import java.util.Collections;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class UpdateStatementHandler extends MutateStatementHandler {

  public UpdateStatementHandler(CosmosClient client, TableMetadataHandler metadataHandler) {
    super(client, metadataHandler);
  }

  @Override
  protected List<Record> execute(Operation operation) throws CosmosClientException {
    checkArgument(operation, Put.class);
    Put put = (Put) operation;

    CosmosStoredProcedureRequestOptions options =
        new CosmosStoredProcedureRequestOptions()
            .setConsistencyLevel(convert(put))
            .setPartitionKey(new PartitionKey(getConcatPartitionKey(put)));

    Record record = makeRecord(put);
    MapVisitor visitor = new MapVisitor();
    put.getValues()
        .values()
        .forEach(
            v -> {
              v.accept(visitor);
            });
    record.setValues(visitor.get());

    String query = makeConditionalQuery(put);

    CosmosStoredProcedure storedProcedure =
        getContainer(put).getScripts().getStoredProcedure("PutIf.js");
    storedProcedure.execute(new Object[] {record, query}, options);

    return Collections.emptyList();
  }
}
