package com.scalar.db.storage.cosmos;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientException;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import java.util.Collections;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class InsertStatementHandler extends MutateStatementHandler {

  public InsertStatementHandler(CosmosClient client, TableMetadataHandler metadataHandler) {
    super(client, metadataHandler);
  }

  @Override
  protected List<Record> execute(Operation operation) throws CosmosClientException {
    checkArgument(operation, Put.class);
    Put put = (Put) operation;

    CosmosItemRequestOptions options =
        new CosmosItemRequestOptions().setConsistencyLevel(convert(put));

    TableMetadata metadata = metadataHandler.getTableMetadata(put);
    PartitionKey partitionKey = new PartitionKey(getConcatPartitionKey(put));

    Record record = makeRecord(put);
    MapVisitor visitor = new MapVisitor();
    put.getValues()
        .values()
        .forEach(
            v -> {
              v.accept(visitor);
            });
    record.setValues(visitor.get());

    getContainer(put).createItem(record, partitionKey, options);

    return Collections.emptyList();
  }
}
