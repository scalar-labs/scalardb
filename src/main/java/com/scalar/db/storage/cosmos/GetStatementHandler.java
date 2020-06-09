package com.scalar.db.storage.cosmos;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.PartitionKey;
import com.scalar.db.api.Get;
import com.scalar.db.api.Operation;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nonnull;

public class GetStatementHandler extends StatementHandler {
  public GetStatementHandler(CosmosClient client, TableMetadataHandler metadataHandler) {
    super(client, metadataHandler);
  }

  @Override
  @Nonnull
  protected List<Record> execute(Operation operation) {
    checkArgument(operation, Get.class);

    String id = getId(operation);
    CosmosItemResponse<Record> response =
        getContainer(operation)
            .readItem(
                id, new PartitionKey(getConcatPartitionKey(operation)), Record.class);

    return Arrays.asList(response.getItem());
  }
}
