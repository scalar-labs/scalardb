package com.scalar.db.storage.dynamo.request;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Map;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

public class QueryRequest implements PaginatedRequest {

  private final software.amazon.awssdk.services.dynamodb.model.QueryRequest dynamoRequest;
  private final DynamoDbClient client;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public QueryRequest(
      DynamoDbClient client,
      software.amazon.awssdk.services.dynamodb.model.QueryRequest dynamoRequest) {
    this.client = client;
    this.dynamoRequest = dynamoRequest;
  }

  @Override
  public PaginatedRequestResponse execute() {
    QueryResponse response = client.query(dynamoRequest);

    return new PaginatedRequestResponse(
        response.items(), response.hasLastEvaluatedKey(), response.lastEvaluatedKey());
  }

  @Override
  public PaginatedRequestResponse execute(int limit) {
    QueryRequest request =
        new QueryRequest(this.client, this.dynamoRequest.toBuilder().limit(limit).build());
    return request.execute();
  }

  @Override
  public PaginatedRequestResponse execute(Map<String, AttributeValue> exclusiveStartKey) {
    QueryRequest request =
        new QueryRequest(
            this.client,
            this.dynamoRequest.toBuilder().exclusiveStartKey(exclusiveStartKey).build());
    return request.execute();
  }

  @Override
  public PaginatedRequestResponse execute(
      Map<String, AttributeValue> exclusiveStartKey, int limit) {
    QueryRequest request =
        new QueryRequest(
            this.client,
            this.dynamoRequest
                .toBuilder()
                .exclusiveStartKey(exclusiveStartKey)
                .limit(limit)
                .build());
    return request.execute();
  }
}
