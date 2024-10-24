package com.scalar.db.storage.dynamo.request;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Map;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

public class ScanRequest implements PaginatedRequest {

  private final software.amazon.awssdk.services.dynamodb.model.ScanRequest dynamoRequest;
  private final DynamoDbClient client;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public ScanRequest(
      DynamoDbClient client,
      software.amazon.awssdk.services.dynamodb.model.ScanRequest dynamoRequest) {
    this.client = client;
    this.dynamoRequest = dynamoRequest;
  }

  @Override
  public PaginatedRequestResponse execute() {
    ScanResponse response = client.scan(dynamoRequest);

    return new PaginatedRequestResponse(
        response.items(), response.hasLastEvaluatedKey(), response.lastEvaluatedKey());
  }

  @Override
  public PaginatedRequestResponse execute(int limit) {
    ScanRequest request =
        new ScanRequest(this.client, this.dynamoRequest.toBuilder().limit(limit).build());
    return request.execute();
  }

  @Override
  public PaginatedRequestResponse execute(Map<String, AttributeValue> exclusiveStartKey) {
    ScanRequest request =
        new ScanRequest(
            this.client,
            this.dynamoRequest.toBuilder().exclusiveStartKey(exclusiveStartKey).build());
    return request.execute();
  }

  @Override
  public PaginatedRequestResponse execute(
      Map<String, AttributeValue> exclusiveStartKey, int limit) {
    ScanRequest request =
        new ScanRequest(
            this.client,
            this.dynamoRequest
                .toBuilder()
                .exclusiveStartKey(exclusiveStartKey)
                .limit(limit)
                .build());
    return request.execute();
  }
}
