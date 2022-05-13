package com.scalar.db.storage.dynamo.request;

import java.util.List;
import java.util.Map;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class PaginatedRequestResponse {

  private final List<Map<String, AttributeValue>> items;
  private final boolean hasLastEvaluatedKey;
  private final Map<String, AttributeValue> lastEvaluatedKey;

  public PaginatedRequestResponse(
      List<Map<String, AttributeValue>> items,
      boolean hasLastEvaluatedKey,
      Map<String, AttributeValue> lastEvaluatedKey) {
    this.items = items;
    this.hasLastEvaluatedKey = hasLastEvaluatedKey;
    this.lastEvaluatedKey = lastEvaluatedKey;
  }

  public List<Map<String, AttributeValue>> items() {
    return items;
  }

  public Map<String, AttributeValue> lastEvaluatedKey() {
    return lastEvaluatedKey;
  }

  public boolean hasLastEvaluatedKey() {
    return hasLastEvaluatedKey;
  }
}
