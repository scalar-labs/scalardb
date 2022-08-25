package com.scalar.db.storage.dynamo.request;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class PaginatedRequestResponse {

  private final List<Map<String, AttributeValue>> items;
  private final boolean hasLastEvaluatedKey;
  private final Map<String, AttributeValue> lastEvaluatedKey;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public PaginatedRequestResponse(
      List<Map<String, AttributeValue>> items,
      boolean hasLastEvaluatedKey,
      Map<String, AttributeValue> lastEvaluatedKey) {
    this.items = items;
    this.hasLastEvaluatedKey = hasLastEvaluatedKey;
    this.lastEvaluatedKey = lastEvaluatedKey;
  }

  @SuppressFBWarnings("EI_EXPOSE_REP")
  public List<Map<String, AttributeValue>> items() {
    return items;
  }

  @SuppressFBWarnings("EI_EXPOSE_REP")
  public Map<String, AttributeValue> lastEvaluatedKey() {
    return lastEvaluatedKey;
  }

  public boolean hasLastEvaluatedKey() {
    return hasLastEvaluatedKey;
  }
}
