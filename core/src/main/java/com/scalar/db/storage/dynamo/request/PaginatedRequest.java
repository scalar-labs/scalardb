package com.scalar.db.storage.dynamo.request;

import java.util.Map;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/** This interface abstracts DynamoDB requests that are paginated */
public interface PaginatedRequest {
  /**
   * Execute the request
   *
   * @return the request response
   */
  PaginatedRequestResponse execute();

  /**
   * Execute the request that will be evaluated starting from the given start key
   *
   * @param exclusiveStartKey The primary key of the first item that this operation will evaluate.
   * @return the request response
   */
  PaginatedRequestResponse execute(Map<String, AttributeValue> exclusiveStartKey);

  /**
   * Returns the request limit
   *
   * @return the request limit
   */
  Integer limit();
}
