package com.scalar.db.storage.dynamo.request;

import java.util.Map;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/** This interface abstracts DynamoDB requests that are paginated */
public interface PaginatedRequest {
  /**
   * Execute the request
   *
   * @return the response
   */
  PaginatedRequestResponse execute();

  /**
   * Execute the request with limit
   *
   * @param limit the maximum number of items to evaluate (not necessarily the number of matching
   *     items)
   * @return the response
   */
  PaginatedRequestResponse execute(int limit);

  /**
   * Execute the request that will be evaluated starting from the given start key
   *
   * @param exclusiveStartKey the primary key of the first item that this operation will evaluate.
   * @return the response
   */
  PaginatedRequestResponse execute(Map<String, AttributeValue> exclusiveStartKey);

  /**
   * Execute the request that will be evaluated starting from the given start key with limit
   *
   * @param exclusiveStartKey the primary key of the first item that this operation will evaluate.
   * @param limit the maximum number of items to evaluate (not necessarily the number of matching
   *     items)
   * @return the response
   */
  PaginatedRequestResponse execute(Map<String, AttributeValue> exclusiveStartKey, int limit);
}
