package com.scalar.db.storage.cosmos;

/**
 * Error code of {@code CosmosException}. Refer to ErrorCodes in
 * https://azure.github.io/azure-cosmosdb-js-server/Collection.html
 */
enum CosmosErrorCode {
  BAD_REQUEST(400),
  FORBIDDEN(403),
  NOT_FOUND(404),
  CONFLICT(409),
  PRECONDITION_FAILED(412),
  REQUEST_ENTITY_TOO_LARGE(413),
  RETRY_WITH(449),
  INTERNAL_SERVER_ERROR(500);

  private final int code;

  CosmosErrorCode(int code) {
    this.code = code;
  }

  public int get() {
    return this.code;
  }
}
