package com.scalar.db.storage.dynamo;

import software.amazon.awssdk.core.exception.SdkException;

final class SdkAttempts {
  private SdkAttempts() {}

  /**
   * Returns whether an earlier attempt of the failed request may have been applied. The AWS SDK
   * resends a request after a lost response or a server error, and a single-item write request
   * carries no idempotency token, so a resent conditional write can fail because of its own
   * earlier, applied attempt.
   *
   * @param e the exception thrown by the AWS SDK
   * @return whether an earlier attempt of the request may have been applied
   */
  static boolean earlierAttemptMayHaveBeenApplied(SdkException e) {
    Integer numAttempts = e.numAttempts();
    return numAttempts != null && numAttempts > 1;
  }
}
