package com.scalar.db.common.error;

import java.util.Objects;

public enum Category {
  USER_ERROR("1"),
  CONCURRENCY_ERROR("2"),
  INTERNAL_ERROR("3"),
  UNKNOWN_TRANSACTION_STATUS_ERROR("4");

  private final String id;

  Category(String id) {
    Objects.requireNonNull(id, "The id must not be null.");
    if (id.length() != 1) {
      throw new IllegalArgumentException("The length of the id must be 1");
    }
    this.id = id;
  }

  public String getId() {
    return id;
  }
}
