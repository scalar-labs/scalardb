package com.scalar.db.common.error;

import java.util.Objects;

public enum Category {
  USER_ERROR("1"),
  TRANSACTION_RETRYABLE_ERROR("2"),
  INTERNAL_ERROR("3");

  private final String id;

  Category(String id) {
    if (Objects.requireNonNull(id).length() != 1) {
      throw new IllegalArgumentException("The length of the id must be 1");
    }
    this.id = id;
  }

  public String getId() {
    return id;
  }
}
