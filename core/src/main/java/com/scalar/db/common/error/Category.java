package com.scalar.db.common.error;

import java.util.Objects;

public enum Category {
  RETRYABLE_ERROR("3"),
  USER_ERROR("4"),
  INTERNAL_ERROR("5");

  private final String code;

  Category(String code) {
    if (Objects.requireNonNull(code).length() != 1) {
      throw new IllegalArgumentException("The length of the code must be 1");
    }
    this.code = code;
  }

  public String getCode() {
    return code;
  }
}
