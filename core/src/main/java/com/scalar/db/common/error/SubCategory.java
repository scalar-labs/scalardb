package com.scalar.db.common.error;

import java.util.Objects;

public enum SubCategory {

  //
  // Sub-categories for retryable errors
  //
  TRANSACTION_CONFLICT(Category.RETRYABLE_ERROR, "0"),
  TRANSACTION_NOT_FOUND(Category.RETRYABLE_ERROR, "1"),

  //
  // Sub-categories for user errors
  //
  ILLEGAL_ARGUMENT(Category.USER_ERROR, "0"),
  AUTHENTICATION_ERROR(Category.USER_ERROR, "1"),
  AUTHORIZATION_ERROR(Category.USER_ERROR, "2"),

  //
  // Sub-categories for internal errors
  //
  UNKNOWN_TRANSACTION_STATUS(Category.INTERNAL_ERROR, "0"),
  UNKNOWN(Category.INTERNAL_ERROR, "9");

  private final Category parent;
  private final String code;

  SubCategory(Category parent, String code) {
    this.parent = Objects.requireNonNull(parent);

    if (Objects.requireNonNull(code).length() != 1) {
      throw new IllegalArgumentException("The length of the code must be 1");
    }
    this.code = code;
  }

  Category getParent() {
    return parent;
  }

  String getCode() {
    return code;
  }
}
