package com.scalar.db.common.error;

import java.util.Objects;

public interface ScalarDbError {

  String getComponentName();

  Category getCategory();

  SubCategory getSubCategory();

  String getCode();

  String getMessage();

  String getCause();

  String getSolution();

  default void validate(
      String componentName,
      Category category,
      SubCategory subCategory,
      String code,
      String message,
      String cause,
      String solution) {
    Objects.requireNonNull(componentName, "The component name must not be null.");
    Objects.requireNonNull(category, "The category must not be null.");

    Objects.requireNonNull(subCategory, "The sub-category must not be null.");
    if (subCategory.getParent() != category) {
      throw new IllegalArgumentException(
          "The parent category of the sub-category must be the same as the category.");
    }

    Objects.requireNonNull(code, "The code must not be null.");
    if (code.length() != 3) {
      throw new IllegalArgumentException("The length of the code must be 3");
    }

    Objects.requireNonNull(message, "The message must not be null.");
    Objects.requireNonNull(cause, "The cause must not be null.");
    Objects.requireNonNull(solution, "The solution must not be null.");
  }

  /**
   * Builds the error code. The code is built as follows:
   *
   * <p>{@code <componentName>-<categoryCode><subCategoryCode><code>}
   *
   * @return the built code
   */
  default String buildCode() {
    return getComponentName()
        + "-"
        + getCategory().getCode()
        + getSubCategory().getCode()
        + getCode();
  }

  /**
   * Builds the error message with the given arguments. The message is built as follows:
   *
   * <p>{@code <componentName>-<categoryCode><subCategoryCode><code>: <message>}
   *
   * @param args the arguments to be formatted into the message
   * @return the formatted message
   */
  default String buildMessage(Object... args) {
    return buildCode()
        + ": "
        + (args.length == 0 ? getMessage() : String.format(getMessage(), args));
  }
}
