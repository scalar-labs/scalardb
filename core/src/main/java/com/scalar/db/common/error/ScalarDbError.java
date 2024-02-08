package com.scalar.db.common.error;

import java.util.Objects;

public interface ScalarDbError {

  String getComponentName();

  Category getCategory();

  String getId();

  String getMessage();

  String getCause();

  String getSolution();

  // This method validates the error. It is called in the constructor of the enum to ensure that the
  // error is valid.
  default void validate(
      String componentName,
      Category category,
      String id,
      String message,
      String cause,
      String solution) {
    Objects.requireNonNull(componentName, "The component name must not be null.");
    Objects.requireNonNull(category, "The category must not be null.");

    Objects.requireNonNull(id, "The id must not be null.");
    if (id.length() != 4) {
      throw new IllegalArgumentException("The length of the id must be 4");
    }

    Objects.requireNonNull(message, "The message must not be null.");
    Objects.requireNonNull(cause, "The cause must not be null.");
    Objects.requireNonNull(solution, "The solution must not be null.");
  }

  /**
   * Builds the error code. The code is built as follows:
   *
   * <p>{@code <componentName>-<categoryId><id>}
   *
   * @return the built code
   */
  default String buildCode() {
    return getComponentName() + "-" + getCategory().getId() + getId();
  }

  /**
   * Builds the error message with the given arguments. The message is built as follows:
   *
   * <p>{@code <componentName>-<categoryId><id>: <message>}
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
