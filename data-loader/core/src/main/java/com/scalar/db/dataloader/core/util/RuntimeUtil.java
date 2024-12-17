package com.scalar.db.dataloader.core.util;

import static com.scalar.db.dataloader.core.ErrorMessage.ERROR_METHOD_NULL_ARGUMENT;

/** Utils for runtime checks */
public class RuntimeUtil {

  /**
   * Argument null check
   *
   * @param values List of arguments
   * @throws NullPointerException when one of the arguments is null
   */
  public static void checkNotNull(Object... values) {
    for (Object value : values) {
      if (value == null) {
        throw new NullPointerException(ERROR_METHOD_NULL_ARGUMENT);
      }
    }
  }
}
