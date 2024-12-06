package com.scalar.db.dataloader.core.util;

import static com.scalar.db.common.error.CoreError.DATA_LOADER_ERROR_METHOD_NULL_ARGUMENT;

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
        throw new NullPointerException(DATA_LOADER_ERROR_METHOD_NULL_ARGUMENT.getMessage());
      }
    }
  }
}
