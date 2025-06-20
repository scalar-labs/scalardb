package com.scalar.db.dataloader.core.util;

import com.scalar.db.dataloader.core.DataLoaderError;

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
        throw new NullPointerException(DataLoaderError.ERROR_METHOD_NULL_ARGUMENT.buildMessage());
      }
    }
  }
}
