package com.scalar.db.dataloader.core.util;

import static com.scalar.db.common.error.CoreError.DATA_LOADER_ERROR_METHOD_NULL_ARGUMENT;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/** RuntimeUtils unit tests */
class RuntimeUtilTest {

  @Test
  void checkNotNull_HasNullValues_ShouldThrowException() {
    assertThatThrownBy(() -> RuntimeUtil.checkNotNull(null, null))
        .isExactlyInstanceOf(NullPointerException.class)
        .hasMessage(DATA_LOADER_ERROR_METHOD_NULL_ARGUMENT.buildMessage());
  }

  @Test
  void checkNotNull_HasNoNullValues_ShouldNotThrowException() {
    String string = "1";
    Object object = new Object();
    RuntimeUtil.checkNotNull(string, object);
  }
}
