package com.scalar.db.dataloader.core;

import java.util.Arrays;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class DataLoaderErrorTest {

  @Test
  public void checkDuplicateErrorCode() {
    Assertions.assertThat(Arrays.stream(DataLoaderError.values()).map(DataLoaderError::buildCode))
        .doesNotHaveDuplicates();
  }
}
