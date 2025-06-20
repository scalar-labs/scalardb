package com.scalar.db.schemaloader;

import java.util.Arrays;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class SchemaLoaderErrorTest {

  @Test
  public void checkDuplicateErrorCode() {
    Assertions.assertThat(
            Arrays.stream(SchemaLoaderError.values()).map(SchemaLoaderError::buildCode))
        .doesNotHaveDuplicates();
  }
}
