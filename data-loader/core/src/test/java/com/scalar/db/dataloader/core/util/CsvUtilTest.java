package com.scalar.db.dataloader.core.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** Unit tests for CsvUtils */
class CsvUtilTest {

  @Test
  void removeTrailingDelimiter_HasTrailingDelimiter_ShouldRemoveDelimiter() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("testing;");
    CsvUtil.removeTrailingDelimiter(stringBuilder, ";");
    assertThat(stringBuilder.toString()).isEqualTo("testing");
  }

  @Test
  void removeTrailingDelimiter_DoesNotHaveTrailingDelimiter_ShouldNotRemoveAnything() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("testing");
    CsvUtil.removeTrailingDelimiter(stringBuilder, ";");
    assertThat(stringBuilder.toString()).isEqualTo("testing");
  }
}
