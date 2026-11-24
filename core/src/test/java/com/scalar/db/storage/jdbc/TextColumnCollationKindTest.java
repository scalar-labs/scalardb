package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class TextColumnCollationKindTest {

  @ParameterizedTest
  @CsvSource({
    "true,true,CODE_POINT_ORDER",
    "true,false,OTHER_ORDER",
    "false,true,PARTIAL_CHARACTER_SET",
    "false,false,PARTIAL_CHARACTER_SET"
  })
  void of_ShouldClassifyByCharacterSetFirst(
      boolean fullUnicodeCharset, boolean binaryCollation, TextColumnCollationKind expected) {
    // Act
    TextColumnCollationKind actual =
        TextColumnCollationKind.of(fullUnicodeCharset, binaryCollation);

    // Assert
    assertThat(actual).isEqualTo(expected);
  }
}
