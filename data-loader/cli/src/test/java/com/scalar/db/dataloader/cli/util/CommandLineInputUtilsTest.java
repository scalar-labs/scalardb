package com.scalar.db.dataloader.cli.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.scalar.db.common.error.CoreError;
import com.scalar.db.dataloader.core.ColumnKeyValue;
import org.junit.jupiter.api.Test;

class CommandLineInputUtilsTest {

  @Test
  void parseKeyValue_validInput_shouldReturnKeyValue() {
    ColumnKeyValue result = CommandLineInputUtils.parseKeyValue("name=John");
    assertEquals("name", result.getColumnName());
    assertEquals("John", result.getColumnValue());
  }

  @Test
  void parseKeyValue_noEqualSign_shouldThrowException() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> CommandLineInputUtils.parseKeyValue("invalidpair"));
    assertTrue(exception.getMessage().contains("Invalid key-value format"));
  }

  @Test
  void parseKeyValue_emptyKey_shouldThrowException() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> CommandLineInputUtils.parseKeyValue("=value"));
    assertTrue(exception.getMessage().contains("Invalid key-value format"));
  }

  @Test
  void parseKeyValue_emptyValue_shouldThrowException() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> CommandLineInputUtils.parseKeyValue("key="));
    assertTrue(
        exception
            .getMessage()
            .contains(CoreError.DATA_LOADER_INVALID_KEY_VALUE_INPUT.buildMessage("key=")));
  }

  @Test
  void parseKeyValue_blankInput_shouldThrowException() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> CommandLineInputUtils.parseKeyValue("   "));
    assertTrue(
        exception
            .getMessage()
            .contains(CoreError.DATA_LOADER_NULL_OR_EMPTY_KEY_VALUE_INPUT.buildMessage()));
  }

  @Test
  void parseKeyValue_nullInput_shouldThrowException() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> CommandLineInputUtils.parseKeyValue(null));
    assertTrue(
        exception
            .getMessage()
            .contains(CoreError.DATA_LOADER_NULL_OR_EMPTY_KEY_VALUE_INPUT.buildMessage()));
  }

  @Test
  void splitByDelimiter_validSplit_shouldReturnArray() {
    String[] result = CommandLineInputUtils.splitByDelimiter("a=b", "=", 2);
    assertArrayEquals(new String[] {"a", "b"}, result);
  }

  @Test
  void splitByDelimiter_multipleDelimiters_shouldSplitAll() {
    String[] result = CommandLineInputUtils.splitByDelimiter("a=b=c", "=", 0);
    assertArrayEquals(new String[] {"a", "b", "c"}, result);
  }

  @Test
  void splitByDelimiter_nullValue_shouldThrowException() {
    NullPointerException exception =
        assertThrows(
            NullPointerException.class, () -> CommandLineInputUtils.splitByDelimiter(null, "=", 2));
    assertTrue(
        exception
            .getMessage()
            .contains(CoreError.DATA_LOADER_SPLIT_INPUT_VALUE_NULL.buildMessage()));
  }

  @Test
  void splitByDelimiter_nullDelimiter_shouldThrowException() {
    NullPointerException exception =
        assertThrows(
            NullPointerException.class,
            () -> CommandLineInputUtils.splitByDelimiter("a=b", null, 2));
    assertTrue(
        exception
            .getMessage()
            .contains(CoreError.DATA_LOADER_SPLIT_INPUT_DELIMITER_NULL.buildMessage()));
  }
}
