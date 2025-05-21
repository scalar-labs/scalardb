package com.scalar.db.dataloader.cli.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.scalar.db.common.error.CoreError;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CommandLineInputUtilsTest {

  @Test
  public void parseKeyValue_validKeyValue_ShouldReturnEntry() {
    Map.Entry<String, String> result = CommandLineInputUtils.parseKeyValue("foo=bar");

    assertEquals("foo", result.getKey());
    assertEquals("bar", result.getValue());
  }

  @Test
  public void parseKeyValue_whitespaceTrimmed_ShouldReturnTrimmedEntry() {
    Map.Entry<String, String> result = CommandLineInputUtils.parseKeyValue("  key  =  value  ");

    assertEquals("key", result.getKey());
    assertEquals("value", result.getValue());
  }

  @Test
  public void parseKeyValue_nullInput_ShouldThrowException() {
    assertThrows(IllegalArgumentException.class, () -> CommandLineInputUtils.parseKeyValue(null));
  }

  @Test
  public void parseKeyValue_emptyInput_ShouldThrowException() {
    assertThrows(IllegalArgumentException.class, () -> CommandLineInputUtils.parseKeyValue(" "));
  }

  @Test
  public void parseKeyValue_missingEquals_ShouldThrowException() {
    assertThrows(
        IllegalArgumentException.class, () -> CommandLineInputUtils.parseKeyValue("keyvalue"));
  }

  @Test
  public void parseKeyValue_emptyKey_ShouldThrowException() {
    assertThrows(
        IllegalArgumentException.class, () -> CommandLineInputUtils.parseKeyValue(" =value"));
  }

  @Test
  public void parseKeyValue_emptyValue_ShouldThrowException() {
    assertThrows(
        IllegalArgumentException.class, () -> CommandLineInputUtils.parseKeyValue("key= "));
  }

  @Test
  public void parseKeyValue_multipleEquals_ShouldParseFirstOnly() {
    Map.Entry<String, String> result = CommandLineInputUtils.parseKeyValue("key=val=ue");

    assertEquals("key", result.getKey());
    assertEquals("val=ue", result.getValue());
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
