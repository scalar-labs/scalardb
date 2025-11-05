package com.scalar.db.dataloader.cli.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.scalar.db.dataloader.core.DataLoaderError;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

class CommandLineInputUtilsTest {

  private File tempFile;

  @AfterEach
  void cleanUp() {
    if (tempFile != null && tempFile.exists()) {
      tempFile.delete();
    }
  }

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
        exception.getMessage().contains(DataLoaderError.SPLIT_INPUT_VALUE_NULL.buildMessage()));
  }

  @Test
  void splitByDelimiter_nullDelimiter_shouldThrowException() {
    NullPointerException exception =
        assertThrows(
            NullPointerException.class,
            () -> CommandLineInputUtils.splitByDelimiter("a=b", null, 2));
    assertTrue(
        exception.getMessage().contains(DataLoaderError.SPLIT_INPUT_DELIMITER_NULL.buildMessage()));
  }

  @Test
  public void validatePositiveValue_positiveValue_shouldNotThrowException() {
    // Arrange
    CommandLine commandLine = mock(CommandLine.class);
    int positiveValue = 5;

    // Act & Assert - No exception should be thrown
    assertDoesNotThrow(
        () ->
            CommandLineInputUtils.validatePositiveValue(
                commandLine, positiveValue, DataLoaderError.INVALID_DATA_CHUNK_SIZE));
  }

  @Test
  public void validatePositiveValue_one_shouldNotThrowException() {
    // Arrange
    CommandLine commandLine = mock(CommandLine.class);
    int minimumPositiveValue = 1;

    // Act & Assert - No exception should be thrown
    assertDoesNotThrow(
        () ->
            CommandLineInputUtils.validatePositiveValue(
                commandLine, minimumPositiveValue, DataLoaderError.INVALID_DATA_CHUNK_SIZE));
  }

  @Test
  public void validatePositiveValue_zero_shouldThrowException() {
    // Arrange
    CommandLine commandLine = mock(CommandLine.class);
    int zeroValue = 0;
    DataLoaderError error = DataLoaderError.INVALID_DATA_CHUNK_SIZE;

    // Act & Assert
    CommandLine.ParameterException exception =
        assertThrows(
            CommandLine.ParameterException.class,
            () -> CommandLineInputUtils.validatePositiveValue(commandLine, zeroValue, error));

    // Verify the exception message contains the error message
    assertTrue(exception.getMessage().contains(error.buildMessage()));
  }

  @Test
  public void validatePositiveValue_negativeValue_shouldThrowException() {
    // Arrange
    CommandLine commandLine = mock(CommandLine.class);
    int negativeValue = -5;
    DataLoaderError error = DataLoaderError.INVALID_TRANSACTION_SIZE;

    // Act & Assert
    CommandLine.ParameterException exception =
        assertThrows(
            CommandLine.ParameterException.class,
            () -> CommandLineInputUtils.validatePositiveValue(commandLine, negativeValue, error));

    // Verify the exception message contains the error message
    assertTrue(exception.getMessage().contains(error.buildMessage()));
  }

  @Test
  public void validatePositiveValue_differentErrorTypes_shouldUseCorrectErrorMessage() {
    // Arrange
    CommandLine commandLine = mock(CommandLine.class);
    int negativeValue = -1;

    // Act & Assert for DATA_LOADER_INVALID_MAX_THREADS
    CommandLine.ParameterException exception1 =
        assertThrows(
            CommandLine.ParameterException.class,
            () ->
                CommandLineInputUtils.validatePositiveValue(
                    commandLine, negativeValue, DataLoaderError.INVALID_MAX_THREADS));
    assertTrue(
        exception1.getMessage().contains(DataLoaderError.INVALID_MAX_THREADS.buildMessage()));

    // Act & Assert for DATA_LOADER_INVALID_DATA_CHUNK_QUEUE_SIZE
    CommandLine.ParameterException exception2 =
        assertThrows(
            CommandLine.ParameterException.class,
            () ->
                CommandLineInputUtils.validatePositiveValue(
                    commandLine, negativeValue, DataLoaderError.INVALID_DATA_CHUNK_QUEUE_SIZE));
    assertTrue(
        exception2
            .getMessage()
            .contains(DataLoaderError.INVALID_DATA_CHUNK_QUEUE_SIZE.buildMessage()));
  }

  @Test
  void testIsSingleCrudOperation_ReturnsTrue_WhenPropertyMatches() throws IOException {
    tempFile = createTempPropertiesFile("scalar.db.transaction_manager=single-crud-operation");

    boolean result = CommandLineInputUtils.isSingleCrudOperation(tempFile.getAbsolutePath());

    assertTrue(result, "Expected true when property is 'single-crud-operation'");
  }

  @Test
  void testIsSingleCrudOperation_ReturnsFalse_WhenPropertyDiffers() throws IOException {
    tempFile = createTempPropertiesFile("scalar.db.transaction_manager=two-phase-commit");

    boolean result = CommandLineInputUtils.isSingleCrudOperation(tempFile.getAbsolutePath());

    assertFalse(result, "Expected false when property is not 'single-crud-operation'");
  }

  @Test
  void testIsSingleCrudOperation_ReturnsFalse_WhenPropertyMissing() throws IOException {
    tempFile = createTempPropertiesFile("some.other.property=value");

    boolean result = CommandLineInputUtils.isSingleCrudOperation(tempFile.getAbsolutePath());

    assertFalse(result, "Expected false when property is missing");
  }

  @Test
  void testIsSingleCrudOperation_HandlesMissingFileGracefully() {
    boolean result = CommandLineInputUtils.isSingleCrudOperation("nonexistent.properties");

    // Should not throw an exception, just print stack trace and return false
    assertFalse(result, "Expected false when file does not exist");
  }

  private File createTempPropertiesFile(String content) throws IOException {
    File file = Files.createTempFile("test-config", ".properties").toFile();
    try (FileWriter writer = new FileWriter(file)) {
      writer.write(content);
    }
    return file;
  }
}
