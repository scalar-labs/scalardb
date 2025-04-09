package com.scalar.db.dataloader.core.dataimport.processor;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.scalar.db.dataloader.core.FileFormat;
import com.scalar.db.dataloader.core.dataimport.ImportOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DefaultImportProcessorFactory} class. Tests the factory's ability to create
 * appropriate import processors based on different file formats.
 */
class DefaultImportProcessorFactoryTest {

  private DefaultImportProcessorFactory factory;

  @BeforeEach
  void setUp() {
    factory = new DefaultImportProcessorFactory();
  }

  /**
   * Tests that the factory creates a {@link JsonLinesImportProcessor} when JSONL format is
   * specified.
   */
  @Test
  void createImportProcessor_givenFileFormatIsJsonl_shouldReturnJsonLinesImportProcessor() {
    // Arrange
    ImportOptions importOptions = ImportOptions.builder().fileFormat(FileFormat.JSONL).build();
    ImportProcessorParams params =
        ImportProcessorParams.builder().importOptions(importOptions).build();

    // Act
    ImportProcessor result = factory.createImportProcessor(params);

    // Assert
    assertInstanceOf(JsonLinesImportProcessor.class, result);
  }

  /** Tests that the factory creates a {@link JsonImportProcessor} when JSON format is specified. */
  @Test
  void createImportProcessor_givenFileFormatIsJson_shouldReturnJsonImportProcessor() {
    // Given
    ImportOptions importOptions = ImportOptions.builder().fileFormat(FileFormat.JSON).build();
    ImportProcessorParams params =
        ImportProcessorParams.builder().importOptions(importOptions).build();

    // When
    ImportProcessor result = factory.createImportProcessor(params);

    // Then
    assertInstanceOf(JsonImportProcessor.class, result);
  }

  /** Tests that the factory creates a {@link CsvImportProcessor} when CSV format is specified. */
  @Test
  void createImportProcessor_givenFileFormatIsCsv_shouldReturnCsvImportProcessor() {
    // Given
    ImportOptions importOptions = ImportOptions.builder().fileFormat(FileFormat.CSV).build();
    ImportProcessorParams params =
        ImportProcessorParams.builder().importOptions(importOptions).build();

    // When
    ImportProcessor result = factory.createImportProcessor(params);

    // Then
    assertInstanceOf(CsvImportProcessor.class, result);
  }
}
