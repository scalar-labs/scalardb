package com.scalar.db.dataloader.core.dataimport.processor;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.scalar.db.dataloader.core.FileFormat;
import com.scalar.db.dataloader.core.dataimport.ImportOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultImportProcessorFactoryTest {

  private DefaultImportProcessorFactory factory;

  @BeforeEach
  void setUp() {
    factory = new DefaultImportProcessorFactory();
  }

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
