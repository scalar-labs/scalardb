package com.scalar.db.dataloader.core.dataimport.processor;

import com.scalar.db.common.error.CoreError;

/**
 * A factory class that creates appropriate ImportProcessor instances based on the input file
 * format. This factory implements the ImportProcessorFactory interface and provides a default
 * implementation for creating processors that handle different file formats (JSON, JSONL, CSV).
 */
public class DefaultImportProcessorFactory implements ImportProcessorFactory {

  /**
   * Creates an appropriate ImportProcessor instance based on the file format specified in the
   * import parameters.
   *
   * @param params ImportProcessorParams containing configuration and import options, including the
   *     file format
   * @return An ImportProcessor instance configured for the specified file format
   * @throws IllegalArgumentException if the specified file format is not supported
   *     <p>Supported file formats:
   *     <ul>
   *       <li>JSONL - Creates a JsonLinesImportProcessor for JSON Lines format
   *       <li>JSON - Creates a JsonImportProcessor for JSON format
   *       <li>CSV - Creates a CsvImportProcessor for CSV format
   *     </ul>
   */
  @Override
  public ImportProcessor createImportProcessor(ImportProcessorParams params) {
    ImportProcessor importProcessor;
    switch (params.getImportOptions().getFileFormat()) {
      case JSONL:
        importProcessor = new JsonLinesImportProcessor(params);
        break;
      case JSON:
        importProcessor = new JsonImportProcessor(params);
        break;
      case CSV:
        importProcessor = new CsvImportProcessor(params);
        break;
      default:
        throw new IllegalArgumentException(
            CoreError.DATA_LOADER_FILE_FORMAT_NOT_SUPPORTED.buildMessage(
                params.getImportOptions().getFileFormat().toString()));
    }
    return importProcessor;
  }
}
