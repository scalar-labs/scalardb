package com.scalar.db.dataloader.core.dataimport.processor;

import com.scalar.db.common.error.CoreError;

public class DefaultImportProcessorFactory implements ImportProcessorFactory {

  /**
   * Create import processor object based in file format in import params
   *
   * @param params import processor params objects
   * @return generated import processor object
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
