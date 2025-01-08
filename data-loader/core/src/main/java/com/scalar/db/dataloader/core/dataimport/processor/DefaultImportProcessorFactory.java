package com.scalar.db.dataloader.core.dataimport.processor;

public class DefaultImportProcessorFactory implements ImportProcessorFactory {

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
        importProcessor = null;
    }
    return importProcessor;
  }
}
