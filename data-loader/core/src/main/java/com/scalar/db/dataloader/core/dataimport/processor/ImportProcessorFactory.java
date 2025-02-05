package com.scalar.db.dataloader.core.dataimport.processor;

public interface ImportProcessorFactory {
  ImportProcessor createImportProcessor(ImportProcessorParams params);
}
