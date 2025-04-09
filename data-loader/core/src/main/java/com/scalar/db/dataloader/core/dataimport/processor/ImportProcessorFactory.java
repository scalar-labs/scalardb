package com.scalar.db.dataloader.core.dataimport.processor;

/**
 * A factory interface for creating {@link ImportProcessor} instances. This factory follows the
 * Factory design pattern to encapsulate the creation of specific import processor implementations.
 */
public interface ImportProcessorFactory {

  /**
   * Creates a new instance of an {@link ImportProcessor}.
   *
   * @param params The parameters required for configuring the import processor
   * @return A new {@link ImportProcessor} instance configured with the provided parameters
   * @throws IllegalArgumentException if the provided parameters are invalid
   */
  ImportProcessor createImportProcessor(ImportProcessorParams params);
}
