package com.scalar.db.dataloader.core.dataexport.validation;

/** A custom exception for export options validation errors */
public class ExportOptionsValidationException extends Exception {

  /**
   * Class constructor
   *
   * @param message error message
   */
  public ExportOptionsValidationException(String message) {
    super(message);
  }
}
