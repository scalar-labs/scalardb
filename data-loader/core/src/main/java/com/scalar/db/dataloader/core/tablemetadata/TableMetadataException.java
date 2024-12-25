package com.scalar.db.dataloader.core.tablemetadata;

/** A custom exception that encapsulates errors thrown by the TableMetaDataService */
public class TableMetadataException extends Exception {

  /**
   * Class constructor
   *
   * @param message error message
   * @param cause reason for exception
   */
  public TableMetadataException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Class constructor
   *
   * @param message error message
   */
  public TableMetadataException(String message) {
    super(message);
  }
}
