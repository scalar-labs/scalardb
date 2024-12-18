package com.scalar.db.dataloader.core.exception;

/** Exception thrown when an error occurs while trying to encode or decode base64 values. */
public class Base64Exception extends Exception {

  /**
   * Class constructor
   *
   * @param message Exception message
   */
  public Base64Exception(String message) {
    super(message);
  }
}
