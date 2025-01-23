package com.scalar.db.dataloader.core.dataimport.controlfile;

/** Represents the control file */
public class ControlFileValidationException extends Exception {

  /**
   * Class constructor
   *
   * @param message error message
   */
  public ControlFileValidationException(String message) {
    super(message);
  }
}
