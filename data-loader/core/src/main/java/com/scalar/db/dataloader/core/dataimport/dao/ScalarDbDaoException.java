package com.scalar.db.dataloader.core.dataimport.dao;

/** A custom DAO exception that encapsulates errors thrown by ScalarDB operations */
public class ScalarDbDaoException extends Exception {

  /**
   * Class constructor
   *
   * @param message error message
   * @param cause reason for exception
   */
  public ScalarDbDaoException(String message, Throwable cause) {
    super(message, cause);
  }
}
