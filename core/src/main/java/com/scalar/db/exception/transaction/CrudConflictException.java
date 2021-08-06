package com.scalar.db.exception.transaction;

public class CrudConflictException extends CrudException {

  public CrudConflictException(String message) {
    super(message);
  }

  public CrudConflictException(String message, Throwable cause) {
    super(message, cause);
  }
}
