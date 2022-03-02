package com.scalar.db.sql.exception;

public class SqlException extends RuntimeException {
  public SqlException() {
    super();
  }

  public SqlException(String message) {
    super(message);
  }

  public SqlException(String message, Throwable cause) {
    super(message, cause);
  }

  public SqlException(Throwable cause) {
    super(cause);
  }
}
