package com.scalar.db.sql.exception;

public class RetriableSqlException extends SqlException {
  public RetriableSqlException() {
    super();
  }

  public RetriableSqlException(String message) {
    super(message);
  }

  public RetriableSqlException(String message, Throwable cause) {
    super(message, cause);
  }

  public RetriableSqlException(Throwable cause) {
    super(cause);
  }
}
