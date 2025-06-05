package com.scalar.db.exception.transaction;

import com.scalar.db.api.AuthAdmin;
import javax.annotation.Nullable;

/**
 * An exception thrown when a transaction CRUD operation fails due to transient or nontransient
 * faults. You can try retrying the transaction from the beginning, but the transaction may still
 * fail if the cause is nontransient.
 */
public class CrudException extends TransactionException {

  public CrudException(String message, String transactionId) {
    super(message, transactionId);
  }

  public CrudException(String message, Throwable cause, String transactionId) {
    super(message, cause, transactionId);
  }

  public CrudException(
      String message,
      @Nullable String transactionId,
      boolean authenticationError,
      boolean authorizationError,
      @Nullable AuthAdmin.Privilege requiredPrivileges) {
    super(
        message, transactionId, authenticationError, authorizationError, false, requiredPrivileges);
  }

  public CrudException(
      String message,
      Throwable cause,
      @Nullable String transactionId,
      boolean authenticationError,
      boolean authorizationError,
      @Nullable AuthAdmin.Privilege requiredPrivileges) {
    super(
        message,
        cause,
        transactionId,
        authenticationError,
        authorizationError,
        false,
        requiredPrivileges);
  }
}
