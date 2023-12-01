package com.scalar.db.exception.transaction;

import com.scalar.db.api.AuthAdmin;
import java.util.Optional;
import javax.annotation.Nullable;

/** Base class for all exceptions thrown by the Transaction API. */
public class TransactionException extends Exception {

  @Nullable private final String transactionId;
  private final boolean authenticationError;
  private final boolean authorizationError;
  private final boolean superuserRequired;
  @Nullable private final AuthAdmin.Privilege requiredPrivilege;

  public TransactionException(String message, @Nullable String transactionId) {
    this(message, transactionId, false, false, false, null);
  }

  public TransactionException(String message, Throwable cause, @Nullable String transactionId) {
    this(message, cause, transactionId, false, false, false, null);
  }

  public TransactionException(
      String message,
      @Nullable String transactionId,
      boolean authenticationError,
      boolean authorizationError,
      boolean superuserRequired,
      @Nullable AuthAdmin.Privilege requiredPrivilege) {
    super(addTransactionIdToMessage(message, transactionId));
    this.transactionId = transactionId;
    this.authenticationError = authenticationError;
    this.authorizationError = authorizationError;
    this.superuserRequired = superuserRequired;
    this.requiredPrivilege = requiredPrivilege;
  }

  public TransactionException(
      String message,
      Throwable cause,
      @Nullable String transactionId,
      boolean authenticationError,
      boolean authorizationError,
      boolean superuserRequired,
      @Nullable AuthAdmin.Privilege requiredPrivilege) {
    super(addTransactionIdToMessage(message, transactionId), cause);
    this.transactionId = transactionId;
    this.authenticationError = authenticationError;
    this.authorizationError = authorizationError;
    this.superuserRequired = superuserRequired;
    this.requiredPrivilege = requiredPrivilege;
  }

  /**
   * Returns the transaction ID associated with the transaction that threw the exception.
   *
   * @return the transaction ID associated with the transaction that threw the exception
   */
  public Optional<String> getTransactionId() {
    return Optional.ofNullable(transactionId);
  }

  private static String addTransactionIdToMessage(String message, @Nullable String transactionId) {
    if (transactionId == null) {
      return message;
    }

    String suffix = ". transaction ID: " + transactionId;

    // To avoid a duplicated transaction ID, check if the message already has a transaction ID
    if (!message.endsWith(suffix)) {
      return message + suffix;
    } else {
      return message;
    }
  }

  public boolean isAuthenticationError() {
    return authenticationError;
  }

  public boolean isAuthorizationError() {
    return authorizationError;
  }

  public boolean isSuperuserRequired() {
    return superuserRequired;
  }

  public Optional<AuthAdmin.Privilege> getRequiredPrivilege() {
    return Optional.ofNullable(requiredPrivilege);
  }
}
