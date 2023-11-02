package com.scalar.db.exception.storage;

import com.scalar.db.api.AuthAdmin;
import java.util.Optional;
import javax.annotation.Nullable;

public class ExecutionException extends Exception {

  private final boolean authenticationError;
  private final boolean authorizationError;
  private final boolean superuserRequired;
  @Nullable private final AuthAdmin.Privilege requiredPrivilege;

  public ExecutionException(String message) {
    super(message);
    authenticationError = false;
    authorizationError = false;
    superuserRequired = false;
    requiredPrivilege = null;
  }

  public ExecutionException(String message, Throwable cause) {
    super(message, cause);
    authenticationError = false;
    authorizationError = false;
    superuserRequired = false;
    requiredPrivilege = null;
  }

  public ExecutionException(
      String message,
      boolean authenticationError,
      boolean authorizationError,
      boolean superuserRequired,
      @Nullable AuthAdmin.Privilege requiredPrivilege) {
    super(message);
    this.authenticationError = authenticationError;
    this.authorizationError = authorizationError;
    this.superuserRequired = superuserRequired;
    this.requiredPrivilege = requiredPrivilege;
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
