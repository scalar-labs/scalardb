package com.scalar.db.storage.jdbc;

import com.google.api.gax.core.CredentialsProvider;
import com.google.auth.Credentials;
import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.common.CoreError;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Exposes Spanner service-account credentials to the Google Cloud Spanner JDBC driver via the
 * {@code credentialsProvider} connection property.
 *
 * <p>The Spanner JDBC driver instantiates this class reflectively through its no-arg constructor,
 * so credentials cannot be passed through the constructor. They are registered statically by {@link
 * RdbEngineSpanner#setConnectionCredentials} before the connection pool is initialized. Only one
 * set of Spanner credentials per JVM is supported; attempting to register a different credential
 * fails fast rather than silently overwriting the existing one.
 */
public class SpannerCredentialsProvider implements CredentialsProvider {

  private static final AtomicReference<Credentials> CREDENTIALS = new AtomicReference<>();

  static void register(Credentials credentials) {
    Objects.requireNonNull(credentials);
    Credentials existing = CREDENTIALS.get();
    if (existing == null) {
      if (!CREDENTIALS.compareAndSet(null, credentials)) {
        // Lost the race — fall through to the equality check against the winner
        existing = CREDENTIALS.get();
      } else {
        return;
      }
    }
    if (!existing.equals(credentials)) {
      throw new IllegalStateException(
          CoreError.JDBC_SPANNER_CREDENTIALS_ALREADY_REGISTERED.buildMessage());
    }
  }

  @VisibleForTesting
  static void clear() {
    CREDENTIALS.set(null);
  }

  /** Required by the Spanner JDBC driver, which instantiates this class reflectively. */
  public SpannerCredentialsProvider() {}

  @Override
  public Credentials getCredentials() {
    Credentials credentials = CREDENTIALS.get();
    if (credentials == null) {
      throw new IllegalStateException(
          "Spanner credentials have not been registered. "
              + SpannerCredentialsProvider.class.getName()
              + " is intended to be used only by ScalarDB's Spanner JDBC adapter");
    }
    return credentials;
  }
}
