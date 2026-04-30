package com.scalar.db.storage.jdbc;

import com.google.api.gax.core.CredentialsProvider;
import com.google.auth.Credentials;
import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.common.CoreError;
import java.security.MessageDigest;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * Exposes Spanner service-account credentials to the Google Cloud Spanner JDBC driver via the
 * {@code credentialsProvider} connection property.
 *
 * <p>The Spanner JDBC driver instantiates the configured class reflectively through its no-arg
 * constructor, so credentials cannot be passed through the constructor. They are registered
 * statically by {@link RdbEngineSpanner#setConnectionCredentials} before the connection pool is
 * initialized. To allow the same JVM to use multiple Spanner data sources with different
 * credentials, this class declares {@value #MAX_CREDENTIALS} concrete slot subclasses; each
 * registered credential is bound to its own slot subclass and Hikari is pointed at that subclass's
 * fully qualified name.
 */
public abstract class SpannerCredentialsProvider implements CredentialsProvider {

  static final int MAX_CREDENTIALS = 5;

  private static final AtomicReference<Slot>[] SLOTS = newSlotArray();

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static AtomicReference<Slot>[] newSlotArray() {
    AtomicReference[] slots = new AtomicReference[MAX_CREDENTIALS];
    for (int i = 0; i < MAX_CREDENTIALS; i++) {
      slots[i] = new AtomicReference<>();
    }
    return (AtomicReference<Slot>[]) slots;
  }

  /**
   * Claims (or reuses) a slot for the given credentials and returns the fully qualified class name
   * of the slot subclass that should be used as the {@code credentialsProvider} connection property
   * value.
   *
   * <p>If a previously registered slot has the same {@code identityKey}, that slot's class name is
   * returned (idempotent). Otherwise the first empty slot is claimed.
   *
   * @param credentials the parsed credentials object handed back to the JDBC driver
   * @param identityKey caller-supplied bytes that uniquely identify these credentials (the raw
   *     service-account JSON).
   * @throws IllegalStateException when all {@value #MAX_CREDENTIALS} slots are occupied with
   *     distinct identity keys.
   */
  static String register(Credentials credentials, byte[] identityKey) {
    Objects.requireNonNull(credentials);
    Objects.requireNonNull(identityKey);
    // Reuse the slot if these exact identity bytes are already registered.
    for (int i = 0; i < MAX_CREDENTIALS; i++) {
      Slot existing = SLOTS[i].get();
      if (existing != null && MessageDigest.isEqual(existing.identityKey, identityKey)) {
        return slotClassName(i);
      }
    }
    // Otherwise claim the first empty slot.
    Slot newSlot = new Slot(credentials, identityKey.clone());
    for (int i = 0; i < MAX_CREDENTIALS; i++) {
      if (SLOTS[i].compareAndSet(null, newSlot)) {
        return slotClassName(i);
      }
      // Lost the compare-and-swap race — if the winner has matching identity bytes, reuse this
      // slot.
      Slot existing = SLOTS[i].get();
      if (existing != null && MessageDigest.isEqual(existing.identityKey, identityKey)) {
        return slotClassName(i);
      }
    }
    throw new IllegalStateException(
        CoreError.JDBC_SPANNER_CREDENTIALS_LIMIT_EXCEEDED.buildMessage(MAX_CREDENTIALS));
  }

  @VisibleForTesting
  static void clear() {
    for (AtomicReference<Slot> slot : SLOTS) {
      slot.set(null);
    }
  }

  private static String slotClassName(int index) {
    switch (index) {
      case 0:
        return Slot0.class.getName();
      case 1:
        return Slot1.class.getName();
      case 2:
        return Slot2.class.getName();
      case 3:
        return Slot3.class.getName();
      case 4:
        return Slot4.class.getName();
      default:
        throw new IllegalStateException("Unexpected slot index: " + index);
    }
  }

  abstract int slot();

  @Override
  public Credentials getCredentials() {
    @Nullable Slot s = SLOTS[slot()].get();
    if (s == null) {
      throw new IllegalStateException(
          "Spanner credentials have not been registered. "
              + getClass().getName()
              + " is intended to be used only by ScalarDB's Spanner JDBC adapter");
    }
    return s.credentials;
  }

  private static final class Slot {
    final Credentials credentials;
    final byte[] identityKey;

    Slot(Credentials credentials, byte[] identityKey) {
      this.credentials = credentials;
      this.identityKey = identityKey;
    }
  }

  /** Required by the Spanner JDBC driver, which instantiates this class reflectively. */
  public static final class Slot0 extends SpannerCredentialsProvider {
    public Slot0() {}

    @Override
    int slot() {
      return 0;
    }
  }

  /** Required by the Spanner JDBC driver, which instantiates this class reflectively. */
  public static final class Slot1 extends SpannerCredentialsProvider {
    public Slot1() {}

    @Override
    int slot() {
      return 1;
    }
  }

  /** Required by the Spanner JDBC driver, which instantiates this class reflectively. */
  public static final class Slot2 extends SpannerCredentialsProvider {
    public Slot2() {}

    @Override
    int slot() {
      return 2;
    }
  }

  /** Required by the Spanner JDBC driver, which instantiates this class reflectively. */
  public static final class Slot3 extends SpannerCredentialsProvider {
    public Slot3() {}

    @Override
    int slot() {
      return 3;
    }
  }

  /** Required by the Spanner JDBC driver, which instantiates this class reflectively. */
  public static final class Slot4 extends SpannerCredentialsProvider {
    public Slot4() {}

    @Override
    int slot() {
      return 4;
    }
  }
}
