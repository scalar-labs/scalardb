package com.scalar.database.transaction.consensuscommit;

import com.scalar.database.api.TransactionState;
import com.scalar.database.io.BigIntValue;
import com.scalar.database.io.IntValue;
import com.scalar.database.io.TextValue;
import com.scalar.database.io.Value;

/** A utility class for constructing {@code Value}s to be used as {@code Attributes} */
public final class Attribute {
  public static final String ID = "tx_id";
  public static final String STATE = "tx_state";
  public static final String VERSION = "tx_version";
  public static final String PREPARED_AT = "tx_prepared_at";
  public static final String COMMITTED_AT = "tx_committed_at";
  public static final String CREATED_AT = "tx_created_at";
  public static final String METADATA = "tx_metadata";
  public static final String BEFORE_PREFIX = "before_";
  public static final String BEFORE_ID = BEFORE_PREFIX + ID;
  public static final String BEFORE_STATE = BEFORE_PREFIX + STATE;
  public static final String BEFORE_VERSION = BEFORE_PREFIX + VERSION;
  public static final String BEFORE_PREPARED_AT = BEFORE_PREFIX + PREPARED_AT;
  public static final String BEFORE_COMMITTED_AT = BEFORE_PREFIX + COMMITTED_AT;

  /**
   * Returns a {@link TextValue} with name {@code Attribute.ID} and the specified value
   *
   * @param transactionId a String representing a transaction id
   * @return a {@link TextValue} with name {@code Attribute.ID} and the specified value
   */
  public static Value toIdValue(String transactionId) {
    return new TextValue(Attribute.ID, transactionId);
  }

  /**
   * Returns an {@link IntValue} with name {@code Attribute.STATE} and value the int representation
   * of the specified {@link TransactionState}
   *
   * @param state a {@link TransactionState}
   * @return an {@link IntValue} with name {@code Attribute.ID} and value the int representation of
   *     the specified {@link TransactionState}
   */
  public static Value toStateValue(TransactionState state) {
    return new IntValue(Attribute.STATE, state.get());
  }

  /**
   * Returns an {@link IntValue} with name {@code Attribute.VERSION} and the specified value
   *
   * @param version an int representing a version
   * @return an {@link IntValue} with name {@code Attribute.VERSION} and the specified value
   */
  public static Value toVersionValue(int version) {
    return new IntValue(Attribute.VERSION, version);
  }

  /**
   * Returns a {@link BigIntValue} with name {@code Attribute.PREPARED_AT} and the specified value
   *
   * @param preparedAt a long representing a prepared at time
   * @return a {@link BigIntValue} with name {@code Attribute.PREPARED_AT} and the specified value
   */
  public static Value toPreparedAtValue(long preparedAt) {
    return new BigIntValue(Attribute.PREPARED_AT, preparedAt);
  }

  /**
   * Returns a {@link BigIntValue} with name {@code Attribute.COMMITTED_AT} and the specified value
   *
   * @param committedAt a long representing a committed at time
   * @return a {@link BigIntValue} with name {@code Attribute.COMMITTED_AT} and the specified value
   */
  public static Value toCommittedAtValue(long committedAt) {
    return new BigIntValue(Attribute.COMMITTED_AT, committedAt);
  }

  /**
   * Returns a {@link BigIntValue} with name {@code Attribute.CREATED_AT} and the specified value
   *
   * @param createdAt a long representing a created at time
   * @return a {@link BigIntValue} with name {@code Attribute.CREATED_AT} and the specified value
   */
  public static Value toCreatedAtValue(long createdAt) {
    return new BigIntValue(Attribute.CREATED_AT, createdAt);
  }

  /**
   * Returns a {@link TextValue} with name {@code Attribute.METADATA} and the specified value
   *
   * @param metadata a String representing metadata
   * @return a {@link TextValue} with name {@code Attribute.METADATA} and the specified value
   */
  public static Value toMetadataValue(String metadata) {
    return new TextValue(Attribute.METADATA, metadata);
  }

  /**
   * Returns a {@link TextValue} with name {@code Attribute.BEFORE_ID} and the specified value
   *
   * @param transactionId a String representing a transaction id
   * @return a {@link TextValue} with name {@code Attribute.BEFORE_ID} and the specified value
   */
  public static Value toBeforeIdValue(String transactionId) {
    return new TextValue(Attribute.BEFORE_ID, transactionId);
  }

  /**
   * Returns an {@link IntValue} with name {@code Attribute.BEFORE_STATE} and value the int
   * representation of the specified {@link TransactionState}
   *
   * @param state a {@link TransactionState}
   * @return an {@link IntValue} with name {@code Attribute.ID} and value the int representation of
   *     the specified {@link TransactionState}
   */
  public static Value toBeforeStateValue(TransactionState state) {
    return new IntValue(Attribute.BEFORE_STATE, state.get());
  }

  /**
   * Returns an {@link IntValue} with name {@code Attribute.BEFORE_VERSION} and the specified value
   *
   * @param version an int representing a version
   * @return an {@link IntValue} with name {@code Attribute.BEFORE_VERSION} and the specified value
   */
  public static Value toBeforeVersionValue(int version) {
    return new IntValue(Attribute.BEFORE_VERSION, version);
  }

  /**
   * Returns a {@link BigIntValue} with name {@code Attribute.BEFORE_PREPARED_AT} and the specified
   * value
   *
   * @param preparedAt a long representing a prepared at time
   * @return a {@link BigIntValue} with name {@code Attribute.BEFORE_PREPARED_AT} and the specified
   *     value
   */
  public static Value toBeforePreparedAtValue(long preparedAt) {
    return new BigIntValue(Attribute.BEFORE_PREPARED_AT, preparedAt);
  }

  /**
   * Returns a {@link BigIntValue} with name {@code Attribute.BEFORE_COMMITTED_AT} and the specified
   * value
   *
   * @param committedAt a long representing a committed at time
   * @return a {@link BigIntValue} with name {@code Attribute.BEFORE_COMMITTED_AT} and the specified
   *     value
   */
  public static Value toBeforeCommittedAtValue(long committedAt) {
    return new BigIntValue(Attribute.BEFORE_COMMITTED_AT, committedAt);
  }
}
