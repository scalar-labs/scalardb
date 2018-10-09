package com.scalar.database.transaction.consensuscommit;

import com.scalar.database.api.TransactionState;
import com.scalar.database.io.BigIntValue;
import com.scalar.database.io.IntValue;
import com.scalar.database.io.TextValue;
import com.scalar.database.io.Value;

/** */
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

  public static Value toIdValue(String transactionId) {
    return new TextValue(Attribute.ID, transactionId);
  }

  public static Value toStateValue(TransactionState state) {
    return new IntValue(Attribute.STATE, state.get());
  }

  public static Value toVersionValue(int version) {
    return new IntValue(Attribute.VERSION, version);
  }

  public static Value toPreparedAtValue(long preparedAt) {
    return new BigIntValue(Attribute.PREPARED_AT, preparedAt);
  }

  public static Value toCommittedAtValue(long committedAt) {
    return new BigIntValue(Attribute.COMMITTED_AT, committedAt);
  }

  public static Value toCreatedAtValue(long createdAt) {
    return new BigIntValue(Attribute.CREATED_AT, createdAt);
  }

  public static Value toMetadataValue(String metadata) {
    return new TextValue(Attribute.METADATA, metadata);
  }

  public static Value toBeforeIdValue(String transactionId) {
    return new TextValue(Attribute.BEFORE_ID, transactionId);
  }

  public static Value toBeforeStateValue(TransactionState state) {
    return new IntValue(Attribute.BEFORE_STATE, state.get());
  }

  public static Value toBeforeVersionValue(int version) {
    return new IntValue(Attribute.BEFORE_VERSION, version);
  }

  public static Value toBeforePreparedAtValue(long preparedAt) {
    return new BigIntValue(Attribute.BEFORE_PREPARED_AT, preparedAt);
  }

  public static Value toBeforeCommittedAtValue(long committedAt) {
    return new BigIntValue(Attribute.BEFORE_COMMITTED_AT, committedAt);
  }
}
