package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.api.TransactionState;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.TextValue;

public final class Attribute {
  public static final String ID = "tx_id";
  public static final String CHILD_IDS = "tx_child_ids";
  public static final String STATE = "tx_state";
  public static final String VERSION = "tx_version";
  public static final String PREPARED_AT = "tx_prepared_at";
  public static final String COMMITTED_AT = "tx_committed_at";
  public static final String CREATED_AT = "tx_created_at";
  public static final String BEFORE_PREFIX = "before_";
  public static final String BEFORE_ID = BEFORE_PREFIX + ID;
  public static final String BEFORE_STATE = BEFORE_PREFIX + STATE;
  public static final String BEFORE_VERSION = BEFORE_PREFIX + VERSION;
  public static final String BEFORE_PREPARED_AT = BEFORE_PREFIX + PREPARED_AT;
  public static final String BEFORE_COMMITTED_AT = BEFORE_PREFIX + COMMITTED_AT;

  public static TextValue toIdValue(String transactionId) {
    return new TextValue(Attribute.ID, transactionId);
  }

  public static TextValue toChildIdsValue(String childTransactionIds) {
    return new TextValue(Attribute.CHILD_IDS, childTransactionIds);
  }

  public static IntValue toStateValue(TransactionState state) {
    return new IntValue(Attribute.STATE, state.get());
  }

  public static IntValue toVersionValue(int version) {
    return new IntValue(Attribute.VERSION, version);
  }

  public static BigIntValue toPreparedAtValue(long preparedAt) {
    return new BigIntValue(Attribute.PREPARED_AT, preparedAt);
  }

  public static BigIntValue toCommittedAtValue(long committedAt) {
    return new BigIntValue(Attribute.COMMITTED_AT, committedAt);
  }

  public static BigIntValue toCreatedAtValue(long createdAt) {
    return new BigIntValue(Attribute.CREATED_AT, createdAt);
  }

  public static TextValue toBeforeIdValue(String transactionId) {
    return new TextValue(Attribute.BEFORE_ID, transactionId);
  }

  public static IntValue toBeforeStateValue(TransactionState state) {
    return new IntValue(Attribute.BEFORE_STATE, state.get());
  }

  public static IntValue toBeforeVersionValue(int version) {
    return new IntValue(Attribute.BEFORE_VERSION, version);
  }

  public static BigIntValue toBeforePreparedAtValue(long preparedAt) {
    return new BigIntValue(Attribute.BEFORE_PREPARED_AT, preparedAt);
  }

  public static BigIntValue toBeforeCommittedAtValue(long committedAt) {
    return new BigIntValue(Attribute.BEFORE_COMMITTED_AT, committedAt);
  }
}
