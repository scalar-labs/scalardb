package com.scalar.db.transaction.consensuscommit;

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
}
