package com.scalar.db.common;

/**
 * Option keys that ScalarDB sets on its own {@code createTable} calls. They are internal and not
 * for users: their meaning can change without notice.
 */
public final class InternalTableOptions {

  /**
   * The name of a table in the same namespace whose TEXT key columns the new table copies the
   * collation from. Only the JDBC admin reads it.
   */
  public static final String KEY_COLLATION_SOURCE_TABLE =
      "scalardb.internal.key_collation_source_table";

  private InternalTableOptions() {}
}
