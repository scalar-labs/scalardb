package com.scalar.db.transaction.consensuscommit.replication.server;

public class LogApplier {
  // TODO: Make these configurable
  private static final String REPLICATION_DB_NAMESPACE = "replication";
  private static final String REPLICATION_DB_TABLE = "transactions";
  private static final int REPLICATION_DB_PARTITION_SIZE = 256;
  private static final int REPLICATION_DB_THREAD_SIZE = 8;
}
