package com.scalar.db.sql;

import com.scalar.db.sql.statement.Statement;

public interface SqlStatementSession {
  void begin();

  void join(String transactionId);

  void resume(String transactionId);

  ResultSet execute(Statement statement);

  void prepare();

  void validate();

  void commit();

  void rollback();

  String getTransactionId();

  boolean isTransactionInProgress();

  Metadata getMetadata();
}
