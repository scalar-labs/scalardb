package com.scalar.db.sql;

import com.scalar.db.sql.statement.SelectStatement;
import com.scalar.db.sql.statement.Statement;

public interface SqlSession {
  void beginTransaction();

  void joinTransaction(String transactionId);

  void execute(Statement statement);

  ResultSet getResultSet();

  ResultSet executeQuery(SelectStatement statement);

  void prepare();

  void validate();

  void commit();

  void rollback();

  String getTransactionId();

  Metadata getMetadata();
}
