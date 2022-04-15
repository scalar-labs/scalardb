package com.scalar.db.sql;

import com.scalar.db.sql.metadata.Metadata;
import com.scalar.db.sql.statement.BindableStatement;
import com.scalar.db.sql.statement.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public interface SqlStatementSession {
  void begin();

  void join(String transactionId);

  void resume(String transactionId);

  ResultSet execute(Statement statement);

  default ResultSet execute(BindableStatement<?> statement, Value... positionalValues) {
    return execute(statement, Arrays.asList(positionalValues));
  }

  default ResultSet execute(BindableStatement<?> bindableStatement, List<Value> positionalValues) {
    return execute(bindableStatement.bind(positionalValues));
  }

  default ResultSet execute(
      BindableStatement<?> bindableStatement, Map<String, Value> namedValues) {
    return execute(bindableStatement.bind(namedValues));
  }

  void prepare();

  void validate();

  void commit();

  void rollback();

  String getTransactionId();

  boolean isTransactionInProgress();

  Metadata getMetadata();
}
