package com.scalar.db.sql;

import com.scalar.db.sql.statement.Statement;

public interface SqlSession {
  ResultSet execute(Statement statement);
}
