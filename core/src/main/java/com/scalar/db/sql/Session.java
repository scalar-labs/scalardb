package com.scalar.db.sql;

import com.scalar.db.sql.statement.Statement;

public interface Session {
  ResultSet execute(Statement statement);
}
