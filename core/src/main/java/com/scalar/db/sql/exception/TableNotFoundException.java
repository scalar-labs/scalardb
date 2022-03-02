package com.scalar.db.sql.exception;

import com.scalar.db.util.ScalarDbUtils;

public class TableNotFoundException extends SqlException {

  public TableNotFoundException(String namespaceName, String tableName) {
    super(ScalarDbUtils.getFullTableName(namespaceName, tableName) + " is not found");
  }
}
