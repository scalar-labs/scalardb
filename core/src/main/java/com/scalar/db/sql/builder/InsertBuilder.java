package com.scalar.db.sql.builder;

import com.scalar.db.sql.Assignment;
import com.scalar.db.sql.statement.InsertStatement;
import java.util.Arrays;
import java.util.List;

public final class InsertBuilder {

  private InsertBuilder() {}

  public static class Start {
    private final String namespaceName;
    private final String tableName;

    Start(String namespaceName, String tableName) {
      this.namespaceName = namespaceName;
      this.tableName = tableName;
    }

    public End values(Assignment... assignments) {
      return new End(namespaceName, tableName, Arrays.asList(assignments));
    }
  }

  public static class End {
    private final String namespaceName;
    private final String tableName;
    private final List<Assignment> assignments;
    private boolean ifNotExists;

    public End(String namespaceName, String tableName, List<Assignment> assignments) {
      this.namespaceName = namespaceName;
      this.tableName = tableName;
      this.assignments = assignments;
    }

    public End ifNotExists() {
      ifNotExists = true;
      return this;
    }

    public InsertStatement build() {
      return new InsertStatement(namespaceName, tableName, assignments, ifNotExists);
    }
  }
}
