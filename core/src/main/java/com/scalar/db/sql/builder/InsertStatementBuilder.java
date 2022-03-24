package com.scalar.db.sql.builder;

import com.google.common.collect.ImmutableList;
import com.scalar.db.sql.Assignment;
import com.scalar.db.sql.statement.InsertStatement;
import java.util.List;

public final class InsertStatementBuilder {

  private InsertStatementBuilder() {}

  public static class Start {
    private final String namespaceName;
    private final String tableName;

    Start(String namespaceName, String tableName) {
      this.namespaceName = namespaceName;
      this.tableName = tableName;
    }

    public Buildable values(Assignment... assignments) {
      return new Buildable(namespaceName, tableName, ImmutableList.copyOf(assignments));
    }

    public Buildable values(List<Assignment> assignments) {
      return new Buildable(namespaceName, tableName, ImmutableList.copyOf(assignments));
    }
  }

  public static class Buildable {
    private final String namespaceName;
    private final String tableName;
    private final ImmutableList<Assignment> assignments;

    private Buildable(
        String namespaceName, String tableName, ImmutableList<Assignment> assignments) {
      this.namespaceName = namespaceName;
      this.tableName = tableName;
      this.assignments = assignments;
    }

    public InsertStatement build() {
      return InsertStatement.of(namespaceName, tableName, assignments);
    }
  }
}
