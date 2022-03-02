package com.scalar.db.sql.builder;

import com.google.common.collect.ImmutableList;
import com.scalar.db.sql.statement.BatchStatement;
import com.scalar.db.sql.statement.BatchableStatement;

public class BatchBuilder {

  private final ImmutableList.Builder<BatchableStatement> statementsBuilder;

  BatchBuilder() {
    statementsBuilder = ImmutableList.builder();
  }

  public BatchBuilder addStatement(BatchableStatement statement) {
    statementsBuilder.add(statement);
    return this;
  }

  public BatchStatement build() {
    return new BatchStatement(statementsBuilder.build());
  }
}
