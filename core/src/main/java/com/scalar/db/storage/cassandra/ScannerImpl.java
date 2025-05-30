package com.scalar.db.storage.cassandra;

import static com.google.common.base.Preconditions.checkNotNull;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.scalar.db.api.Result;
import com.scalar.db.common.AbstractScanner;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public final class ScannerImpl extends AbstractScanner {
  private final ResultSet resultSet;
  private final ResultInterpreter resultInterpreter;

  public ScannerImpl(ResultSet resultSet, ResultInterpreter resultInterpreter) {
    this.resultSet = checkNotNull(resultSet);
    this.resultInterpreter = checkNotNull(resultInterpreter);
  }

  @Override
  @Nonnull
  public Optional<Result> one() {
    Row row = resultSet.one();
    if (row == null) {
      return Optional.empty();
    }
    return Optional.of(resultInterpreter.interpret(row));
  }

  @Override
  @Nonnull
  public List<Result> all() {
    List<Result> results = new ArrayList<>();
    resultSet.forEach(r -> results.add(resultInterpreter.interpret(r)));
    return results;
  }

  @Override
  public void close() {}
}
