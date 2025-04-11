package com.scalar.db.storage.cosmos;

import com.google.common.collect.ImmutableList;
import com.scalar.db.api.Result;
import com.scalar.db.common.AbstractScanner;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public final class SingleRecordScanner extends AbstractScanner {

  @Nullable private Record record;
  private final ResultInterpreter resultInterpreter;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public SingleRecordScanner(@Nullable Record record, ResultInterpreter resultInterpreter) {
    this.record = record;
    this.resultInterpreter = resultInterpreter;
  }

  @Override
  @Nonnull
  public Optional<Result> one() {
    if (record != null) {
      Optional<Result> ret = Optional.of(resultInterpreter.interpret(record));
      record = null;
      return ret;
    } else {
      return Optional.empty();
    }
  }

  @Override
  @Nonnull
  public List<Result> all() {
    if (record != null) {
      List<Result> ret = ImmutableList.of(resultInterpreter.interpret(record));
      record = null;
      return ret;
    } else {
      return Collections.emptyList();
    }
  }

  @Override
  public void close() {}
}
