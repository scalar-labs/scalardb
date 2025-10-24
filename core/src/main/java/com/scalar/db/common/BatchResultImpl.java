package com.scalar.db.common;

import com.scalar.db.api.CrudOperable;
import com.scalar.db.api.Result;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;

public class BatchResultImpl implements CrudOperable.BatchResult {

  public static final BatchResultImpl PUT_BATCH_RESULT = new BatchResultImpl(Type.PUT);
  public static final BatchResultImpl INSERT_BATCH_RESULT = new BatchResultImpl(Type.INSERT);
  public static final BatchResultImpl UPDATE_BATCH_RESULT = new BatchResultImpl(Type.UPDATE);
  public static final BatchResultImpl UPSERT_BATCH_RESULT = new BatchResultImpl(Type.UPSERT);
  public static final BatchResultImpl DELETE_BATCH_RESULT = new BatchResultImpl(Type.DELETE);

  private final Type type;

  @Nullable private final Optional<Result> getResult;

  @Nullable private final List<Result> scanResult;

  private BatchResultImpl(Type type) {
    this.type = type;
    this.getResult = null;
    this.scanResult = null;
  }

  public BatchResultImpl(Optional<Result> getResult) {
    this.type = Type.GET;
    this.getResult = Objects.requireNonNull(getResult);
    this.scanResult = null;
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public BatchResultImpl(List<Result> scanResult) {
    this.type = Type.SCAN;
    this.getResult = null;
    this.scanResult = Objects.requireNonNull(scanResult);
  }

  @Override
  public Type getType() {
    return type;
  }

  @Override
  public Optional<Result> getGetResult() {
    if (getResult == null) {
      throw new IllegalStateException(
          CoreError.BATCH_RESULT_DOES_NOT_HAVE_GET_RESULT.buildMessage());
    }
    return getResult;
  }

  @SuppressFBWarnings("EI_EXPOSE_REP")
  @Override
  public List<Result> getScanResult() {
    if (scanResult == null) {
      throw new IllegalStateException(
          CoreError.BATCH_RESULT_DOES_NOT_HAVE_SCAN_RESULT.buildMessage());
    }
    return scanResult;
  }
}
