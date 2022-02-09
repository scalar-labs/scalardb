package com.scalar.db.schemaloader.util.either;

import javax.annotation.Nullable;

public class Right<L, R> extends Either<L, R> {
  @Nullable public final R rightValue;

  public Right(@Nullable R b) {
    rightValue = b;
  }

  @Override
  public boolean isLeft() {
    return false;
  }

  @Override
  public boolean isRight() {
    return true;
  }

  @Nullable
  @Override
  public L getLeft() {
    throw new IllegalStateException("No left");
  }

  @Nullable
  @Override
  public R getRight() {
    return rightValue;
  }
}
