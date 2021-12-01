package com.scalar.db.schemaloader.util.either;

import javax.annotation.Nullable;

public class Right<L, R> extends Either<L, R> {
  @Nullable public R rightValue;

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

  @Override
  public L getLeft() {
    throw new IllegalStateException("No left");
  }

  @Override
  public R getRight() {
    return rightValue;
  }
}
