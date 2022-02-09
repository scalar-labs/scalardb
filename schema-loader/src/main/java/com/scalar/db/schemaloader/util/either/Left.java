package com.scalar.db.schemaloader.util.either;

import javax.annotation.Nullable;

public class Left<L, R> extends Either<L, R> {
  @Nullable public final L leftValue;

  public Left(@Nullable L a) {
    leftValue = a;
  }

  @Override
  public boolean isLeft() {
    return true;
  }

  @Override
  public boolean isRight() {
    return false;
  }

  @Nullable
  @Override
  public L getLeft() {
    return leftValue;
  }

  @Nullable
  @Override
  public R getRight() {
    throw new IllegalStateException("No right");
  }
}
