package com.scalar.db.schemaloader.util.either;

import javax.annotation.Nullable;

public abstract class Either<L, R> {
  public abstract boolean isLeft();

  public abstract boolean isRight();

  @Nullable
  public abstract L getLeft();

  @Nullable
  public abstract R getRight();
}
