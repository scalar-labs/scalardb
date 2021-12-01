package com.scalar.db.schemaloader.util.either;

public abstract class Either<L, R> {
  public abstract boolean isLeft();

  public abstract boolean isRight();

  public abstract L getLeft();

  public abstract R getRight();
}
