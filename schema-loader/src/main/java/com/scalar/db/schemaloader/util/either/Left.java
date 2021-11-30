package com.scalar.db.schemaloader.util.either;

import javax.annotation.Nullable;

public class Left<A, B> extends Either<A, B> {
  @Nullable public A leftValue;

  public Left(@Nullable A a) {
    leftValue = a;
  }

  public A getValue() {
    return leftValue;
  }
}
