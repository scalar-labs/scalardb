package com.scalar.db.schemaloader.util.either;

import javax.annotation.Nullable;

public class Right<A, B> extends Either<A, B> {
  @Nullable public B rightValue;

  public Right(@Nullable B b) {
    rightValue = b;
  }

  public B getValue() {
    return rightValue;
  }
}
