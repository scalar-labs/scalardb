package com.scalar.db.io;

import java.util.Arrays;

public enum DataType {
  BOOLEAN,
  INT,
  BIGINT,
  FLOAT,
  DOUBLE,
  TEXT,
  BLOB,
  DATE,
  TIME,
  TIMESTAMP,
  TIMESTAMPTZ;
  // TODO remove this
  public static DataType[] valuesWithoutTimesRelatedTypes() {
    return Arrays.stream(values())
        .filter(type -> type != DATE && type != TIME && type != TIMESTAMP && type != TIMESTAMPTZ)
        .toArray(DataType[]::new);
  }
}
