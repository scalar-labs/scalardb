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

  // Temporary added until the CRUD operations for time-related types is completed
  public static DataType[] valuesWithoutTimesRelatedTypes() {
    return Arrays.stream(values())
        .filter(type -> type != DATE && type != TIME && type != TIMESTAMP && type != TIMESTAMPTZ)
        .toArray(DataType[]::new);
  }
}
