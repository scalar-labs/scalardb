package com.scalar.db.storage.dynamo;

import com.scalar.db.io.DoubleColumn;
import java.util.Random;

public final class DynamoTestUtils {

  public static final double MAX_DYNAMO_DOUBLE_VALUE = 9.99999999999999E125D;
  public static final double MIN_DYNAMO_DOUBLE_VALUE = -9.99999999999999E125D;

  private DynamoTestUtils() {}

  public static DoubleColumn getRandomDynamoDoubleValue(Random random, String columnName) {
    return DoubleColumn.of(columnName, nextDynamoDouble(random));
  }

  public static DoubleColumn getRandomDynamoDoubleColumn(Random random, String columnName) {
    return DoubleColumn.of(columnName, nextDynamoDouble(random));
  }

  public static double nextDynamoDouble(Random random) {
    return random
        .doubles(1, MIN_DYNAMO_DOUBLE_VALUE, MAX_DYNAMO_DOUBLE_VALUE)
        .findFirst()
        .orElse(0.0d);
  }

  public static DoubleColumn getMinDynamoDoubleValue(String columnName) {
    return DoubleColumn.of(columnName, MIN_DYNAMO_DOUBLE_VALUE);
  }

  public static DoubleColumn getMaxDynamoDoubleValue(String columnName) {
    return DoubleColumn.of(columnName, MAX_DYNAMO_DOUBLE_VALUE);
  }
}
