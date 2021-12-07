package com.scalar.db.storage.dynamo;

import com.scalar.db.io.DoubleValue;
import java.util.Random;

public final class DynamoTestUtils {

  public static final double MAX_DYNAMO_DOUBLE_VALUE = 9.99999999999999E125D;
  public static final double MIN_DYNAMO_DOUBLE_VALUE = -9.99999999999999E125D;

  private DynamoTestUtils() {}

  public static DoubleValue getRandomDynamoDoubleValue(Random random, String columnName) {
    return new DoubleValue(columnName, nextDynamoDouble(random));
  }

  public static double nextDynamoDouble(Random random) {
    return random
        .doubles(MIN_DYNAMO_DOUBLE_VALUE, MAX_DYNAMO_DOUBLE_VALUE)
        .limit(1)
        .findFirst()
        .orElse(0.0d);
  }

  public static DoubleValue getMinDynamoDoubleValue(String columnName) {
    return new DoubleValue(columnName, MIN_DYNAMO_DOUBLE_VALUE);
  }

  public static DoubleValue getMaxDynamoDoubleValue(String columnName) {
    return new DoubleValue(columnName, MAX_DYNAMO_DOUBLE_VALUE);
  }
}
