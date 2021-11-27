package com.scalar.db.storage;

import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;
import org.apache.commons.lang3.RandomStringUtils;

public final class TestUtils {

  public static final int MAX_TEXT_COUNT = 20;
  public static final int MAX_BLOB_LENGTH = 20;

  private TestUtils() {}

  public static Value<?> getRandomValue(Random random, String columnName, DataType dataType) {
    return getRandomValue(random, columnName, dataType, false);
  }

  public static Value<?> getRandomValue(
      Random random, String columnName, DataType dataType, boolean allowEmpty) {
    switch (dataType) {
      case BIGINT:
        return new BigIntValue(columnName, nextBigInt(random));
      case INT:
        return new IntValue(columnName, random.nextInt());
      case FLOAT:
        return new FloatValue(columnName, nextFloat(random));
      case DOUBLE:
        return new DoubleValue(columnName, nextDouble(random));
      case BLOB:
        int length =
            allowEmpty ? random.nextInt(MAX_BLOB_LENGTH) : random.nextInt(MAX_BLOB_LENGTH - 1) + 1;
        byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        return new BlobValue(columnName, bytes);
      case TEXT:
        int count =
            allowEmpty ? random.nextInt(MAX_TEXT_COUNT) : random.nextInt(MAX_TEXT_COUNT - 1) + 1;
        return new TextValue(
            columnName, RandomStringUtils.random(count, 0, 0, true, true, null, random));
      case BOOLEAN:
        return new BooleanValue(columnName, random.nextBoolean());
      default:
        throw new AssertionError();
    }
  }

  public static long nextBigInt(Random random) {
    return random
        .longs(BigIntValue.MIN_VALUE, (BigIntValue.MAX_VALUE + 1))
        .limit(1)
        .findFirst()
        .orElse(0);
  }

  public static float nextFloat(Random random) {
    return (float)
        random.doubles(Float.MIN_VALUE, Float.MAX_VALUE).limit(1).findFirst().orElse(0.0d);
  }

  public static double nextDouble(Random random) {
    return random.doubles(Double.MIN_VALUE, Double.MAX_VALUE).limit(1).findFirst().orElse(0.0d);
  }

  public static Value<?> getMinValue(String columnName, DataType dataType) {
    return getMinValue(columnName, dataType, false);
  }

  public static Value<?> getMinValue(String columnName, DataType dataType, boolean allowEmpty) {
    switch (dataType) {
      case BIGINT:
        return new BigIntValue(columnName, BigIntValue.MIN_VALUE);
      case INT:
        return new IntValue(columnName, Integer.MIN_VALUE);
      case FLOAT:
        return new FloatValue(columnName, Float.MIN_VALUE);
      case DOUBLE:
        return new DoubleValue(columnName, Double.MIN_VALUE);
      case BLOB:
        return new BlobValue(columnName, allowEmpty ? new byte[0] : new byte[] {0x00});
      case TEXT:
        return new TextValue(columnName, allowEmpty ? "" : "\u0001");
      case BOOLEAN:
        return new BooleanValue(columnName, false);
      default:
        throw new AssertionError();
    }
  }

  public static Value<?> getMaxValue(String columnName, DataType dataType) {
    switch (dataType) {
      case BIGINT:
        return new BigIntValue(columnName, BigIntValue.MAX_VALUE);
      case INT:
        return new IntValue(columnName, Integer.MAX_VALUE);
      case FLOAT:
        return new FloatValue(columnName, Float.MAX_VALUE);
      case DOUBLE:
        return new DoubleValue(columnName, Double.MAX_VALUE);
      case BLOB:
        byte[] blobBytes = new byte[MAX_BLOB_LENGTH];
        Arrays.fill(blobBytes, (byte) 0xff);
        return new BlobValue(columnName, blobBytes);
      case TEXT:
        StringBuilder builder = new StringBuilder();
        IntStream.range(0, MAX_TEXT_COUNT).forEach(i -> builder.append(Character.MAX_VALUE));
        return new TextValue(columnName, builder.toString());
      case BOOLEAN:
        return new BooleanValue(columnName, true);
      default:
        throw new AssertionError();
    }
  }

  public static List<BooleanValue> booleanValues(String columnName) {
    return Arrays.asList(new BooleanValue(columnName, false), new BooleanValue(columnName, true));
  }

  public static Value<?> getNullValue(String columnName, DataType dataType) {
    switch (dataType) {
      case BLOB:
        return new BlobValue(columnName, null); // null value
      case TEXT:
        return new TextValue(columnName, (String) null); // null value
      default:
        throw new AssertionError();
    }
  }
}
