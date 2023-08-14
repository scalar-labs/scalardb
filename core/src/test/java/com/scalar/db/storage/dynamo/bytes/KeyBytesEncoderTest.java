package com.scalar.db.storage.dynamo.bytes;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.primitives.UnsignedBytes;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class KeyBytesEncoderTest {

  private static final String COL1 = "c1";
  private static final String COL2 = "c2";
  private static final String COL3 = "c3";

  private static final int TEXT_MAX_COUNT = 100;
  private static final int BLOB_MAX_LENGTH = 100;

  private static final int ATTEMPT_COUNT = 50;
  private static final int KEY_ELEMENT_COUNT = 30;
  private static final Random random = new Random();
  private static final ImmutableList<Order> ORDERS = ImmutableList.of(Order.ASC, Order.DESC);
  private static final ImmutableList<DataType> KEY_TYPES =
      ImmutableList.of(
          DataType.BOOLEAN,
          DataType.INT,
          DataType.BIGINT,
          DataType.FLOAT,
          DataType.DOUBLE,
          DataType.TEXT,
          DataType.BLOB);
  private static long seed;

  @BeforeAll
  public static void beforeAll() {
    seed = System.currentTimeMillis();
    System.out.println("The seed used in KeyBytesEncoderTest is " + seed);
  }

  @Test
  public void encode_SingleKeysGiven_ShouldEncodeProperlyWithPreservingSortOrder() {
    random.setSeed(seed);
    runTest(
        () -> {
          for (DataType col1Type : KEY_TYPES) {
            for (Order col1Order : ORDERS) {
              encode_SingleKeysGiven_ShouldEncodeProperlyWithPreservingSortOrder(
                  col1Type, col1Order);
            }
          }
        });
  }

  private void encode_SingleKeysGiven_ShouldEncodeProperlyWithPreservingSortOrder(
      DataType col1Type, Order col1Order) {
    // Arrange

    // Add min and max values and random values
    List<Key> target = new ArrayList<>(KEY_ELEMENT_COUNT);
    target.add(new Key(getMinValue(COL1, col1Type)));
    target.add(new Key(getMaxValue(COL1, col1Type)));
    for (int i = 0; i < KEY_ELEMENT_COUNT - 2; i++) {
      target.add(new Key(getRandomValue(COL1, col1Type, col1Order)));
    }

    Map<String, Order> keyOrders = new HashMap<>();
    keyOrders.put(COL1, col1Order);

    // Act
    List<Key> actual = sortWithKeyBytesEncoder(target, keyOrders);
    List<Key> expected =
        target.stream().sorted(getComparator(keyOrders)).collect(Collectors.toList());

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void encode_DoubleKeysGiven_ShouldEncodeProperlyWithPreservingSortOrder() {
    random.setSeed(seed);
    runTest(
        () -> {
          for (DataType col1Type : KEY_TYPES) {
            // the BLOB type is supported only for the last key
            if (col1Type == DataType.BLOB) {
              continue;
            }
            for (DataType col2Type : KEY_TYPES) {
              for (Order col1Order : ORDERS) {
                for (Order col2Order : ORDERS) {
                  encode_DoubleKeysGiven_ShouldEncodeProperlyWithPreservingSortOrder(
                      col1Type, col2Type, col1Order, col2Order);
                }
              }
            }
          }
        });
  }

  private void encode_DoubleKeysGiven_ShouldEncodeProperlyWithPreservingSortOrder(
      DataType col1Type, DataType col2Type, Order col1Order, Order col2Order) {
    // Arrange

    // Add min and max values and random values
    List<Key> target = new ArrayList<>(KEY_ELEMENT_COUNT);
    target.add(new Key(getMinValue(COL1, col1Type), getMinValue(COL2, col2Type)));
    target.add(new Key(getMaxValue(COL1, col1Type), getMaxValue(COL2, col2Type)));
    for (int i = 0; i < KEY_ELEMENT_COUNT - 2; i++) {
      target.add(
          new Key(
              getRandomValue(COL1, col1Type, col1Order),
              getRandomValue(COL2, col2Type, col2Order)));
    }

    Map<String, Order> keyOrders = new HashMap<>();
    keyOrders.put(COL1, col1Order);
    keyOrders.put(COL2, col2Order);

    // Act
    List<Key> actual = sortWithKeyBytesEncoder(target, keyOrders);
    List<Key> expected =
        target.stream().sorted(getComparator(keyOrders)).collect(Collectors.toList());

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void encode_TripleKeysGiven_ShouldEncodeProperlyWithPreservingSortOrder() {
    random.setSeed(seed);
    runTest(
        () -> {
          for (DataType col1Type : KEY_TYPES) {
            // the BLOB type is supported only for the last key
            if (col1Type == DataType.BLOB) {
              continue;
            }
            for (DataType col2Type : KEY_TYPES) {
              // the BLOB type is supported only for the last key
              if (col2Type == DataType.BLOB) {
                continue;
              }
              for (DataType col3Type : KEY_TYPES) {
                for (Order col1Order : ORDERS) {
                  for (Order col2Order : ORDERS) {
                    for (Order col3Order : ORDERS) {
                      encode_TripleKeysGiven_ShouldEncodeProperlyWithPreservingSortOrder(
                          col1Type, col2Type, col3Type, col1Order, col2Order, col3Order);
                    }
                  }
                }
              }
            }
          }
        });
  }

  private void encode_TripleKeysGiven_ShouldEncodeProperlyWithPreservingSortOrder(
      DataType col1Type,
      DataType col2Type,
      DataType col3Type,
      Order col1Order,
      Order col2Order,
      Order col3Order) {
    // Arrange

    // Add min and max values and random values
    List<Key> target = new ArrayList<>(KEY_ELEMENT_COUNT);
    target.add(
        new Key(
            getMinValue(COL1, col1Type), getMinValue(COL2, col2Type), getMinValue(COL3, col3Type)));
    target.add(
        new Key(
            getMaxValue(COL1, col1Type), getMaxValue(COL2, col2Type), getMaxValue(COL3, col3Type)));
    for (int i = 0; i < KEY_ELEMENT_COUNT - 2; i++) {
      target.add(
          new Key(
              getRandomValue(COL1, col1Type, col1Order),
              getRandomValue(COL2, col2Type, col2Order),
              getRandomValue(COL3, col3Type, col3Order)));
    }

    Map<String, Order> keyOrders = new HashMap<>();
    keyOrders.put(COL1, col1Order);
    keyOrders.put(COL2, col2Order);
    keyOrders.put(COL3, col3Order);

    // Act
    List<Key> actual = sortWithKeyBytesEncoder(target, keyOrders);
    List<Key> expected =
        target.stream().sorted(getComparator(keyOrders)).collect(Collectors.toList());

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  private static void runTest(Runnable test) {
    IntStream.range(0, ATTEMPT_COUNT).forEach(i -> test.run());
  }

  private Value<?> getMinValue(String columnName, DataType dataType) {
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
        return new BlobValue(columnName, new byte[0]);
      case TEXT:
        return new TextValue(columnName, "");
      case BOOLEAN:
        return new BooleanValue(columnName, false);
      default:
        throw new AssertionError();
    }
  }

  private Value<?> getMaxValue(String columnName, DataType dataType) {
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
        byte[] blobBytes = new byte[BLOB_MAX_LENGTH];
        Arrays.fill(blobBytes, (byte) 0xff);
        return new BlobValue(columnName, blobBytes);
      case TEXT:
        byte[] textBytes = new byte[TEXT_MAX_COUNT];
        Arrays.fill(textBytes, (byte) 0xff);
        return new TextValue(columnName, new String(textBytes, StandardCharsets.UTF_8));
      case BOOLEAN:
        return new BooleanValue(columnName, true);
      default:
        throw new AssertionError();
    }
  }

  private static Value<?> getRandomValue(String columnName, DataType dataType, Order order) {
    switch (dataType) {
      case BIGINT:
        return new BigIntValue(columnName, nextBigInt());
      case INT:
        return new IntValue(columnName, random.nextInt());
      case FLOAT:
        return new FloatValue(columnName, nextFloat());
      case DOUBLE:
        return new DoubleValue(columnName, nextDouble());
      case BLOB:
        int length = random.nextInt(BLOB_MAX_LENGTH);
        byte[] bytes = new byte[length];
        random.nextBytes(bytes);

        // 0x00 bytes not accepted in blob values in DESC order
        if (order == Order.DESC) {
          for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] == 0x00) {
              bytes[i]++;
            }
          }
        }

        return new BlobValue(columnName, bytes);
      case TEXT:
        int count = random.nextInt(TEXT_MAX_COUNT);
        return new TextValue(
            columnName, RandomStringUtils.random(count, 0, 0, true, true, null, random));
      case BOOLEAN:
        return new BooleanValue(columnName, random.nextBoolean());
      default:
        throw new AssertionError();
    }
  }

  private static long nextBigInt() {
    OptionalLong randomLong =
        random.longs(BigIntValue.MIN_VALUE, (BigIntValue.MAX_VALUE + 1)).limit(1).findFirst();
    return randomLong.orElse(0);
  }

  private static float nextFloat() {
    OptionalDouble randomDouble =
        random.doubles(Float.MIN_VALUE, Float.MAX_VALUE).limit(1).findFirst();
    return (float) randomDouble.orElse(0.0d);
  }

  private static double nextDouble() {
    OptionalDouble randomDouble =
        random.doubles(Double.MIN_VALUE, Double.MAX_VALUE).limit(1).findFirst();
    return randomDouble.orElse(0.0d);
  }

  private static List<Key> sortWithKeyBytesEncoder(List<Key> target, Map<String, Order> keyOrders) {
    List<Element> list = new ArrayList<>();
    for (Key key : target) {
      ByteBuffer byteBuffer = new KeyBytesEncoder().encode(key, keyOrders);
      list.add(new Element(byteBuffer, key));
    }
    return list.stream().sorted().map(e -> e.key).collect(Collectors.toList());
  }

  private static Comparator<Key> getComparator(Map<String, Order> keyOrders) {
    return (o1, o2) -> {
      if (o1.size() != o2.size()) {
        throw new IllegalArgumentException("The key size is different");
      }

      ComparisonChain comparisonChain = ComparisonChain.start();
      for (int i = 0; i < o1.size(); i++) {
        Value<?> left = o1.get().get(i);
        Value<?> right = o2.get().get(i);
        if (!left.getName().equals(right.getName())) {
          throw new IllegalArgumentException("The value name is different");
        }
        Order order = keyOrders.get(left.getName());
        comparisonChain =
            comparisonChain.compare(
                left,
                right,
                order == Order.ASC ? Ordering.natural() : Ordering.natural().reverse());
      }
      return comparisonChain.result();
    };
  }

  private static class Element implements Comparable<Element> {
    public final byte[] encodedBytes;
    public final Key key;

    public Element(ByteBuffer byteBuffer, Key key) {
      byte[] bytes = new byte[byteBuffer.remaining()];
      byteBuffer.get(bytes);
      this.encodedBytes = bytes;
      this.key = key;
    }

    @Override
    public int compareTo(Element o) {
      return UnsignedBytes.lexicographicalComparator().compare(encodedBytes, o.encodedBytes);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Element)) {
        return false;
      }
      Element element = (Element) o;
      return Arrays.equals(encodedBytes, element.encodedBytes) && key.equals(element.key);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(key);
      result = 31 * result + Arrays.hashCode(encodedBytes);
      return result;
    }
  }
}
