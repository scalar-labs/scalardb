package com.scalar.db.storage.dynamo.ordered;

import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.UnsignedBytes;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.TextValue;
import com.scalar.db.storage.dynamo.ordered.OrderedEncoder;
import java.util.Random;
import java.util.Set;
import org.apache.commons.lang3.RandomStringUtils;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

public class OrderedEncoderTest {
  Set<Order> ORDER_SET = ImmutableSet.<Order>builder().add(Order.ASC).add(Order.DESC).build();

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void orderedEncode_WithIntegerValues_ShouldReturnOrderedEncodedBytes() {
    Random randomGenerator = new Random(1);
    for (Order order : ORDER_SET) {
      // using random values for testing
      for (int i = 0; i < 1000; i++) {
        // Arrange
        int v1 = randomGenerator.nextInt();
        int v2 = randomGenerator.nextInt();

        IntValue intValue1 = new IntValue(v1);
        IntValue intValue2 = new IntValue(v2);
        int expectedComparision = Integer.compare(v1, v2);

        // Act
        byte[] encoded1 = OrderedEncoder.orderedEncode(intValue1, order);
        byte[] encoded2 = OrderedEncoder.orderedEncode(intValue2, order);
        int bytesComparision =
            UnsignedBytes.lexicographicalComparator().compare(encoded1, encoded2);

        // Assert
        if (order == Order.ASC) {
          Assertions.assertThat(bytesComparision > 0).isEqualTo(expectedComparision > 0);
        } else {
          Assertions.assertThat(bytesComparision > 0).isEqualTo(expectedComparision < 0);
        }
        Assertions.assertThat(bytesComparision == 0).isEqualTo(expectedComparision == 0);
      }
    }
  }

  @Test
  public void orderedEncode_WithLongValues_ShouldReturnOrderedEncodedBytes() {
    Random randomGenerator = new Random(2);
    for (Order order : ORDER_SET) {
      // using random values for testing
      for (int i = 0; i < 1000; i++) {
        // Arrange
        long v1 = randomGenerator.nextLong();
        long v2 = randomGenerator.nextLong();

        BigIntValue bigIntValue1 = new BigIntValue(v1);
        BigIntValue bigIntValue2 = new BigIntValue(v2);
        int expectedComparision = Long.compare(v1, v2);

        // Act
        byte[] encoded1 = OrderedEncoder.orderedEncode(bigIntValue1, order);
        byte[] encoded2 = OrderedEncoder.orderedEncode(bigIntValue2, order);
        int bytesComparision =
            UnsignedBytes.lexicographicalComparator().compare(encoded1, encoded2);

        // Assert
        if (order == Order.ASC) {
          Assertions.assertThat(bytesComparision > 0).isEqualTo(expectedComparision > 0);
        } else {
          Assertions.assertThat(bytesComparision > 0).isEqualTo(expectedComparision < 0);
        }
        Assertions.assertThat(bytesComparision == 0).isEqualTo(expectedComparision == 0);
      }
    }
  }

  @Test
  public void orderedEncode_WithFloatValues_ShouldReturnOrderedEncodedBytes() {
    Random randomGenerator = new Random(3);
    for (Order order : ORDER_SET) {
      // using random values for testing
      for (int i = 0; i < 1000; i++) {
        // Arrange
        float v1 = randomGenerator.nextFloat();
        float v2 = randomGenerator.nextFloat();

        FloatValue floatValue1 = new FloatValue(v1);
        FloatValue floatValue2 = new FloatValue(v2);
        int expectedComparision = Float.compare(v1, v2);

        // Act
        byte[] encoded1 = OrderedEncoder.orderedEncode(floatValue1, order);
        byte[] encoded2 = OrderedEncoder.orderedEncode(floatValue2, order);
        int bytesComparision =
            UnsignedBytes.lexicographicalComparator().compare(encoded1, encoded2);

        // Assert
        if (order == Order.ASC) {
          Assertions.assertThat(bytesComparision > 0).isEqualTo(expectedComparision > 0);
        } else {
          Assertions.assertThat(bytesComparision > 0).isEqualTo(expectedComparision < 0);
        }
        Assertions.assertThat(bytesComparision == 0).isEqualTo(expectedComparision == 0);
      }
    }
  }

  @Test
  public void orderedEncode_WithDoubleValues_ShouldReturnOrderedEncodedBytes() {
    Random randomGenerator = new Random(4);
    for (Order order : ORDER_SET) {
      // using random values for testing
      for (int i = 0; i < 1000; i++) {
        // Arrange
        double v1 = randomGenerator.nextDouble();
        double v2 = randomGenerator.nextDouble();

        DoubleValue doubleValue1 = new DoubleValue(v1);
        DoubleValue doubleValue2 = new DoubleValue(v2);
        int expectedComparision = Double.compare(v1, v2);

        // Act
        byte[] encoded1 = OrderedEncoder.orderedEncode(doubleValue1, order);
        byte[] encoded2 = OrderedEncoder.orderedEncode(doubleValue2, order);
        int bytesComparision =
            UnsignedBytes.lexicographicalComparator().compare(encoded1, encoded2);

        // Assert
        if (order == Order.ASC) {
          Assertions.assertThat(bytesComparision > 0).isEqualTo(expectedComparision > 0);
        } else {
          Assertions.assertThat(bytesComparision > 0).isEqualTo(expectedComparision < 0);
        }
        Assertions.assertThat(bytesComparision == 0).isEqualTo(expectedComparision == 0);
      }
    }
  }

  @Test
  public void orderedEncode_WithBlobValues_ShouldReturnOrderedEncodedBytes() {
    Random randomGenerator = new Random(5);
    for (Order order : ORDER_SET) {
      // using random values for testing
      for (int i = 0; i < 1000; i++) {
        // Arrange
        byte[] v1 = new byte[20];
        randomGenerator.nextBytes(v1);
        byte[] v2 = new byte[20];
        randomGenerator.nextBytes(v2);

        BlobValue blobValue1 = new BlobValue(v1);
        BlobValue blobValue2 = new BlobValue(v2);
        int expectedComparision = UnsignedBytes.lexicographicalComparator().compare(v1, v2);

        // Act
        byte[] encoded1 = OrderedEncoder.orderedEncode(blobValue1, order);
        byte[] encoded2 = OrderedEncoder.orderedEncode(blobValue2, order);
        int bytesComparision =
            UnsignedBytes.lexicographicalComparator().compare(encoded1, encoded2);

        // Assert
        if (order == Order.ASC) {
          Assertions.assertThat(bytesComparision > 0).isEqualTo(expectedComparision > 0);
        } else {
          Assertions.assertThat(bytesComparision > 0).isEqualTo(expectedComparision < 0);
        }
        Assertions.assertThat(bytesComparision == 0).isEqualTo(expectedComparision == 0);
      }
    }
  }

  @Test
  public void orderedEncode_WithBooleanValues_ShouldReturnOrderedEncodedBytes() {
    Random randomGenerator = new Random(6);
    for (Order order : ORDER_SET) {
      // using random values for testing
      for (int i = 0; i < 1000; i++) {
        // Arrange
        boolean v1 = randomGenerator.nextBoolean();
        boolean v2 = randomGenerator.nextBoolean();

        BooleanValue booleanValue1 = new BooleanValue(v1);
        BooleanValue booleanValue2 = new BooleanValue(v2);
        int expectedComparision = Boolean.compare(v1, v2);

        // Act
        byte[] encoded1 = OrderedEncoder.orderedEncode(booleanValue1, order);
        byte[] encoded2 = OrderedEncoder.orderedEncode(booleanValue2, order);
        int bytesComparision =
            UnsignedBytes.lexicographicalComparator().compare(encoded1, encoded2);

        // Assert
        if (order == Order.ASC) {
          Assertions.assertThat(bytesComparision > 0).isEqualTo(expectedComparision > 0);
        } else {
          Assertions.assertThat(bytesComparision > 0).isEqualTo(expectedComparision < 0);
        }
        Assertions.assertThat(bytesComparision == 0).isEqualTo(expectedComparision == 0);
      }
    }
  }

  @Test
  public void orderedEncode_WithTextValues_ShouldReturnOrderedEncodedBytes() {
    Random randomGenerator = new Random(7);
    for (Order order : ORDER_SET) {
      // using random values for testing
      for (int i = 0; i < 1000; i++) {
        // Arrange
        String v1 = RandomStringUtils.random(20, 0, 0, true, true, null, randomGenerator);
        String v2 = RandomStringUtils.random(20, 0, 0, true, true, null, randomGenerator);

        TextValue textValue1 = new TextValue(v1);
        TextValue textValue2 = new TextValue(v2);
        int expectedComparision = textValue1.compareTo(textValue2);

        // Act
        byte[] encoded1 = OrderedEncoder.orderedEncode(textValue1, order);
        byte[] encoded2 = OrderedEncoder.orderedEncode(textValue2, order);
        int bytesComparision =
            UnsignedBytes.lexicographicalComparator().compare(encoded1, encoded2);

        // Assert
        if (order == Order.ASC) {
          Assertions.assertThat(bytesComparision > 0).isEqualTo(expectedComparision > 0);
        } else {
          Assertions.assertThat(bytesComparision > 0).isEqualTo(expectedComparision < 0);
        }
        Assertions.assertThat(bytesComparision == 0).isEqualTo(expectedComparision == 0);
      }
    }
  }
}
