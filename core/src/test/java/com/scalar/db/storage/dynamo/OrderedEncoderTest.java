package com.scalar.db.storage.dynamo;

import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.UnsignedBytes;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.TextValue;
import java.util.Random;
import java.util.Set;
import org.apache.commons.lang.RandomStringUtils;
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
    Random randomGenerator = new Random();
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
  public void orderedEncode_WithDoubleValues_ShouldReturnOrderedEncodedBytes() {
    Random randomGenerator = new Random();
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
  public void orderedEncode_WithTextValues_ShouldReturnOrderedEncodedBytes() {
    for (Order order : ORDER_SET) {
      // using random values for testing
      for (int i = 0; i < 1000; i++) {
        // Arrange
        String v1 = RandomStringUtils.randomAlphanumeric(20);
        String v2 = RandomStringUtils.randomAlphanumeric(20);

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
