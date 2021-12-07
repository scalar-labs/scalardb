package com.scalar.db.graphql.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.io.BigIntValue;
import graphql.schema.CoercingParseLiteralException;
import graphql.schema.CoercingParseValueException;
import graphql.schema.CoercingSerializeException;
import java.math.BigDecimal;
import java.math.BigInteger;
import org.junit.Test;

public class BigIntCoercingTest {
  @Test
  public void serialize_LongInValidRangeGiven_ShouldReturnLong() {
    // Arrange
    Long value = BigIntValue.MAX_VALUE;
    Long expected = BigIntValue.MAX_VALUE;

    // Act
    Long actual = BigIntCoercing.INSTANCE.serialize(value);

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void serialize_LongOutOfUpperBoundGiven_ShouldThrowException() {
    // Arrange
    Long value = BigIntValue.MAX_VALUE + 1;

    // Act Assert
    assertThatThrownBy(() -> BigIntCoercing.INSTANCE.serialize(value))
        .isInstanceOf(CoercingSerializeException.class);
  }

  @Test
  public void serialize_LongOutOfLowerBoundGiven_ShouldThrowException() {
    // Arrange
    Long value = BigIntValue.MIN_VALUE - 1;

    // Act Assert
    assertThatThrownBy(() -> BigIntCoercing.INSTANCE.serialize(value))
        .isInstanceOf(CoercingSerializeException.class);
  }

  @Test
  public void serialize_InvalidTypeGiven_ShouldThrowException() {
    // Arrange
    String value = "1";

    // Act Assert
    assertThatThrownBy(() -> BigIntCoercing.INSTANCE.serialize(value))
        .isInstanceOf(CoercingSerializeException.class);
  }

  @Test
  public void parseValue_BigIntegerInValidRangeGiven_ShouldReturnLongValue() {
    // Arrange
    BigInteger value = BigInteger.valueOf(BigIntValue.MAX_VALUE);
    Long expected = BigIntValue.MAX_VALUE;

    // Act
    Long actual = BigIntCoercing.INSTANCE.parseValue(value);

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void parseValue_LongInValidRangeGiven_ShouldReturnLongValue() {
    // Arrange
    Long value = BigIntValue.MAX_VALUE;
    Long expected = BigIntValue.MAX_VALUE;

    // Act
    Long actual = BigIntCoercing.INSTANCE.parseValue(value);

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void parseValue_StringInValidRangeGiven_ShouldReturnLongValue() {
    // Arrange
    String value = String.valueOf(BigIntValue.MAX_VALUE);
    Long expected = BigIntValue.MAX_VALUE;

    // Act
    Long actual = BigIntCoercing.INSTANCE.parseValue(value);

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void parseValue_ValueOutOfUpperBoundGiven_ShouldThrowException() {
    // Arrange
    Long value = BigIntValue.MAX_VALUE + 1;

    // Act Assert
    assertThatThrownBy(() -> BigIntCoercing.INSTANCE.parseValue(value))
        .isInstanceOf(CoercingParseValueException.class);
  }

  @Test
  public void parseValue_ValueOutOfLowerBoundGiven_ShouldThrowException() {
    // Arrange
    Long value = BigIntValue.MIN_VALUE - 1;

    // Act Assert
    assertThatThrownBy(() -> BigIntCoercing.INSTANCE.parseValue(value))
        .isInstanceOf(CoercingParseValueException.class);
  }

  @Test
  public void parseValue_InvalidTypeGiven_ShouldThrowException() {
    // Arrange
    double value = 0.1;

    // Act Assert
    assertThatThrownBy(() -> BigIntCoercing.INSTANCE.parseValue(value))
        .isInstanceOf(CoercingParseValueException.class);
  }

  @Test
  public void parseLiteral_IntValueInValidRangeGiven_ShouldReturnLongValue() {
    // Arrange
    graphql.language.IntValue value =
        new graphql.language.IntValue(BigInteger.valueOf(BigIntValue.MAX_VALUE));
    Long expected = BigIntValue.MAX_VALUE;

    // Act
    Long actual = BigIntCoercing.INSTANCE.parseLiteral(value);

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void parseLiteral_IntValueOutOfUpperBoundGiven_ShouldThrowException() {
    // Arrange
    graphql.language.IntValue value =
        new graphql.language.IntValue(BigInteger.valueOf(BigIntValue.MAX_VALUE + 1));

    // Act Assert
    assertThatThrownBy(() -> BigIntCoercing.INSTANCE.parseLiteral(value))
        .isInstanceOf(CoercingParseLiteralException.class);
  }

  @Test
  public void parseLiteral_IntValueOutOfLowerBoundGiven_ShouldThrowException() {
    // Arrange
    graphql.language.IntValue value =
        new graphql.language.IntValue(BigInteger.valueOf(BigIntValue.MIN_VALUE - 1));

    // Act Assert
    assertThatThrownBy(() -> BigIntCoercing.INSTANCE.parseLiteral(value))
        .isInstanceOf(CoercingParseLiteralException.class);
  }

  @Test
  public void parseLiteral_InvalidTypeGiven_ShouldThrowException() {
    // Arrange
    graphql.language.FloatValue value = new graphql.language.FloatValue(new BigDecimal("0.1"));

    // Act Assert
    assertThatThrownBy(() -> BigIntCoercing.INSTANCE.parseLiteral(value))
        .isInstanceOf(CoercingParseLiteralException.class);
  }
}
