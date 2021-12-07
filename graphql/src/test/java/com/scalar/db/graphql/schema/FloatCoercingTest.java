package com.scalar.db.graphql.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import graphql.schema.CoercingParseLiteralException;
import graphql.schema.CoercingParseValueException;
import graphql.schema.CoercingSerializeException;
import java.math.BigDecimal;
import java.math.BigInteger;
import org.junit.Test;

public class FloatCoercingTest {
  @Test
  public void serialize_FloatGiven_ShouldReturnFloat() {
    // Arrange
    Float value = Float.MAX_VALUE;
    Float expected = Float.MAX_VALUE;

    // Act
    Float actual = FloatCoercing.INSTANCE.serialize(value);

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void serialize_InvalidTypeGiven_ShouldThrowException() {
    // Arrange
    Integer value = 1;

    // Act Assert
    assertThatThrownBy(() -> FloatCoercing.INSTANCE.serialize(value))
        .isInstanceOf(CoercingSerializeException.class);
  }

  @Test
  public void parseValue_IntegerGiven_ShouldReturnFloatValue() {
    // Arrange
    Integer value = Integer.MAX_VALUE;
    Float expected = (float) Integer.MAX_VALUE;

    // Act
    Float actual = FloatCoercing.INSTANCE.parseValue(value);

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void parseValue_FloatGiven_ShouldReturnFloatValue() {
    // Arrange
    Float value = Float.MAX_VALUE;
    Float expected = Float.MAX_VALUE;

    // Act
    Float actual = FloatCoercing.INSTANCE.parseValue(value);

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void parseValue_DoubleGiven_ShouldReturnFloatValue() {
    // Arrange
    Double value = (double) Float.MAX_VALUE;
    Float expected = Float.MAX_VALUE;

    // Act
    Float actual = FloatCoercing.INSTANCE.parseValue(value);

    // Act Assert
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void parseValue_StringGiven_ShouldThrowException() {
    // Arrange
    String value = "";

    // Act Assert
    assertThatThrownBy(() -> FloatCoercing.INSTANCE.parseValue(value))
        .isInstanceOf(CoercingParseValueException.class);
  }

  @Test
  public void parseLiteral_IntValueGiven_ShouldReturnFloat() {
    // Arrange
    BigInteger bi = BigInteger.valueOf(Long.MAX_VALUE);
    graphql.language.IntValue value = new graphql.language.IntValue(bi);
    Float expected = bi.floatValue();

    // Act
    Float actual = FloatCoercing.INSTANCE.parseLiteral(value);

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void parseLiteral_FloatValueGiven_ShouldReturnFloat() {
    // Arrange
    BigDecimal bd = BigDecimal.valueOf(Float.MAX_VALUE);
    graphql.language.FloatValue value = new graphql.language.FloatValue(bd);
    Float expected = bd.floatValue();

    // Act
    Float actual = FloatCoercing.INSTANCE.parseLiteral(value);

    // Assert
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void parseLiteral_StringValueGiven_ShouldThrowException() {
    // Arrange
    graphql.language.StringValue value = new graphql.language.StringValue("1");

    // Act Assert
    assertThatThrownBy(() -> FloatCoercing.INSTANCE.parseLiteral(value))
        .isInstanceOf(CoercingParseLiteralException.class);
  }
}
