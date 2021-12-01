package com.scalar.db.graphql.schema;

import com.scalar.db.io.BigIntValue;
import graphql.schema.Coercing;
import graphql.schema.CoercingParseLiteralException;
import graphql.schema.CoercingParseValueException;
import graphql.schema.CoercingSerializeException;
import java.math.BigInteger;

/** Handles type conversions for Scalar DB {@link BigIntValue}. */
class BigIntCoercing implements Coercing<Long, Long> {
  static final BigIntCoercing INSTANCE = new BigIntCoercing();

  private BigIntCoercing() {}

  @Override
  public Long serialize(Object dataFetcherResult) throws CoercingSerializeException {
    if (!(dataFetcherResult instanceof Long)) {
      throw new CoercingSerializeException("Expected a long value");
    }
    Long value = (Long) dataFetcherResult;
    if (!isInBigIntValueRange(value)) {
      throw new CoercingSerializeException("Out of range for BigIntValue");
    }
    return value;
  }

  @Override
  public Long parseValue(Object input) throws CoercingParseValueException {
    long value;
    try {
      if (input instanceof BigInteger) {
        value = ((BigInteger) input).longValueExact();
      } else if (input instanceof Long || input instanceof Integer) {
        value = ((Number) input).longValue();
      } else if (input instanceof String) {
        value = Long.parseLong((String) input);
      } else {
        throw new CoercingParseValueException("Expected a long, integer, or string value");
      }
    } catch (ArithmeticException | NumberFormatException e) {
      throw new CoercingParseValueException(e);
    }
    if (isInBigIntValueRange(value)) {
      return value;
    } else {
      throw new CoercingParseValueException("Out of range for BigIntValue");
    }
  }

  @Override
  public Long parseLiteral(Object input) throws CoercingParseLiteralException {
    if (!(input instanceof graphql.language.IntValue)) {
      throw new CoercingParseLiteralException("Expected AST type 'IntValue'");
    }
    long value;
    try {
      value = ((graphql.language.IntValue) input).getValue().longValueExact();
    } catch (ArithmeticException e) {
      throw new CoercingParseLiteralException(e);
    }
    if (isInBigIntValueRange(value)) {
      return value;
    } else {
      throw new CoercingParseLiteralException("Out of range for BigIntValue");
    }
  }

  private boolean isInBigIntValueRange(Long value) {
    return value >= BigIntValue.MIN_VALUE && value <= BigIntValue.MAX_VALUE;
  }
}
