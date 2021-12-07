package com.scalar.db.graphql.schema;

import graphql.schema.Coercing;
import graphql.schema.CoercingParseLiteralException;
import graphql.schema.CoercingParseValueException;
import graphql.schema.CoercingSerializeException;
import java.math.BigDecimal;
import java.math.BigInteger;
import javax.annotation.Nonnull;

/** Handles type conversions for Scalar DB {@link com.scalar.db.io.FloatValue}. */
class FloatCoercing implements Coercing<Float, Float> {
  static FloatCoercing INSTANCE = new FloatCoercing();

  private FloatCoercing() {}

  @Nonnull
  @Override
  public Float serialize(@Nonnull Object dataFetcherResult) throws CoercingSerializeException {
    if (dataFetcherResult instanceof Float) {
      return (Float) dataFetcherResult;
    } else {
      throw new CoercingSerializeException("Expected a long value");
    }
  }

  @Nonnull
  @Override
  public Float parseValue(@Nonnull Object input) throws CoercingParseValueException {
    if (input instanceof Number) {
      return ((Number) input).floatValue();
    } else {
      throw new CoercingParseValueException("Expected an integer or float value");
    }
  }

  @Nonnull
  @Override
  public Float parseLiteral(@Nonnull Object input) throws CoercingParseLiteralException {
    if (input instanceof graphql.language.IntValue) {
      BigInteger bi = ((graphql.language.IntValue) input).getValue();
      return bi.floatValue();
    } else if (input instanceof graphql.language.FloatValue) {
      BigDecimal bd = ((graphql.language.FloatValue) input).getValue();
      return bd.floatValue();
    } else {
      throw new CoercingParseLiteralException("Expected AST type 'IntValue' or 'FloatValue'");
    }
  }
}
