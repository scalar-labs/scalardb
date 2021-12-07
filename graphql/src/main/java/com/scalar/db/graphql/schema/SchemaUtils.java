package com.scalar.db.graphql.schema;

import com.scalar.db.io.DataType;
import graphql.Scalars;
import graphql.schema.GraphQLScalarType;

final class SchemaUtils {

  private SchemaUtils() {}

  static GraphQLScalarType dataTypeToGraphQLScalarType(DataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return Scalars.GraphQLBoolean;
      case INT:
        return Scalars.GraphQLInt;
      case BIGINT:
        return CommonSchema.BIG_INT_SCALAR;
      case FLOAT:
        return CommonSchema.FLOAT_32_SCALAR;
      case DOUBLE:
        return Scalars.GraphQLFloat;
      case TEXT:
        return Scalars.GraphQLString;
      case BLOB:
      default:
        throw new IllegalArgumentException(dataType.name() + " type is not supported");
    }
  }
}
