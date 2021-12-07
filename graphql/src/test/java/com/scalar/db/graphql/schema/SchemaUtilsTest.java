package com.scalar.db.graphql.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.io.DataType;
import graphql.Scalars;
import graphql.schema.GraphQLScalarType;
import org.junit.Test;

public class SchemaUtilsTest {
  @Test
  public void dataTypeToGraphQLScalarType_BooleanGiven_ShouldReturnBoolean() {
    // Act
    GraphQLScalarType type = SchemaUtils.dataTypeToGraphQLScalarType(DataType.BOOLEAN);

    // Assert
    assertThat(type).isEqualTo(Scalars.GraphQLBoolean);
  }

  @Test
  public void dataTypeToGraphQLScalarType_FloatGiven_ShouldReturnCustomFloat32() {
    // Act
    GraphQLScalarType type = SchemaUtils.dataTypeToGraphQLScalarType(DataType.FLOAT);

    // Assert
    assertThat(type).isEqualTo(CommonSchema.FLOAT_32_SCALAR);
  }

  @Test
  public void dataTypeToGraphQLScalarType_DoubleGiven_ShouldReturnFloat() {
    // Act
    GraphQLScalarType type = SchemaUtils.dataTypeToGraphQLScalarType(DataType.DOUBLE);

    // Assert
    assertThat(type).isEqualTo(Scalars.GraphQLFloat);
  }

  @Test
  public void dataTypeToGraphQLScalarType_BigIntGiven_ShouldReturnCustomBigInt() {
    // Act
    GraphQLScalarType type = SchemaUtils.dataTypeToGraphQLScalarType(DataType.BIGINT);

    // Assert
    assertThat(type).isEqualTo(CommonSchema.BIG_INT_SCALAR);
  }

  @Test
  public void dataTypeToGraphQLScalarType_TextGiven_ShouldReturnString() {
    // Act
    GraphQLScalarType type = SchemaUtils.dataTypeToGraphQLScalarType(DataType.TEXT);

    // Assert
    assertThat(type).isEqualTo(Scalars.GraphQLString);
  }

  @Test
  public void dataTypeToGraphQLScalarType_BlobGiven_ShouldThrowIllegalArgumentException() {
    // Act Assert
    assertThatThrownBy(() -> SchemaUtils.dataTypeToGraphQLScalarType(DataType.BLOB))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
