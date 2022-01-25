package com.scalar.db.graphql.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.io.DataType;
import graphql.Scalars;
import graphql.schema.GraphQLScalarType;
import org.junit.Test;

public class ScalarDbTypesTest {
  @Test
  public void dataTypeToGraphQLScalarType_BooleanGiven_ShouldReturnBoolean() {
    // Act
    GraphQLScalarType type = ScalarDbTypes.dataTypeToGraphQLScalarType(DataType.BOOLEAN);

    // Assert
    assertThat(type).isEqualTo(Scalars.GraphQLBoolean);
  }

  @Test
  public void dataTypeToGraphQLScalarType_FloatGiven_ShouldReturnCustomFloat32() {
    // Act
    GraphQLScalarType type = ScalarDbTypes.dataTypeToGraphQLScalarType(DataType.FLOAT);

    // Assert
    assertThat(type).isEqualTo(ScalarDbTypes.FLOAT_32_SCALAR);
  }

  @Test
  public void dataTypeToGraphQLScalarType_DoubleGiven_ShouldReturnFloat() {
    // Act
    GraphQLScalarType type = ScalarDbTypes.dataTypeToGraphQLScalarType(DataType.DOUBLE);

    // Assert
    assertThat(type).isEqualTo(Scalars.GraphQLFloat);
  }

  @Test
  public void dataTypeToGraphQLScalarType_BigIntGiven_ShouldReturnCustomBigInt() {
    // Act
    GraphQLScalarType type = ScalarDbTypes.dataTypeToGraphQLScalarType(DataType.BIGINT);

    // Assert
    assertThat(type).isEqualTo(ScalarDbTypes.BIG_INT_SCALAR);
  }

  @Test
  public void dataTypeToGraphQLScalarType_TextGiven_ShouldReturnString() {
    // Act
    GraphQLScalarType type = ScalarDbTypes.dataTypeToGraphQLScalarType(DataType.TEXT);

    // Assert
    assertThat(type).isEqualTo(Scalars.GraphQLString);
  }

  @Test
  public void dataTypeToGraphQLScalarType_BlobGiven_ShouldThrowIllegalArgumentException() {
    // Act Assert
    assertThatThrownBy(() -> ScalarDbTypes.dataTypeToGraphQLScalarType(DataType.BLOB))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
