package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Put;
import com.scalar.db.io.Key;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class JdbcOperationAttributesTest {

  @Test
  public void
      isLeftOuterVirtualTablePutIfIsNullOnRightColumnsConversionEnabled_WithoutAttribute_ShouldReturnTrue() {
    // Arrange
    Put put =
        Put.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("pk", 1)).build();

    // Act
    boolean result =
        JdbcOperationAttributes.isLeftOuterVirtualTablePutIfIsNullOnRightColumnsConversionEnabled(
            put);

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void
      isLeftOuterVirtualTablePutIfIsNullOnRightColumnsConversionEnabled_WithTrueAttribute_ShouldReturnTrue() {
    // Arrange
    Map<String, String> attributes = new HashMap<>();
    JdbcOperationAttributes.setLeftOuterVirtualTablePutIfIsNullOnRightColumnsConversionEnabled(
        attributes, true);

    Put put =
        Put.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("pk", 1))
            .attributes(attributes)
            .build();

    // Act
    boolean result =
        JdbcOperationAttributes.isLeftOuterVirtualTablePutIfIsNullOnRightColumnsConversionEnabled(
            put);

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void
      isLeftOuterVirtualTablePutIfIsNullOnRightColumnsConversionEnabled_WithFalseAttribute_ShouldReturnFalse() {
    // Arrange
    Map<String, String> attributes = new HashMap<>();
    JdbcOperationAttributes.setLeftOuterVirtualTablePutIfIsNullOnRightColumnsConversionEnabled(
        attributes, false);

    Put put =
        Put.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("pk", 1))
            .attributes(attributes)
            .build();

    // Act
    boolean result =
        JdbcOperationAttributes.isLeftOuterVirtualTablePutIfIsNullOnRightColumnsConversionEnabled(
            put);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void
      setLeftOuterVirtualTablePutIfIsNullOnRightColumnsConversionEnabled_ShouldSetAttributeProperly() {
    // Arrange
    Map<String, String> attributes = new HashMap<>();

    // Act
    JdbcOperationAttributes.setLeftOuterVirtualTablePutIfIsNullOnRightColumnsConversionEnabled(
        attributes, false);

    // Assert
    assertThat(attributes)
        .containsEntry(
            JdbcOperationAttributes
                .LEFT_OUTER_VIRTUAL_TABLE_PUT_IF_IS_NULL_ON_RIGHT_COLUMNS_CONVERSION_ENABLED,
            "false");
  }

  @Test
  public void
      isLeftOuterVirtualTableDeleteIfIsNullOnRightColumnsAllowed_WithoutAttribute_ShouldReturnFalse() {
    // Arrange
    Delete delete =
        Delete.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofInt("pk", 1)).build();

    // Act
    boolean result =
        JdbcOperationAttributes.isLeftOuterVirtualTableDeleteIfIsNullOnRightColumnsAllowed(delete);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void
      isLeftOuterVirtualTableDeleteIfIsNullOnRightColumnsAllowed_WithTrueAttribute_ShouldReturnTrue() {
    // Arrange
    Map<String, String> attributes = new HashMap<>();
    JdbcOperationAttributes.setLeftOuterVirtualTableDeleteIfIsNullOnRightColumnsAllowed(
        attributes, true);

    Delete delete =
        Delete.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("pk", 1))
            .attributes(attributes)
            .build();

    // Act
    boolean result =
        JdbcOperationAttributes.isLeftOuterVirtualTableDeleteIfIsNullOnRightColumnsAllowed(delete);

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void
      isLeftOuterVirtualTableDeleteIfIsNullOnRightColumnsAllowed_WithFalseAttribute_ShouldReturnFalse() {
    // Arrange
    Map<String, String> attributes = new HashMap<>();
    JdbcOperationAttributes.setLeftOuterVirtualTableDeleteIfIsNullOnRightColumnsAllowed(
        attributes, false);

    Delete delete =
        Delete.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("pk", 1))
            .attributes(attributes)
            .build();

    // Act
    boolean result =
        JdbcOperationAttributes.isLeftOuterVirtualTableDeleteIfIsNullOnRightColumnsAllowed(delete);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void
      setLeftOuterVirtualTableDeleteIfIsNullOnRightColumnsAllowed_ShouldSetAttributeProperly() {
    // Arrange
    Map<String, String> attributes = new HashMap<>();

    // Act
    JdbcOperationAttributes.setLeftOuterVirtualTableDeleteIfIsNullOnRightColumnsAllowed(
        attributes, true);

    // Assert
    assertThat(attributes)
        .containsEntry(
            JdbcOperationAttributes.LEFT_OUTER_VIRTUAL_TABLE_DELETE_IF_IS_NULL_FOR_RIGHT_ALLOWED,
            "true");
  }
}
