package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class DatabaseOperationAttributesTest {

  @Test
  public void setCrossPartitionScanEnabled_ShouldSetAttribute() {
    // Arrange
    Map<String, String> attributes = new HashMap<>();

    // Act
    DatabaseOperationAttributes.setCrossPartitionScanEnabled(attributes, true);

    // Assert
    assertThat(attributes)
        .containsEntry(DatabaseOperationAttributes.CROSS_PARTITION_SCAN_ENABLED, "true");
  }

  @Test
  public void isCrossPartitionScanEnabled_WithAttributeSetToTrue_ShouldReturnTrue() {
    // Arrange
    ScanAll scanAll =
        (ScanAll)
            Scan.newBuilder()
                .namespace("ns")
                .table("tbl")
                .all()
                .attribute(DatabaseOperationAttributes.CROSS_PARTITION_SCAN_ENABLED, "true")
                .build();

    // Act
    boolean actual = DatabaseOperationAttributes.isCrossPartitionScanEnabled(scanAll);

    // Assert
    assertThat(actual).isTrue();
  }

  @Test
  public void isCrossPartitionScanEnabled_WithAttributeNotSet_ShouldReturnFalse() {
    // Arrange
    ScanAll scanAll = (ScanAll) Scan.newBuilder().namespace("ns").table("tbl").all().build();

    // Act
    boolean actual = DatabaseOperationAttributes.isCrossPartitionScanEnabled(scanAll);

    // Assert
    assertThat(actual).isFalse();
  }

  @Test
  public void setCrossPartitionScanFilteringEnabled_ShouldSetAttribute() {
    // Arrange
    Map<String, String> attributes = new HashMap<>();

    // Act
    DatabaseOperationAttributes.setCrossPartitionScanFilteringEnabled(attributes, true);

    // Assert
    assertThat(attributes)
        .containsEntry(DatabaseOperationAttributes.CROSS_PARTITION_SCAN_FILTERING_ENABLED, "true");
  }

  @Test
  public void isCrossPartitionScanFilteringEnabled_WithAttributeSetToTrue_ShouldReturnTrue() {
    // Arrange
    ScanAll scanAll =
        (ScanAll)
            Scan.newBuilder()
                .namespace("ns")
                .table("tbl")
                .all()
                .attribute(
                    DatabaseOperationAttributes.CROSS_PARTITION_SCAN_FILTERING_ENABLED, "true")
                .build();

    // Act
    boolean actual = DatabaseOperationAttributes.isCrossPartitionScanFilteringEnabled(scanAll);

    // Assert
    assertThat(actual).isTrue();
  }

  @Test
  public void isCrossPartitionScanFilteringEnabled_WithAttributeNotSet_ShouldReturnFalse() {
    // Arrange
    ScanAll scanAll = (ScanAll) Scan.newBuilder().namespace("ns").table("tbl").all().build();

    // Act
    boolean actual = DatabaseOperationAttributes.isCrossPartitionScanFilteringEnabled(scanAll);

    // Assert
    assertThat(actual).isFalse();
  }

  @Test
  public void setCrossPartitionScanOrderingEnabled_ShouldSetAttribute() {
    // Arrange
    Map<String, String> attributes = new HashMap<>();

    // Act
    DatabaseOperationAttributes.setCrossPartitionScanOrderingEnabled(attributes, true);

    // Assert
    assertThat(attributes)
        .containsEntry(DatabaseOperationAttributes.CROSS_PARTITION_SCAN_ORDERING_ENABLED, "true");
  }

  @Test
  public void isCrossPartitionScanOrderingEnabled_WithAttributeSetToTrue_ShouldReturnTrue() {
    // Arrange
    ScanAll scanAll =
        (ScanAll)
            Scan.newBuilder()
                .namespace("ns")
                .table("tbl")
                .all()
                .attribute(
                    DatabaseOperationAttributes.CROSS_PARTITION_SCAN_ORDERING_ENABLED, "true")
                .build();

    // Act
    boolean actual = DatabaseOperationAttributes.isCrossPartitionScanOrderingEnabled(scanAll);

    // Assert
    assertThat(actual).isTrue();
  }

  @Test
  public void isCrossPartitionScanOrderingEnabled_WithAttributeNotSet_ShouldReturnFalse() {
    // Arrange
    ScanAll scanAll = (ScanAll) Scan.newBuilder().namespace("ns").table("tbl").all().build();

    // Act
    boolean actual = DatabaseOperationAttributes.isCrossPartitionScanOrderingEnabled(scanAll);

    // Assert
    assertThat(actual).isFalse();
  }
}
