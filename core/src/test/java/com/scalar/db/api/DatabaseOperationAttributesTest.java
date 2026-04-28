package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class DatabaseOperationAttributesTest {

  private static ScanAll scanAllWithAttribute(String key, String value) {
    return (ScanAll)
        Scan.newBuilder().namespace("ns").table("tbl").all().attribute(key, value).build();
  }

  private static ScanAll scanAllWithoutAttributes() {
    return (ScanAll) Scan.newBuilder().namespace("ns").table("tbl").all().build();
  }

  // -------------------- cross_partition_scan --------------------

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
        scanAllWithAttribute(DatabaseOperationAttributes.CROSS_PARTITION_SCAN_ENABLED, "true");

    // Act Assert
    assertThat(DatabaseOperationAttributes.isCrossPartitionScanEnabled(scanAll, false)).isTrue();
    assertThat(DatabaseOperationAttributes.isCrossPartitionScanEnabled(scanAll, true)).isTrue();
  }

  @Test
  public void isCrossPartitionScanEnabled_WithAttributeSetToFalse_ShouldReturnFalse() {
    // Arrange
    ScanAll scanAll =
        scanAllWithAttribute(DatabaseOperationAttributes.CROSS_PARTITION_SCAN_ENABLED, "false");

    // Act Assert
    assertThat(DatabaseOperationAttributes.isCrossPartitionScanEnabled(scanAll, false)).isFalse();
    assertThat(DatabaseOperationAttributes.isCrossPartitionScanEnabled(scanAll, true)).isFalse();
  }

  @Test
  public void isCrossPartitionScanEnabled_WithAttributeNotSet_ShouldReturnDefaultValue() {
    // Arrange
    ScanAll scanAll = scanAllWithoutAttributes();

    // Act Assert
    assertThat(DatabaseOperationAttributes.isCrossPartitionScanEnabled(scanAll, false)).isFalse();
    assertThat(DatabaseOperationAttributes.isCrossPartitionScanEnabled(scanAll, true)).isTrue();
  }

  // -------------------- cross_partition_scan_filtering --------------------

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
        scanAllWithAttribute(
            DatabaseOperationAttributes.CROSS_PARTITION_SCAN_FILTERING_ENABLED, "true");

    // Act Assert
    assertThat(DatabaseOperationAttributes.isCrossPartitionScanFilteringEnabled(scanAll, false))
        .isTrue();
    assertThat(DatabaseOperationAttributes.isCrossPartitionScanFilteringEnabled(scanAll, true))
        .isTrue();
  }

  @Test
  public void isCrossPartitionScanFilteringEnabled_WithAttributeSetToFalse_ShouldReturnFalse() {
    // Arrange
    ScanAll scanAll =
        scanAllWithAttribute(
            DatabaseOperationAttributes.CROSS_PARTITION_SCAN_FILTERING_ENABLED, "false");

    // Act Assert
    assertThat(DatabaseOperationAttributes.isCrossPartitionScanFilteringEnabled(scanAll, false))
        .isFalse();
    assertThat(DatabaseOperationAttributes.isCrossPartitionScanFilteringEnabled(scanAll, true))
        .isFalse();
  }

  @Test
  public void isCrossPartitionScanFilteringEnabled_WithAttributeNotSet_ShouldReturnDefaultValue() {
    // Arrange
    ScanAll scanAll = scanAllWithoutAttributes();

    // Act Assert
    assertThat(DatabaseOperationAttributes.isCrossPartitionScanFilteringEnabled(scanAll, false))
        .isFalse();
    assertThat(DatabaseOperationAttributes.isCrossPartitionScanFilteringEnabled(scanAll, true))
        .isTrue();
  }

  // -------------------- cross_partition_scan_ordering --------------------

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
        scanAllWithAttribute(
            DatabaseOperationAttributes.CROSS_PARTITION_SCAN_ORDERING_ENABLED, "true");

    // Act Assert
    assertThat(DatabaseOperationAttributes.isCrossPartitionScanOrderingEnabled(scanAll, false))
        .isTrue();
    assertThat(DatabaseOperationAttributes.isCrossPartitionScanOrderingEnabled(scanAll, true))
        .isTrue();
  }

  @Test
  public void isCrossPartitionScanOrderingEnabled_WithAttributeSetToFalse_ShouldReturnFalse() {
    // Arrange
    ScanAll scanAll =
        scanAllWithAttribute(
            DatabaseOperationAttributes.CROSS_PARTITION_SCAN_ORDERING_ENABLED, "false");

    // Act Assert
    assertThat(DatabaseOperationAttributes.isCrossPartitionScanOrderingEnabled(scanAll, false))
        .isFalse();
    assertThat(DatabaseOperationAttributes.isCrossPartitionScanOrderingEnabled(scanAll, true))
        .isFalse();
  }

  @Test
  public void isCrossPartitionScanOrderingEnabled_WithAttributeNotSet_ShouldReturnDefaultValue() {
    // Arrange
    ScanAll scanAll = scanAllWithoutAttributes();

    // Act Assert
    assertThat(DatabaseOperationAttributes.isCrossPartitionScanOrderingEnabled(scanAll, false))
        .isFalse();
    assertThat(DatabaseOperationAttributes.isCrossPartitionScanOrderingEnabled(scanAll, true))
        .isTrue();
  }
}
