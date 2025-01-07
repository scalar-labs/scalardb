package com.scalar.db.dataloader.core.dataexport.validation;

import static com.scalar.db.dataloader.core.ErrorMessage.*;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.core.FileFormat;
import com.scalar.db.dataloader.core.ScanRange;
import com.scalar.db.dataloader.core.dataexport.ExportOptions;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ExportOptionsValidatorTest {

  private TableMetadata singlePkCkMetadata;
  private TableMetadata multiplePkCkMetadata;
  private List<String> projectedColumns;

  @BeforeEach
  void setup() {
    singlePkCkMetadata = createMockMetadata(1, 1);
    multiplePkCkMetadata = createMockMetadata(2, 2);
    projectedColumns = createProjectedColumns();
  }

  private TableMetadata createMockMetadata(int pkCount, int ckCount) {
    TableMetadata.Builder builder = TableMetadata.newBuilder();

    // Add partition keys
    for (int i = 1; i <= pkCount; i++) {
      builder.addColumn("pk" + i, DataType.INT);
      builder.addPartitionKey("pk" + i);
    }

    // Add clustering keys
    for (int i = 1; i <= ckCount; i++) {
      builder.addColumn("ck" + i, DataType.TEXT);
      builder.addClusteringKey("ck" + i);
    }

    return builder.build();
  }

  private List<String> createProjectedColumns() {
    List<String> columns = new ArrayList<>();
    columns.add("pk1");
    columns.add("ck1");
    return columns;
  }

  @Test
  void validate_withValidExportOptionsForSinglePkCk_ShouldNotThrowException()
      throws ExportOptionsValidationException {

    Key partitionKey = Key.newBuilder().add(IntColumn.of("pk1", 1)).build();

    ExportOptions exportOptions =
        ExportOptions.builder("test", "sample", partitionKey, FileFormat.JSON)
            .projectionColumns(projectedColumns)
            .scanRange(new ScanRange(null, null, false, false))
            .build();

    ExportOptionsValidator.validate(exportOptions, singlePkCkMetadata);
  }

  @Test
  void validate_withValidExportOptionsForMultiplePkCk_ShouldNotThrowException()
      throws ExportOptionsValidationException {

    Key partitionKey =
        Key.newBuilder().add(IntColumn.of("pk1", 1)).add(IntColumn.of("pk2", 2)).build();

    ExportOptions exportOptions =
        ExportOptions.builder("test", "sample", partitionKey, FileFormat.JSON)
            .projectionColumns(projectedColumns)
            .scanRange(new ScanRange(null, null, false, false))
            .build();

    ExportOptionsValidator.validate(exportOptions, multiplePkCkMetadata);
  }

  @Test
  void validate_withIncompletePartitionKeyForSinglePk_ShouldThrowException() {
    Key incompletePartitionKey = Key.newBuilder().build();

    ExportOptions exportOptions =
        ExportOptions.builder("test", "sample", incompletePartitionKey, FileFormat.JSON).build();

    assertThatThrownBy(() -> ExportOptionsValidator.validate(exportOptions, singlePkCkMetadata))
        .isInstanceOf(ExportOptionsValidationException.class)
        .hasMessage(
            String.format(INCOMPLETE_PARTITION_KEY, singlePkCkMetadata.getPartitionKeyNames()));
  }

  @Test
  void validate_withIncompletePartitionKeyForMultiplePks_ShouldThrowException() {
    Key incompletePartitionKey = Key.newBuilder().add(IntColumn.of("pk1", 1)).build();

    ExportOptions exportOptions =
        ExportOptions.builder("test", "sample", incompletePartitionKey, FileFormat.JSON).build();

    assertThatThrownBy(() -> ExportOptionsValidator.validate(exportOptions, multiplePkCkMetadata))
        .isInstanceOf(ExportOptionsValidationException.class)
        .hasMessage(
            String.format(INCOMPLETE_PARTITION_KEY, multiplePkCkMetadata.getPartitionKeyNames()));
  }

  @Test
  void validate_withInvalidProjectionColumn_ShouldThrowException() {
    ExportOptions exportOptions =
        ExportOptions.builder(
                "test",
                "sample",
                Key.newBuilder().add(IntColumn.of("pk1", 1)).build(),
                FileFormat.JSON)
            .projectionColumns(Collections.singletonList("invalid_column"))
            .build();

    assertThatThrownBy(() -> ExportOptionsValidator.validate(exportOptions, singlePkCkMetadata))
        .isInstanceOf(ExportOptionsValidationException.class)
        .hasMessage(String.format(INVALID_PROJECTION, "invalid_column"));
  }

  @Test
  void validate_withInvalidClusteringKeyInScanRange_ShouldThrowException() {
    ScanRange scanRange =
        new ScanRange(
            Key.newBuilder().add(TextColumn.of("invalid_ck", "value")).build(),
            Key.newBuilder().add(TextColumn.of("ck1", "value")).build(),
            false,
            false);

    ExportOptions exportOptions =
        ExportOptions.builder("test", "sample", createValidPartitionKey(), FileFormat.JSON)
            .scanRange(scanRange)
            .build();

    assertThatThrownBy(() -> ExportOptionsValidator.validate(exportOptions, singlePkCkMetadata))
        .isInstanceOf(ExportOptionsValidationException.class)
        .hasMessage(String.format(CLUSTERING_KEY_ORDER_MISMATCH, "[ck1]"));
  }

  @Test
  void validate_withInvalidPartitionKeyOrder_ShouldThrowException() {
    // Partition key names are expected to be "pk1", "pk2"
    LinkedHashSet<String> partitionKeyNames = new LinkedHashSet<>();
    partitionKeyNames.add("pk1");
    partitionKeyNames.add("pk2");

    // Create a partition key with reversed order, expecting an error
    Key invalidPartitionKey =
        Key.newBuilder()
            .add(IntColumn.of("pk2", 2)) // Incorrect order
            .add(IntColumn.of("pk1", 1)) // Incorrect order
            .build();

    ExportOptions exportOptions =
        ExportOptions.builder("test", "sample", invalidPartitionKey, FileFormat.JSON)
            .projectionColumns(projectedColumns)
            .scanRange(new ScanRange(null, null, false, false))
            .build();

    // Verify that the validator throws the correct exception
    assertThatThrownBy(() -> ExportOptionsValidator.validate(exportOptions, multiplePkCkMetadata))
        .isInstanceOf(ExportOptionsValidationException.class)
        .hasMessage(String.format(PARTITION_KEY_ORDER_MISMATCH, partitionKeyNames));
  }

  private Key createValidPartitionKey() {
    return Key.newBuilder().add(IntColumn.of("pk1", 1)).build();
  }
}
