package com.scalar.db.dataloader.core.dataexport.validation;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.dataloader.core.FileFormat;
import com.scalar.db.dataloader.core.ScanRange;
import com.scalar.db.dataloader.core.dataexport.ExportOptions;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ExportOptionsValidatorTest {

  TableMetadata mockMetadata;
  List<String> projectedColumns;

  @BeforeEach
  void setup() {
    mockMetadata =
        TableMetadata.newBuilder()
            .addColumn("id", DataType.INT)
            .addColumn("name", DataType.TEXT)
            .addColumn("email", DataType.TEXT)
            .addColumn("department", DataType.TEXT)
            .addPartitionKey("id")
            .addClusteringKey("department")
            .build();
    projectedColumns = new ArrayList<>();
    projectedColumns.add("id");
    projectedColumns.add("name");
    projectedColumns.add("email");
    projectedColumns.add("department");
  }

  @Test
  void validate_withValidExportOptions_ShouldNotThrowException()
      throws ExportOptionsValidationException {
    ExportOptions exportOptions =
        ExportOptions.builder(
                "test",
                "sample",
                Key.newBuilder().add(IntColumn.of("id", 1)).build(),
                FileFormat.JSON)
            .sortOrders(Collections.emptyList())
            .scanRange(new ScanRange(null, null, false, false))
            .projectionColumns(projectedColumns)
            .build();
    ExportOptionsValidator.validate(exportOptions, mockMetadata);
  }

  @Test
  void validate_withInValidColumnInProjectionColumnList_ShouldThrowException() {
    ExportOptions exportOptions =
        ExportOptions.builder(
                "test",
                "sample",
                Key.newBuilder().add(IntColumn.of("id", 1)).build(),
                FileFormat.JSON)
            .sortOrders(Collections.emptyList())
            .scanRange(new ScanRange(null, null, false, false))
            .projectionColumns(Collections.singletonList("sample"))
            .build();
    assertThatThrownBy(() -> ExportOptionsValidator.validate(exportOptions, mockMetadata))
        .isInstanceOf(ExportOptionsValidationException.class)
        .hasMessage(CoreError.DATA_LOADER_INVALID_PROJECTION.buildMessage("sample"));
  }

  @Test
  void validate_withInValidSortOrderWithMultipleClusteringKeys_ShouldThrowException() {
    mockMetadata =
        TableMetadata.newBuilder()
            .addColumn("id", DataType.INT)
            .addColumn("name", DataType.TEXT)
            .addColumn("email", DataType.TEXT)
            .addColumn("department", DataType.TEXT)
            .addColumn("building", DataType.TEXT)
            .addPartitionKey("id")
            .addClusteringKey("department")
            .addClusteringKey("building")
            .build();
    ExportOptions exportOptions =
        ExportOptions.builder(
                "test",
                "sample",
                Key.newBuilder().add(IntColumn.of("id", 1)).build(),
                FileFormat.JSON)
            .sortOrders(
                Collections.singletonList(new Scan.Ordering("name", Scan.Ordering.Order.ASC)))
            .scanRange(new ScanRange(null, null, false, false))
            .projectionColumns(Collections.singletonList("id"))
            .build();
    assertThatThrownBy(() -> ExportOptionsValidator.validate(exportOptions, mockMetadata))
        .isInstanceOf(ExportOptionsValidationException.class)
        .hasMessage(CoreError.DATA_LOADER_CLUSTERING_KEY_NOT_FOUND.buildMessage("name"));
  }

  @Test
  void validate_withInValidKeyInSortRange_ShouldThrowException() {
    ExportOptions exportOptions =
        ExportOptions.builder(
                "test",
                "sample",
                Key.newBuilder().add(IntColumn.of("id", 1)).build(),
                FileFormat.JSON)
            .sortOrders(Collections.emptyList())
            .scanRange(
                new ScanRange(
                    Key.newBuilder().add(IntColumn.of("id", 1)).build(),
                    Key.newBuilder().add(IntColumn.of("id", 100)).build(),
                    false,
                    false))
            .projectionColumns(Collections.singletonList("id"))
            .build();
    assertThatThrownBy(() -> ExportOptionsValidator.validate(exportOptions, mockMetadata))
        .isInstanceOf(ExportOptionsValidationException.class)
        .hasMessage(CoreError.DATA_LOADER_CLUSTERING_KEY_NOT_FOUND.buildMessage("id"));
  }

  @Test
  void validate_withInValidEndKeyInSortRange_ShouldThrowException() {
    ExportOptions exportOptions =
        ExportOptions.builder(
                "test",
                "sample",
                Key.newBuilder().add(IntColumn.of("id", 1)).build(),
                FileFormat.JSON)
            .sortOrders(Collections.emptyList())
            .scanRange(
                new ScanRange(
                    Key.newBuilder().add(TextColumn.of("department", "sample")).build(),
                    Key.newBuilder().add(IntColumn.of("name", 100)).build(),
                    false,
                    false))
            .projectionColumns(projectedColumns)
            .build();
    assertThatThrownBy(() -> ExportOptionsValidator.validate(exportOptions, mockMetadata))
        .isInstanceOf(ExportOptionsValidationException.class)
        .hasMessage(CoreError.DATA_LOADER_CLUSTERING_KEY_NOT_FOUND.buildMessage("name"));
  }
}
