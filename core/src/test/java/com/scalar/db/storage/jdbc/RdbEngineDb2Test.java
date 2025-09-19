package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Scan;
import com.scalar.db.api.ScanAll;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class RdbEngineDb2Test {

  @Mock private ScanAll scanAll;
  @Mock private TableMetadata metadata;

  private RdbEngineDb2 rdbEngineDb2;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    rdbEngineDb2 = new RdbEngineDb2();
  }

  @Test
  public void
      throwIfCrossPartitionScanOrderingOnBlobColumnNotSupported_WithBlobOrdering_ShouldThrowException() {
    // Arrange
    Scan.Ordering blobOrdering = Scan.Ordering.asc("blob_column");
    Scan.Ordering intOrdering = Scan.Ordering.desc("int_column");

    when(scanAll.getOrderings()).thenReturn(Arrays.asList(intOrdering, blobOrdering));
    when(metadata.getColumnDataType("blob_column")).thenReturn(DataType.BLOB);
    when(metadata.getColumnDataType("int_column")).thenReturn(DataType.INT);

    // Act & Assert
    assertThatThrownBy(
            () ->
                rdbEngineDb2.throwIfCrossPartitionScanOrderingOnBlobColumnNotSupported(
                    scanAll, metadata))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("blob_column");
  }

  @Test
  public void
      throwIfCrossPartitionScanOrderingOnBlobColumnNotSupported_WithoutBlobOrdering_ShouldNotThrowException() {
    // Arrange
    Scan.Ordering intOrdering = Scan.Ordering.asc("int_column");
    Scan.Ordering textOrdering = Scan.Ordering.desc("text_column");

    when(scanAll.getOrderings()).thenReturn(Arrays.asList(intOrdering, textOrdering));
    when(metadata.getColumnDataType("int_column")).thenReturn(DataType.INT);
    when(metadata.getColumnDataType("text_column")).thenReturn(DataType.TEXT);

    // Act & Assert
    assertThatCode(
            () ->
                rdbEngineDb2.throwIfCrossPartitionScanOrderingOnBlobColumnNotSupported(
                    scanAll, metadata))
        .doesNotThrowAnyException();
  }

  @Test
  public void
      throwIfCrossPartitionScanOrderingOnBlobColumnNotSupported_WithNoOrderings_ShouldNotThrowException() {
    // Arrange
    when(scanAll.getOrderings()).thenReturn(Arrays.asList());

    // Act & Assert
    assertThatCode(
            () ->
                rdbEngineDb2.throwIfCrossPartitionScanOrderingOnBlobColumnNotSupported(
                    scanAll, metadata))
        .doesNotThrowAnyException();
  }

  @Test
  public void
      throwIfCrossPartitionScanOrderingOnBlobColumnNotSupported_WithMultipleBlobOrderings_ShouldThrowForFirst() {
    // Arrange
    Scan.Ordering blobOrdering1 = Scan.Ordering.asc("blob_column1");
    Scan.Ordering blobOrdering2 = Scan.Ordering.desc("blob_column2");

    when(scanAll.getOrderings()).thenReturn(Arrays.asList(blobOrdering1, blobOrdering2));
    when(metadata.getColumnDataType("blob_column1")).thenReturn(DataType.BLOB);
    when(metadata.getColumnDataType("blob_column2")).thenReturn(DataType.BLOB);

    // Act & Assert
    assertThatThrownBy(
            () ->
                rdbEngineDb2.throwIfCrossPartitionScanOrderingOnBlobColumnNotSupported(
                    scanAll, metadata))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("blob_column1");
  }
}
