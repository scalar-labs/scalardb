package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.ScanAll;
import com.scalar.db.api.Selection.Conjunction;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import java.sql.SQLException;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class RdbEngineOracleTest {
  @Mock private ScanAll scanAll;
  @Mock private TableMetadata metadata;
  private RdbEngineOracle rdbEngineOracle;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    rdbEngineOracle = new RdbEngineOracle();
  }

  @Test
  void createTableInternalSqlsAfterCreateTable_GivenSameClusteringOrders_ShouldNotCreateIndex() {
    // Arrange
    RdbEngineStrategy rdbEngine = new RdbEngineOracle();
    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk")
            .addClusteringKey("ck1", Order.ASC)
            .addClusteringKey("ck2", Order.DESC)
            .addColumn("pk", DataType.INT)
            .addColumn("ck1", DataType.INT)
            .addColumn("ck2", DataType.INT)
            .build();

    // Act
    String[] sqls =
        rdbEngine.createTableInternalSqlsAfterCreateTable(
            false, "myschema", "mytable", metadata, false);

    // Assert
    assertThat(sqls).hasSize(1);
    assertThat(sqls[0]).startsWith("ALTER TABLE ");
  }

  @Test
  void createTableInternalSqlsAfterCreateTable_GivenDifferentClusteringOrders_ShouldCreateIndex() {
    // Arrange
    RdbEngineStrategy rdbEngine = new RdbEngineOracle();
    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk")
            .addClusteringKey("ck1", Order.ASC)
            .addClusteringKey("ck2", Order.DESC)
            .addColumn("pk", DataType.INT)
            .addColumn("ck1", DataType.INT)
            .addColumn("ck2", DataType.INT)
            .build();

    // Act
    String[] sqls =
        rdbEngine.createTableInternalSqlsAfterCreateTable(
            true, "myschema", "mytable", metadata, false);

    // Assert
    assertThat(sqls).hasSize(2);
    assertThat(sqls[0]).startsWith("ALTER TABLE ");
    assertThat(sqls[1]).startsWith("CREATE UNIQUE INDEX ");
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
                rdbEngineOracle.throwIfCrossPartitionScanOrderingOnBlobColumnNotSupported(
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
                rdbEngineOracle.throwIfCrossPartitionScanOrderingOnBlobColumnNotSupported(
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
                rdbEngineOracle.throwIfCrossPartitionScanOrderingOnBlobColumnNotSupported(
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
                rdbEngineOracle.throwIfCrossPartitionScanOrderingOnBlobColumnNotSupported(
                    scanAll, metadata))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("blob_column1");
  }

  @Test
  public void throwIfConjunctionsColumnNotSupported_WithoutBlobCondition_ShouldNotThrowException() {
    // Arrange
    TableMetadata metadata = mock(TableMetadata.class);
    java.util.Set<Conjunction> conjunctions =
        java.util.Collections.singleton(
            Conjunction.of(
                ConditionBuilder.column("int_column").isEqualToInt(10),
                ConditionBuilder.column("text_column").isEqualToText("value")));

    when(metadata.getColumnDataType("int_column")).thenReturn(DataType.INT);
    when(metadata.getColumnDataType("text_column")).thenReturn(DataType.TEXT);

    // Act & Assert
    assertThatCode(
            () -> rdbEngineOracle.throwIfConjunctionsColumnNotSupported(conjunctions, metadata))
        .doesNotThrowAnyException();
  }

  @Test
  public void throwIfConjunctionsColumnNotSupported_WithNoConditions_ShouldNotThrowException() {
    // Arrange
    TableMetadata metadata = mock(TableMetadata.class);

    // Act & Assert
    assertThatCode(
            () ->
                rdbEngineOracle.throwIfConjunctionsColumnNotSupported(
                    java.util.Collections.emptySet(), metadata))
        .doesNotThrowAnyException();
  }

  @Test
  public void
      throwIfConjunctionsColumnNotSupported_WithMixedConditions_ShouldThrowWhenBlobPresent() {
    // Arrange
    TableMetadata metadata = mock(TableMetadata.class);
    java.util.Set<Conjunction> conjunctions =
        java.util.Collections.singleton(
            Conjunction.of(
                ConditionBuilder.column("int_column").isGreaterThanInt(100),
                ConditionBuilder.column("blob_column").isNotEqualToBlob(new byte[] {5, 6}),
                ConditionBuilder.column("text_column").isEqualToText("test")));

    when(metadata.getColumnDataType("int_column")).thenReturn(DataType.INT);
    when(metadata.getColumnDataType("blob_column")).thenReturn(DataType.BLOB);
    when(metadata.getColumnDataType("text_column")).thenReturn(DataType.TEXT);

    // Act & Assert
    assertThatThrownBy(
            () -> rdbEngineOracle.throwIfConjunctionsColumnNotSupported(conjunctions, metadata))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("blob_column");
  }

  @Test
  public void isConflict_WithSerializeAccessError_ShouldReturnTrue() {
    // Arrange
    // ORA-08177: can't serialize access for this transaction
    SQLException e = new SQLException("can't serialize access for this transaction", null, 8177);

    // Act
    boolean result = rdbEngineOracle.isConflict(e);

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void isConflict_WithDeadlockError_ShouldReturnTrue() {
    // Arrange
    // ORA-00060: deadlock detected while waiting for resource
    SQLException e = new SQLException("deadlock detected while waiting for resource", null, 60);

    // Act
    boolean result = rdbEngineOracle.isConflict(e);

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void isConflict_WithConsistentReadFailureError_ShouldReturnTrue() {
    // Arrange
    // ORA-08176: consistent read failure; rollback data not available
    SQLException e =
        new SQLException("consistent read failure; rollback data not available", null, 8176);

    // Act
    boolean result = rdbEngineOracle.isConflict(e);

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void isConflict_WithOtherError_ShouldReturnFalse() {
    // Arrange
    // ORA-00942: Table or view does not exist
    SQLException e = new SQLException("Table or view does not exist", null, 942);

    // Act
    boolean result = rdbEngineOracle.isConflict(e);

    // Assert
    assertThat(result).isFalse();
  }
}
