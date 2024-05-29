package com.scalar.db.dataloader.core.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import com.scalar.db.transaction.consensuscommit.Attribute;
import java.util.*;
import org.junit.jupiter.api.Test;

/** Unit tests for TableMetadataUtils */
class TableMetadataUtilsTest {

  private static final String NAMESPACE = "ns";
  private static final String TABLE_NAME = "table";

  @Test
  void isMetadataColumn_IsMetaDataColumn_ShouldReturnTrue() {
    boolean isMetadataColumn =
        TableMetadataUtils.isMetadataColumn(
            Attribute.ID, TableMetadataUtils.getMetadataColumns(), new HashSet<>());
    assertThat(isMetadataColumn).isTrue();
  }

  @Test
  void isMetadataColumn_IsNotMetadataColumn_ShouldReturnFalse() {
    boolean isMetadataColumn =
        TableMetadataUtils.isMetadataColumn(
            "columnName", TableMetadataUtils.getMetadataColumns(), new HashSet<>());
    assertThat(isMetadataColumn).isFalse();
  }

  @Test
  void isMetadataColumn_IsBeforePrefixColumn_ShouldReturnTrue() {
    boolean isMetadataColumn =
        TableMetadataUtils.isMetadataColumn(
            Attribute.BEFORE_PREFIX + "columnName",
            TableMetadataUtils.getMetadataColumns(),
            new HashSet<>());
    assertThat(isMetadataColumn).isTrue();
  }

  @Test
  void isMetadataColumn_IsNotBeforePrefixColumn_ShouldReturnFalse() {
    Set<String> columnNames = new HashSet<>();
    columnNames.add("before_before_testing");
    boolean isMetadataColumn =
        TableMetadataUtils.isMetadataColumn(
            "before_testing", TableMetadataUtils.getMetadataColumns(), columnNames);
    assertThat(isMetadataColumn).isFalse();
  }

  @Test
  void getMetadataColumns_NoArgs_ShouldReturnSet() {
    Set<String> columns =
        new HashSet<>(
            Arrays.asList(
                Attribute.ID,
                Attribute.STATE,
                Attribute.VERSION,
                Attribute.PREPARED_AT,
                Attribute.COMMITTED_AT,
                Attribute.BEFORE_ID,
                Attribute.BEFORE_STATE,
                Attribute.BEFORE_VERSION,
                Attribute.BEFORE_PREPARED_AT,
                Attribute.BEFORE_COMMITTED_AT));
    Set<String> metadataColumns = TableMetadataUtils.getMetadataColumns();
    assertThat(metadataColumns).containsExactlyInAnyOrder(columns.toArray(new String[0]));
  }

  @Test
  void getTableLookupKey_ValidStringArgs_ShouldReturnLookupKey() {
    String actual = TableMetadataUtils.getTableLookupKey(NAMESPACE, TABLE_NAME);
    String expected =
        String.format(TableMetadataUtils.TABLE_LOOKUP_KEY_FORMAT, NAMESPACE, TABLE_NAME);
    assertThat(actual).isEqualTo(expected);
  }

  // Test the isMetadataColumn(String columnName, TableMetadata tableMetadata) method
  @Test
  void isMetadataColumn_WithTableMetadata_IsMetaDataColumn_ShouldReturnTrue() {
    TableMetadata tableMetadata = mock(TableMetadata.class);
    boolean isMetadataColumn = TableMetadataUtils.isMetadataColumn(Attribute.ID, tableMetadata);
    assertThat(isMetadataColumn).isTrue();
  }

  @Test
  void isMetadataColumn_WithTableMetadata_IsNotMetadataColumn_ShouldReturnFalse() {
    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(tableMetadata.getColumnNames()).thenReturn(new LinkedHashSet<>());
    boolean isMetadataColumn = TableMetadataUtils.isMetadataColumn("columnName", tableMetadata);
    assertThat(isMetadataColumn).isFalse();
  }

  @Test
  void isMetadataColumn_WithTableMetadata_IsBeforePrefixColumn_ShouldReturnTrue() {
    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(tableMetadata.getColumnNames()).thenReturn(new LinkedHashSet<>());
    boolean isMetadataColumn =
        TableMetadataUtils.isMetadataColumn(Attribute.BEFORE_PREFIX + "columnName", tableMetadata);
    assertThat(isMetadataColumn).isTrue();
  }

  @Test
  void isMetadataColumn_WithTableMetadata_IsNotBeforePrefixColumn_ShouldReturnFalse() {
    TableMetadata tableMetadata = mock(TableMetadata.class);
    LinkedHashSet<String> columnNames = new LinkedHashSet<>();
    columnNames.add("before_before_testing");
    when(tableMetadata.getColumnNames()).thenReturn(columnNames);
    boolean isMetadataColumn = TableMetadataUtils.isMetadataColumn("before_testing", tableMetadata);
    assertThat(isMetadataColumn).isFalse();
  }

  // Test the extractColumnDataTypes(TableMetadata tableMetadata) method
  @Test
  void extractColumnDataTypes_ValidTableMetadata_ShouldReturnColumnDataTypesMap() {
    TableMetadata tableMetadata = mock(TableMetadata.class);
    LinkedHashSet<String> columnNames = new LinkedHashSet<>();
    columnNames.add("column1");
    columnNames.add("column2");
    when(tableMetadata.getColumnNames()).thenReturn(columnNames);
    when(tableMetadata.getColumnDataType("column1")).thenReturn(DataType.TEXT);
    when(tableMetadata.getColumnDataType("column2")).thenReturn(DataType.INT);
    Map<String, DataType> columnDataTypes =
        TableMetadataUtils.extractColumnDataTypes(tableMetadata);
    assertThat(columnDataTypes).containsEntry("column1", DataType.TEXT);
    assertThat(columnDataTypes).containsEntry("column2", DataType.INT);
  }

  // Test the populateProjectionsWithMetadata(TableMetadata tableMetadata, List<String> projections)
  // method
  @Test
  void
      populateProjectionsWithMetadata_ValidTableMetadataAndProjections_ShouldReturnProjectionsWithMetadata() {
    TableMetadata tableMetadata = mock(TableMetadata.class);
    LinkedHashSet<String> partitionKeyNames = new LinkedHashSet<>();
    partitionKeyNames.add("partitionKey");
    LinkedHashSet<String> clusteringKeyNames = new LinkedHashSet<>();
    clusteringKeyNames.add("clusteringKey");
    when(tableMetadata.getPartitionKeyNames()).thenReturn(partitionKeyNames);
    when(tableMetadata.getClusteringKeyNames()).thenReturn(clusteringKeyNames);

    List<String> projections = Arrays.asList("column1", "column2");
    List<String> projectionMetadata =
        TableMetadataUtils.populateProjectionsWithMetadata(tableMetadata, projections);
    assertThat(projectionMetadata)
        .containsExactlyInAnyOrder(
            "column1",
            Attribute.BEFORE_PREFIX + "column1",
            "column2",
            Attribute.BEFORE_PREFIX + "column2",
            Attribute.ID,
            Attribute.STATE,
            Attribute.VERSION,
            Attribute.PREPARED_AT,
            Attribute.COMMITTED_AT,
            Attribute.BEFORE_ID,
            Attribute.BEFORE_STATE,
            Attribute.BEFORE_VERSION,
            Attribute.BEFORE_PREPARED_AT,
            Attribute.BEFORE_COMMITTED_AT);
  }
}
