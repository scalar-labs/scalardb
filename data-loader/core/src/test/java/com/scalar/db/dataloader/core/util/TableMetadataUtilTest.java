package com.scalar.db.dataloader.core.util;

import static com.scalar.db.dataloader.core.Constants.TABLE_LOOKUP_KEY_FORMAT;
import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.dataloader.core.dataimport.controlfile.ControlFileTable;
import com.scalar.db.transaction.consensuscommit.Attribute;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Unit tests for TableMetadataUtils */
class TableMetadataUtilTest {

  private static final String NAMESPACE = "ns";
  private static final String TABLE_NAME = "table";

  @Test
  void isMetadataColumn_IsMetaDataColumn_ShouldReturnTrue() {
    boolean isMetadataColumn =
        TableMetadataUtil.isMetadataColumn(
            Attribute.ID, TableMetadataUtil.getMetadataColumns(), new HashSet<>());
    assertThat(isMetadataColumn).isTrue();
  }

  @Test
  void isMetadataColumn_IsNotMetadataColumn_ShouldReturnFalse() {
    boolean isMetadataColumn =
        TableMetadataUtil.isMetadataColumn(
            "columnName", TableMetadataUtil.getMetadataColumns(), new HashSet<>());
    assertThat(isMetadataColumn).isFalse();
  }

  @Test
  void isMetadataColumn_IsBeforePrefixColumn_ShouldReturnTrue() {
    boolean isMetadataColumn =
        TableMetadataUtil.isMetadataColumn(
            Attribute.BEFORE_PREFIX + "columnName",
            TableMetadataUtil.getMetadataColumns(),
            new HashSet<>());
    assertThat(isMetadataColumn).isTrue();
  }

  @Test
  void isMetadataColumn_IsNotBeforePrefixColumn_ShouldReturnFalse() {
    Set<String> columnNames = new HashSet<>();
    columnNames.add("before_before_testing");
    boolean isMetadataColumn =
        TableMetadataUtil.isMetadataColumn(
            "before_testing", TableMetadataUtil.getMetadataColumns(), columnNames);
    assertThat(isMetadataColumn).isFalse();
  }

  @Test
  void getMetadataColumns_NoArgs_ShouldReturnSet() {

    Set<String> columns = new HashSet<>();
    columns.add(Attribute.ID);
    columns.add(Attribute.STATE);
    columns.add(Attribute.VERSION);
    columns.add(Attribute.PREPARED_AT);
    columns.add(Attribute.COMMITTED_AT);
    columns.add(Attribute.BEFORE_ID);
    columns.add(Attribute.BEFORE_STATE);
    columns.add(Attribute.BEFORE_VERSION);
    columns.add(Attribute.BEFORE_PREPARED_AT);
    columns.add(Attribute.BEFORE_COMMITTED_AT);

    Set<String> metadataColumns = TableMetadataUtil.getMetadataColumns();
    assertThat(metadataColumns).containsExactlyInAnyOrder(columns.toArray(new String[0]));
  }

  @Test
  void getTableLookupKey_ValidStringArgs_ShouldReturnLookupKey() {
    String actual = TableMetadataUtil.getTableLookupKey(NAMESPACE, TABLE_NAME);
    String expected = String.format(TABLE_LOOKUP_KEY_FORMAT, NAMESPACE, TABLE_NAME);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  void getTableLookupKey_ValidControlFileArg_ShouldReturnLookupKey() {
    ControlFileTable controlFileTable = new ControlFileTable(NAMESPACE, TABLE_NAME);
    String actual = TableMetadataUtil.getTableLookupKey(controlFileTable);
    String expected = String.format(TABLE_LOOKUP_KEY_FORMAT, NAMESPACE, TABLE_NAME);
    assertThat(actual).isEqualTo(expected);
  }
}
