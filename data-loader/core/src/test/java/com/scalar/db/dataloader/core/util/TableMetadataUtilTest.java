package com.scalar.db.dataloader.core.util;

import static com.scalar.db.dataloader.core.Constants.TABLE_LOOKUP_KEY_FORMAT;
import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.dataloader.core.dataimport.controlfile.ControlFileTable;
import org.junit.jupiter.api.Test;

/** Unit tests for TableMetadataUtils */
class TableMetadataUtilTest {

  private static final String NAMESPACE = "ns";
  private static final String TABLE_NAME = "table";

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
