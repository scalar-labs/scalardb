package com.scalar.db.api;

import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DistributedStorageCaseSensitivityIntegrationTestBase
    extends DistributedStorageIntegrationTestBase {

  private static final String TEST_NAME = "storage_case";
  private static final String NAMESPACE = "Int_test_" + TEST_NAME;
  private static final String TABLE = "Test_table";
  private static final String COL_NAME1 = "Column1";
  private static final String COL_NAME2 = "Column2";
  private static final String COL_NAME3 = "Column3";
  private static final String COL_NAME4 = "Column4";
  private static final String COL_NAME5 = "Column5";
  private static final String COL_NAME6 = "Column6";

  @Override
  protected String getTestName() {
    return TEST_NAME;
  }

  @Override
  protected String getNamespace() {
    return NAMESPACE;
  }

  @Override
  protected String getTableName() {
    return TABLE;
  }

  @Override
  protected String getColumnName1() {
    return COL_NAME1;
  }

  @Override
  protected String getColumnName2() {
    return COL_NAME2;
  }

  @Override
  protected String getColumnName3() {
    return COL_NAME3;
  }

  @Override
  protected String getColumnName4() {
    return COL_NAME4;
  }

  @Override
  protected String getColumnName5() {
    return COL_NAME5;
  }

  @Override
  protected String getColumnName6() {
    return COL_NAME6;
  }
}
