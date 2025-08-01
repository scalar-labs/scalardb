package com.scalar.db.api;

public abstract class DistributedStorageAdminCaseSensitivityIntegrationTestBase
    extends DistributedStorageAdminIntegrationTestBase {

  private static final String TEST_NAME = "storage_admin_case";
  private static final String NAMESPACE1 = "Int_test_" + TEST_NAME + "1";
  private static final String NAMESPACE2 = "Int_test_" + TEST_NAME + "2";
  private static final String NAMESPACE3 = "Int_test_" + TEST_NAME + "3";
  private static final String TABLE1 = "Test_table1";
  private static final String TABLE2 = "Test_table2";
  private static final String TABLE3 = "Test_table3";
  private static final String TABLE4 = "Test_table4";
  private static final String COL_NAME1 = "Column1";
  private static final String COL_NAME2 = "Column2";
  private static final String COL_NAME3 = "Column3";
  private static final String COL_NAME4 = "Column4";
  private static final String COL_NAME5 = "Column5";
  private static final String COL_NAME6 = "Column6";
  private static final String COL_NAME7 = "Column7";
  private static final String COL_NAME8 = "Column8";
  private static final String COL_NAME9 = "Column9";
  private static final String COL_NAME10 = "Column10";
  private static final String COL_NAME11 = "Column11";
  private static final String COL_NAME12 = "Column12";
  private static final String COL_NAME13 = "Column13";
  private static final String COL_NAME14 = "Column14";
  private static final String COL_NAME15 = "Column15";

  @Override
  protected String getTestName() {
    return TEST_NAME;
  }

  @Override
  protected String getNamespace1() {
    return NAMESPACE1;
  }

  @Override
  protected String getNamespace2() {
    return NAMESPACE2;
  }

  @Override
  protected String getNamespace3() {
    return NAMESPACE3;
  }

  @Override
  protected String getTable1() {
    return TABLE1;
  }

  @Override
  protected String getTable2() {
    return TABLE2;
  }

  @Override
  protected String getTable3() {
    return TABLE3;
  }

  @Override
  protected String getTable4() {
    return TABLE4;
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

  @Override
  protected String getColumnName7() {
    return COL_NAME7;
  }

  @Override
  protected String getColumnName8() {
    return COL_NAME8;
  }

  @Override
  protected String getColumnName9() {
    return COL_NAME9;
  }

  @Override
  protected String getColumnName10() {
    return COL_NAME10;
  }

  @Override
  protected String getColumnName11() {
    return COL_NAME11;
  }

  @Override
  protected String getColumnName12() {
    return COL_NAME12;
  }

  @Override
  protected String getColumnName13() {
    return COL_NAME13;
  }

  @Override
  protected String getColumnName14() {
    return COL_NAME14;
  }

  @Override
  protected String getColumnName15() {
    return COL_NAME15;
  }
}
