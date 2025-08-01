package com.scalar.db.api;

public abstract class DistributedStorageWithReservedKeywordIntegrationTestBase
    extends DistributedStorageIntegrationTestBase {

  private static final String TEST_NAME = "storage_reserved_kw";

  @Override
  protected String getTestName() {
    return TEST_NAME;
  }
}
