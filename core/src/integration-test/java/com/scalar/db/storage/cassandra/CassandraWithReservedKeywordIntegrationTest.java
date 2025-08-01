package com.scalar.db.storage.cassandra;

import com.scalar.db.api.DistributedStorageWithReservedKeywordIntegrationTestBase;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class CassandraWithReservedKeywordIntegrationTest
    extends DistributedStorageWithReservedKeywordIntegrationTestBase {

  @Override
  protected String getNamespace() {
    // a reserved keyword in Cassandra
    return "keyspace";
  }

  @Override
  protected String getTableName() {
    // a reserved keyword in Cassandra
    return "table";
  }

  @Override
  protected String getColumnName1() {
    // a reserved keyword in Cassandra
    return "from";
  }

  @Override
  protected String getColumnName2() {
    // a reserved keyword in Cassandra
    return "to";
  }

  @Override
  protected String getColumnName3() {
    // a reserved keyword in Cassandra
    return "one";
  }

  @Override
  protected String getColumnName4() {
    // a reserved keyword in Cassandra
    return "select";
  }

  @Override
  protected String getColumnName5() {
    // a reserved keyword in Cassandra
    return "password";
  }

  @Override
  protected Properties getProperties(String testName) {
    return CassandraEnv.getProperties(testName);
  }

  @Disabled(
      "In Cassandra, if an IS NULL condition is specified for a column in a non-existing record, "
          + "the condition is considered satisfied. This behavior differs from that of other adapters")
  @Override
  @Test
  public void put_withPutIfIsNullWhenRecordDoesNotExist_shouldThrowNoMutationException() {}
}
