package com.scalar.db.storage.cassandra;

import com.scalar.db.api.DistributedStorageWithReservedKeywordIntegrationTestBase;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class CassandraWithReservedKeywordIntegrationTest
    extends DistributedStorageWithReservedKeywordIntegrationTestBase {
  @Override
  protected Map<String, String> getCreationOptions() {
    return Collections.singletonMap(CassandraAdmin.REPLICATION_FACTOR, "1");
  }

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
}
