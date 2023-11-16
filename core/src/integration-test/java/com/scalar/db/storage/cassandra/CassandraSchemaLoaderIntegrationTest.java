package com.scalar.db.storage.cassandra;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class CassandraSchemaLoaderIntegrationTest extends SchemaLoaderIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return CassandraEnv.getProperties(testName);
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new CassandraAdminTestUtils(getProperties(testName));
  }

  @Override
  protected List<String> getCommandArgsForCreation(Path configFilePath, Path schemaFilePath)
      throws Exception {
    return ImmutableList.<String>builder()
        .addAll(super.getCommandArgsForCreation(configFilePath, schemaFilePath))
        .add("--replication-factor=1")
        .build();
  }

  @Override
  protected List<String> getCommandArgsForReparation(Path configFilePath, Path schemaFilePath) {
    return ImmutableList.<String>builder()
        .addAll(super.getCommandArgsForReparation(configFilePath, schemaFilePath))
        .add("--replication-factor=1")
        .build();
  }

  @Override
  protected void waitForCreationIfNecessary() {
    // In some of the tests, we modify metadata in one Cassandra cluster session (via the
    // Schema Loader) and verify if such metadata were updated by using another session (via the
    // CassandraAdminTestUtils). But it takes some time for metadata change to be propagated from
    // one session to the other, so we need to wait
    Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
  }

  @Override
  protected List<String> getCommandArgsForUpgrade(Path configFilePath) {
    return ImmutableList.<String>builder()
        .addAll(super.getCommandArgsForUpgrade(configFilePath))
        .add("--replication-factor=1")
        .build();
  }
}
