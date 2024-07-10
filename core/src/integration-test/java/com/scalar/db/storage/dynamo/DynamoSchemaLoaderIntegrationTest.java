package com.scalar.db.storage.dynamo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitIntegrationTestUtils;
import com.scalar.db.util.AdminTestUtils;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class DynamoSchemaLoaderIntegrationTest extends SchemaLoaderIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = DynamoEnv.getProperties(testName);

    // Add testName as a coordinator schema suffix
    ConsensusCommitIntegrationTestUtils.addSuffixToCoordinatorNamespace(properties, testName);

    return properties;
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new DynamoAdminTestUtils(getProperties(testName));
  }

  @Override
  protected List<String> getCommandArgsForCreation(Path configFilePath, Path schemaFilePath)
      throws Exception {
    return ImmutableList.<String>builder()
        .addAll(super.getCommandArgsForCreation(configFilePath, schemaFilePath))
        .add("--no-scaling")
        .add("--no-backup")
        .build();
  }

  @Override
  protected List<String> getCommandArgsForReparation(Path configFilePath, Path schemaFilePath) {
    return ImmutableList.<String>builder()
        .addAll(super.getCommandArgsForReparation(configFilePath, schemaFilePath))
        .add("--no-scaling")
        .add("--no-backup")
        .build();
  }

  @Override
  protected List<String> getCommandArgsForUpgrade(Path configFilePath) {
    return ImmutableList.<String>builder()
        .addAll(super.getCommandArgsForUpgrade(configFilePath))
        .add("--no-backup")
        .build();
  }

  @Override
  protected Map<String, String> storageOption() {
    return ImmutableMap.<String, String>builder()
        .put(DynamoAdmin.NO_BACKUP, "true")
        .put(DynamoAdmin.NO_SCALING, "true")
        .build();
  }
}
