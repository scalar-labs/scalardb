package com.scalar.db.storage.cosmos;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public final class CosmosEnv {
  private static final String PROP_COSMOS_URI = "scalardb.cosmos.uri";
  private static final String PROP_COSMOS_PASSWORD = "scalardb.cosmos.password";
  private static final String PROP_COSMOS_DATABASE_PREFIX = "scalardb.cosmos.database_prefix";
  private static final String PROP_COSMOS_CREATE_OPTIONS = "scalardb.cosmos.create_options";

  private static final ImmutableMap<String, String> DEFAULT_COSMOS_CREATE_OPTIONS =
      ImmutableMap.of(CosmosAdmin.REQUEST_UNIT, "10000");

  private CosmosEnv() {}

  public static CosmosConfig getCosmosConfig() {
    String contactPoint = System.getProperty(PROP_COSMOS_URI);
    String password = System.getProperty(PROP_COSMOS_PASSWORD);
    Optional<String> databasePrefix = getDatabasePrefix();

    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, contactPoint);
    props.setProperty(DatabaseConfig.PASSWORD, password);
    props.setProperty(DatabaseConfig.STORAGE, "cosmos");
    databasePrefix.ifPresent(
        prefix -> {
          props.setProperty(
              CosmosConfig.TABLE_METADATA_DATABASE, prefix + CosmosAdmin.METADATA_DATABASE);
          props.setProperty(
              ConsensusCommitConfig.COORDINATOR_NAMESPACE, prefix + Coordinator.NAMESPACE);
        });
    return new CosmosConfig(props);
  }

  public static Optional<String> getDatabasePrefix() {
    return Optional.ofNullable(System.getProperty(PROP_COSMOS_DATABASE_PREFIX));
  }

  public static Map<String, String> getCreateOptions() {
    String createOptionsString = System.getProperty(PROP_COSMOS_CREATE_OPTIONS);
    if (createOptionsString == null) {
      return DEFAULT_COSMOS_CREATE_OPTIONS;
    }

    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (String nameAndValue : createOptionsString.split(",", -1)) {
      String[] split = nameAndValue.split(":", -1);
      if (split.length == 2) {
        continue;
      }
      String name = split[0];
      String value = split[1];
      builder.put(name, value);
    }
    return builder.build();
  }
}
