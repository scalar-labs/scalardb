package com.scalar.db.storage.cosmos;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.config.DatabaseConfig;
import java.util.Map;
import java.util.Properties;

public final class CosmosEnv {
  private static final String PROP_COSMOS_URI = "scalardb.cosmos.uri";
  private static final String PROP_COSMOS_PASSWORD = "scalardb.cosmos.password";
  private static final String PROP_COSMOS_CREATE_OPTIONS = "scalardb.cosmos.create_options";

  private static final ImmutableMap<String, String> DEFAULT_COSMOS_CREATE_OPTIONS =
      ImmutableMap.of(CosmosAdmin.REQUEST_UNIT, "10000");

  private CosmosEnv() {}

  public static Properties getProperties(String testName) {
    String contactPoint = System.getProperty(PROP_COSMOS_URI);
    String password = System.getProperty(PROP_COSMOS_PASSWORD);

    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, contactPoint);
    properties.setProperty(DatabaseConfig.PASSWORD, password);
    properties.setProperty(DatabaseConfig.STORAGE, "cosmos");
    properties.setProperty(DatabaseConfig.CROSS_PARTITION_SCAN, "true");
    properties.setProperty(DatabaseConfig.CROSS_PARTITION_SCAN_FILTERING, "true");
    properties.setProperty(DatabaseConfig.CROSS_PARTITION_SCAN_ORDERING, "false");

    // Add testName as a metadata database suffix
    properties.setProperty(
        DatabaseConfig.SYSTEM_NAMESPACE_NAME,
        DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME + "_" + testName);

    return properties;
  }

  public static Map<String, String> getCreationOptions() {
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
