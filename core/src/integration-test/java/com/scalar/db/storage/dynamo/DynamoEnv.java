package com.scalar.db.storage.dynamo;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.config.DatabaseConfig;
import java.util.Map;
import java.util.Properties;

public final class DynamoEnv {
  private static final String PROP_DYNAMO_ENDPOINT_OVERRIDE = "scalardb.dynamo.endpoint_override";
  private static final String PROP_DYNAMO_REGION = "scalardb.dynamo.region";
  private static final String PROP_DYNAMO_ACCESS_KEY_ID = "scalardb.dynamo.access_key_id";
  private static final String PROP_DYNAMO_SECRET_ACCESS_KEY = "scalardb.dynamo.secret_access_key";
  private static final String PROP_DYNAMO_CREATE_OPTIONS = "scalardb.dynamo.create_options";

  private static final String DEFAULT_DYNAMO_ENDPOINT_OVERRIDE = "http://localhost:8000";
  private static final String DEFAULT_DYNAMO_REGION = "us-west-2";
  private static final String DEFAULT_DYNAMO_ACCESS_KEY_ID = "fakeMyKeyId";
  private static final String DEFAULT_DYNAMO_SECRET_ACCESS_KEY = "fakeSecretAccessKey";

  private static final ImmutableMap<String, String> DEFAULT_DYNAMO_CREATE_OPTIONS =
      ImmutableMap.of(DynamoAdmin.NO_SCALING, "true", DynamoAdmin.NO_BACKUP, "true");

  private DynamoEnv() {}

  public static DynamoConfig getDynamoConfig() {
    String endpointOverride =
        System.getProperty(PROP_DYNAMO_ENDPOINT_OVERRIDE, DEFAULT_DYNAMO_ENDPOINT_OVERRIDE);
    String region = System.getProperty(PROP_DYNAMO_REGION, DEFAULT_DYNAMO_REGION);
    String accessKeyId =
        System.getProperty(PROP_DYNAMO_ACCESS_KEY_ID, DEFAULT_DYNAMO_ACCESS_KEY_ID);
    String secretAccessKey =
        System.getProperty(PROP_DYNAMO_SECRET_ACCESS_KEY, DEFAULT_DYNAMO_SECRET_ACCESS_KEY);

    Properties props = new Properties();
    if (endpointOverride != null) {
      props.setProperty(DynamoConfig.ENDPOINT_OVERRIDE, endpointOverride);
    }
    props.setProperty(DatabaseConfig.CONTACT_POINTS, region);
    props.setProperty(DatabaseConfig.USERNAME, accessKeyId);
    props.setProperty(DatabaseConfig.PASSWORD, secretAccessKey);
    props.setProperty(DatabaseConfig.STORAGE, "dynamo");
    return new DynamoConfig(props);
  }

  public static Map<String, String> getCreateOptions() {
    String createOptionsString = System.getProperty(PROP_DYNAMO_CREATE_OPTIONS);
    if (createOptionsString == null) {
      return DEFAULT_DYNAMO_CREATE_OPTIONS;
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
