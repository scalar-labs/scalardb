package com.scalar.db.storage.rpc;

import com.google.common.base.Strings;
import com.scalar.db.config.DatabaseConfig;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcConfig extends DatabaseConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcConfig.class);

  public static final String PREFIX = DatabaseConfig.PREFIX + "grpc.";
  public static String DEADLINE_DURATION_MILLIS = PREFIX + "deadline_duration_millis";

  public static long DEFAULT_DEADLINE_DURATION_MILLIS = 60000; // 60 seconds

  private long deadlineDurationMillis;

  public GrpcConfig(File propertiesFile) throws IOException {
    super(propertiesFile);
  }

  public GrpcConfig(InputStream stream) throws IOException {
    super(stream);
  }

  public GrpcConfig(Properties properties) {
    super(properties);
  }

  @Override
  protected void load() {
    String storage = getProperties().getProperty(DatabaseConfig.STORAGE);
    if (storage == null || !storage.equals("grpc")) {
      throw new IllegalArgumentException(DatabaseConfig.STORAGE + " should be 'grpc'");
    }

    super.load();

    deadlineDurationMillis = getLong(DEADLINE_DURATION_MILLIS, DEFAULT_DEADLINE_DURATION_MILLIS);
  }

  private long getLong(String name, long defaultValue) {
    String value = getProperties().getProperty(name);
    if (Strings.isNullOrEmpty(value)) {
      return defaultValue;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException ignored) {
      LOGGER.warn(
          "the specified value of '{}' is not a number. using the default value: {}",
          name,
          defaultValue);
      return defaultValue;
    }
  }

  public long getDeadlineDurationMillis() {
    return deadlineDurationMillis;
  }
}
