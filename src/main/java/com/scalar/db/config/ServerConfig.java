package com.scalar.db.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerConfig.class);

  public static final String PREFIX = "scalar.db.server.";
  public static final String PORT = PREFIX + "port";

  private final Properties props;
  private int port = 60051;

  public ServerConfig(File propertiesFile) throws IOException {
    this(new FileInputStream(propertiesFile));
  }

  public ServerConfig(InputStream stream) throws IOException {
    props = new Properties();
    props.load(stream);
    load();
  }

  public ServerConfig(Properties properties) {
    props = new Properties(properties);
    load();
  }

  public Properties getProperties() {
    return props;
  }

  private void load() {
    if (props.contains(PORT)) {
      try {
        port = Integer.parseInt(props.getProperty(PORT));
      } catch (NumberFormatException e) {
        LOGGER.warn(
            "the specified value of '{}' is not a number. using the default value: {}", PORT, port);
      }
    }
  }

  public int getPort() {
    return port;
  }
}
