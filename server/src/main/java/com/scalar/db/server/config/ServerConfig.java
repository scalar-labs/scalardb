package com.scalar.db.server.config;

import com.google.common.base.Strings;
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

  public static final int DEFAULT_PORT = 60051;

  private final Properties props;
  private int port = DEFAULT_PORT;

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
    String portValue = props.getProperty(PORT);
    if (!Strings.isNullOrEmpty(portValue)) {
      try {
        port = Integer.parseInt(portValue);
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
