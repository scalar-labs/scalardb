package com.scalar.db.server.config;

import com.google.common.base.Strings;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
@SuppressFBWarnings("JCIP_FIELD_ISNT_FINAL_IN_IMMUTABLE_CLASS")
public class ServerConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerConfig.class);

  public static final String PREFIX = "scalar.db.server.";
  public static final String PORT = PREFIX + "port";
  public static final String PROMETHEUS_HTTP_ENDPOINT_PORT =
      PREFIX + "prometheus_http_endpoint_port";

  public static final int DEFAULT_PORT = 60051;
  public static final int DEFAULT_PROMETHEUS_HTTP_ENDPOINT_PORT = 8080;

  private final Properties props;
  private int port;
  private int prometheusHttpEndpointPort;

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
    port = getInt(PORT, DEFAULT_PORT);
    prometheusHttpEndpointPort =
        getInt(PROMETHEUS_HTTP_ENDPOINT_PORT, DEFAULT_PROMETHEUS_HTTP_ENDPOINT_PORT);
  }

  private int getInt(String name, int defaultValue) {
    String value = getProperties().getProperty(name);
    if (Strings.isNullOrEmpty(value)) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException ignored) {
      LOGGER.warn(
          "the specified value of '{}' is not a number. using the default value: {}",
          name,
          defaultValue);
      return defaultValue;
    }
  }

  public int getPort() {
    return port;
  }

  public int getPrometheusHttpEndpointPort() {
    return prometheusHttpEndpointPort;
  }
}
