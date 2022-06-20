package com.scalar.db.server;

import static com.scalar.db.config.ConfigUtils.getInt;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Properties;
import javax.annotation.concurrent.Immutable;

@SuppressFBWarnings("JCIP_FIELD_ISNT_FINAL_IN_IMMUTABLE_CLASS")
@Immutable
public class ServerConfig {

  public static final String PREFIX = "scalar.db.server.";
  public static final String PORT = PREFIX + "port";
  public static final String PROMETHEUS_EXPORTER_PORT = PREFIX + "prometheus_exporter_port";

  public static final int DEFAULT_PORT = 60051;
  public static final int DEFAULT_PROMETHEUS_EXPORTER_PORT = 8080;

  private final Properties props;
  private int port;
  private int prometheusExporterPort;

  public ServerConfig(File propertiesFile) throws IOException {
    try (FileInputStream stream = new FileInputStream(propertiesFile)) {
      props = new Properties();
      props.load(stream);
    }
    load();
  }

  public ServerConfig(InputStream stream) throws IOException {
    props = new Properties();
    props.load(stream);
    load();
  }

  public ServerConfig(Properties properties) {
    props = new Properties();
    props.putAll(properties);
    load();
  }

  public ServerConfig(Path propertiesPath) throws IOException {
    this(propertiesPath.toFile());
  }

  public Properties getProperties() {
    return props;
  }

  private void load() {
    port = getInt(getProperties(), PORT, DEFAULT_PORT);
    prometheusExporterPort =
        getInt(getProperties(), PROMETHEUS_EXPORTER_PORT, DEFAULT_PROMETHEUS_EXPORTER_PORT);
  }

  public int getPort() {
    return port;
  }

  public int getPrometheusExporterPort() {
    return prometheusExporterPort;
  }
}
