package com.scalar.db.graphql.server;

import com.google.common.base.Strings;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
class ServerConfig {
  public static final String PREFIX = "scalar.db.graphql.";
  public static final String PORT = PREFIX + "port";
  public static final String PATH = PREFIX + "path";
  public static final String NAMESPACES = PREFIX + "namespaces";
  public static final String GRAPHIQL = PREFIX + "graphiql";

  public static final int DEFAULT_PORT = 8080;
  private static final String DEFAULT_PATH = "/graphql";
  public static final boolean DEFAULT_GRAPHIQL = true;

  private static final Logger LOGGER = LoggerFactory.getLogger(ServerConfig.class);
  private final Properties props;
  private int port;
  private String path;
  private Set<String> namespaces;
  private boolean graphiql;

  public ServerConfig(File propertiesFile) throws IOException {
    this(new FileInputStream(propertiesFile));
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

  private void load() {
    port = getInt(PORT, DEFAULT_PORT);
    path = props.getProperty(PATH, DEFAULT_PATH);
    namespaces = new LinkedHashSet<>(Arrays.asList(getStringArray(NAMESPACES)));
    graphiql = getBoolean(GRAPHIQL, DEFAULT_GRAPHIQL);
  }

  private int getInt(String name, int defaultValue) {
    String value = props.getProperty(name);
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

  private String[] getStringArray(String name) {
    String value = props.getProperty(name);
    if (Strings.isNullOrEmpty(value)) {
      return new String[] {};
    }
    return value.split("\\s*,\\s*");
  }

  private boolean getBoolean(String name, boolean defaultValue) {
    String value = props.getProperty(name);
    if (Strings.isNullOrEmpty(value)) {
      return defaultValue;
    }
    return Boolean.parseBoolean(value);
  }

  public Properties getProperties() {
    return props;
  }

  public int getPort() {
    return port;
  }

  public String getPath() {
    return path;
  }

  public Set<String> getNamespaces() {
    return namespaces;
  }

  public boolean getGraphiql() { return graphiql; }
}
