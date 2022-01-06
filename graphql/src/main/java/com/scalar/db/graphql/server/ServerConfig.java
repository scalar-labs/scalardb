package com.scalar.db.graphql.server;

import com.scalar.db.config.ConfigUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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
@SuppressFBWarnings("JCIP_FIELD_ISNT_FINAL_IN_IMMUTABLE_CLASS")
class ServerConfig {
  public static final String PREFIX = "scalar.db.graphql.";
  public static final String PORT = PREFIX + "port";
  public static final String PATH = PREFIX + "path";
  public static final String NAMESPACES = PREFIX + "namespaces";
  public static final String GRAPHIQL = PREFIX + "graphiql";

  public static final int DEFAULT_PORT = 8080;
  public static final boolean DEFAULT_GRAPHIQL = true;
  private static final String DEFAULT_PATH = "/graphql";
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerConfig.class);
  private final Properties props;
  private int port;
  private String path;
  private Set<String> namespaces;
  private boolean graphiql;

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

  private void load() {
    port = ConfigUtils.getInt(props, PORT, DEFAULT_PORT);
    path = props.getProperty(PATH, DEFAULT_PATH);
    namespaces =
        new LinkedHashSet<>(
            Arrays.asList(ConfigUtils.getStringArray(props, NAMESPACES, new String[0])));
    graphiql = ConfigUtils.getBoolean(props, GRAPHIQL, DEFAULT_GRAPHIQL);
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

  public boolean getGraphiql() {
    return graphiql;
  }
}
