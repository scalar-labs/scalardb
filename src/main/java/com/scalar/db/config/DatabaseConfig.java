package com.scalar.db.config;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Strings;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Isolation;
import com.scalar.db.storage.cassandra.Cassandra;
import com.scalar.db.storage.cosmos.Cosmos;
import com.scalar.db.storage.dynamo.Dynamo;
import com.scalar.db.transaction.consensuscommit.SerializableStrategy;
import com.scalar.db.storage.jdbc.JDBC;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import javax.annotation.concurrent.Immutable;

@Immutable
public class DatabaseConfig {
  protected final Properties props;
  private List<String> contactPoints;
  private int contactPort;
  private String username;
  private String password;
  private Class<? extends DistributedStorage> storageClass;
  private Optional<String> namespacePrefix;
  private Isolation isolation = Isolation.SNAPSHOT;
  private SerializableStrategy strategy = SerializableStrategy.EXTRA_READ;
  public static final String PREFIX = "scalar.db.";
  public static final String CONTACT_POINTS = PREFIX + "contact_points";
  public static final String CONTACT_PORT = PREFIX + "contact_port";
  public static final String USERNAME = PREFIX + "username";
  public static final String PASSWORD = PREFIX + "password";
  public static final String STORAGE = PREFIX + "storage";
  public static final String NAMESPACE_PREFIX = PREFIX + "namespace_prefix";
  public static final String ISOLATION_LEVEL = PREFIX + "isolation_level";
  public static final String CONSENSUSCOMMIT_PREFIX = PREFIX + "consensuscommit.";
  public static final String SERIALIZABLE_STRATEGY =
      CONSENSUSCOMMIT_PREFIX + "serializable_strategy";

  public DatabaseConfig(File propertiesFile) throws IOException {
    this(new FileInputStream(propertiesFile));
  }

  public DatabaseConfig(InputStream stream) throws IOException {
    props = new Properties();
    props.load(stream);
    load();
  }

  public DatabaseConfig(Properties properties) {
    props = new Properties(properties);
    load();
  }

  protected void load() {
    checkNotNull(props.getProperty(CONTACT_POINTS));
    checkNotNull(props.getProperty(USERNAME));
    checkNotNull(props.getProperty(PASSWORD));

    contactPoints = Arrays.asList(props.getProperty(CONTACT_POINTS).split(","));
    if (props.getProperty(CONTACT_PORT) == null) {
      contactPort = 0;
    } else {
      contactPort = Integer.parseInt(props.getProperty(CONTACT_PORT));
      checkArgument(contactPort > 0);
    }
    username = props.getProperty(USERNAME);
    password = props.getProperty(PASSWORD);

    if (props.getProperty(STORAGE) == null) {
      storageClass = Cassandra.class;
    } else {
      switch (props.getProperty(STORAGE).toLowerCase()) {
        case "cassandra":
          storageClass = Cassandra.class;
          break;
        case "cosmos":
          storageClass = Cosmos.class;
          break;
        case "dynamo":
          storageClass = Dynamo.class;
          break;
        case "jdbc":
          storageClass = JDBC.class;
          break;
        default:
          throw new IllegalArgumentException(props.getProperty(STORAGE) + " isn't supported");
      }
    }

    if (Strings.isNullOrEmpty(props.getProperty(NAMESPACE_PREFIX))) {
      namespacePrefix = Optional.empty();
    } else {
      namespacePrefix = Optional.of(props.getProperty(NAMESPACE_PREFIX) + "_");
    }

    if (!Strings.isNullOrEmpty(props.getProperty(ISOLATION_LEVEL))) {
      isolation = Isolation.valueOf(props.getProperty(ISOLATION_LEVEL).toUpperCase());
    }

    if (!Strings.isNullOrEmpty(props.getProperty(SERIALIZABLE_STRATEGY))) {
      strategy =
          SerializableStrategy.valueOf(props.getProperty(SERIALIZABLE_STRATEGY).toUpperCase());
    }
  }

  public List<String> getContactPoints() {
    return contactPoints;
  }

  public int getContactPort() {
    return contactPort;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public Class<? extends DistributedStorage> getStorageClass() {
    return storageClass;
  }

  public Optional<String> getNamespacePrefix() {
    return namespacePrefix;
  }

  public Isolation getIsolation() {
    return isolation;
  }

  public com.scalar.db.api.SerializableStrategy getSerializableStrategy() {
    return strategy;
  }
}
