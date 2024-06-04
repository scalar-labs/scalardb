package com.scalar.db.config;

import static com.google.common.base.Preconditions.checkArgument;
import static com.scalar.db.config.ConfigUtils.getInt;
import static com.scalar.db.config.ConfigUtils.getLong;
import static com.scalar.db.config.ConfigUtils.getString;
import static com.scalar.db.config.ConfigUtils.getStringArray;

import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@SuppressFBWarnings("JCIP_FIELD_ISNT_FINAL_IN_IMMUTABLE_CLASS")
@Immutable
public class DatabaseConfig {
  private final Properties props;
  private ImmutableList<String> contactPoints;
  private int contactPort;
  @Nullable private String username;
  @Nullable private String password;
  private String storage;
  private String transactionManager;
  private long metadataCacheExpirationTimeSecs;
  private long activeTransactionManagementExpirationTimeMillis;

  public static final String PREFIX = "scalar.db.";
  public static final String CONTACT_POINTS = PREFIX + "contact_points";
  public static final String CONTACT_PORT = PREFIX + "contact_port";
  public static final String USERNAME = PREFIX + "username";
  public static final String PASSWORD = PREFIX + "password";
  public static final String STORAGE = PREFIX + "storage";
  public static final String TRANSACTION_MANAGER = PREFIX + "transaction_manager";
  public static final String METADATA_CACHE_EXPIRATION_TIME_SECS =
      PREFIX + "metadata.cache_expiration_time_secs";
  public static final String ACTIVE_TRANSACTION_MANAGEMENT_EXPIRATION_TIME_MILLIS =
      PREFIX + "active_transaction_management.expiration_time_millis";

  public DatabaseConfig(File propertiesFile) throws IOException {
    try (FileInputStream stream = new FileInputStream(propertiesFile)) {
      props = new Properties();
      props.load(stream);
    }
    load();
  }

  public DatabaseConfig(InputStream stream) throws IOException {
    props = new Properties();
    props.load(stream);
    load();
  }

  public DatabaseConfig(Properties properties) {
    props = new Properties();
    props.putAll(properties);
    load();
  }

  public DatabaseConfig(Path propertiesPath) throws IOException {
    this(propertiesPath.toFile());
  }

  // For the SpotBugs warning CT_CONSTRUCTOR_THROW
  @Override
  protected final void finalize() {}

  public Properties getProperties() {
    Properties ret = new Properties();
    ret.putAll(props);
    return ret;
  }

  protected void load() {
    storage = getString(getProperties(), STORAGE, "cassandra");
    contactPoints =
        ImmutableList.copyOf(getStringArray(getProperties(), CONTACT_POINTS, new String[0]));
    contactPort = getInt(getProperties(), CONTACT_PORT, 0);
    checkArgument(contactPort >= 0);
    username = getString(getProperties(), USERNAME, null);
    password = getString(getProperties(), PASSWORD, null);
    transactionManager = getString(getProperties(), TRANSACTION_MANAGER, "consensus-commit");
    metadataCacheExpirationTimeSecs =
        getLong(getProperties(), METADATA_CACHE_EXPIRATION_TIME_SECS, -1);
    activeTransactionManagementExpirationTimeMillis =
        getLong(getProperties(), ACTIVE_TRANSACTION_MANAGEMENT_EXPIRATION_TIME_MILLIS, 0);
  }

  public List<String> getContactPoints() {
    return contactPoints;
  }

  public int getContactPort() {
    return contactPort;
  }

  public Optional<String> getUsername() {
    return Optional.ofNullable(username);
  }

  public Optional<String> getPassword() {
    return Optional.ofNullable(password);
  }

  public String getStorage() {
    return storage;
  }

  public String getTransactionManager() {
    return transactionManager;
  }

  public long getMetadataCacheExpirationTimeSecs() {
    return metadataCacheExpirationTimeSecs;
  }

  public long getActiveTransactionManagementExpirationTimeMillis() {
    return activeTransactionManagementExpirationTimeMillis;
  }
}
