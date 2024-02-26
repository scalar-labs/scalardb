package com.scalar.db.config;

import static com.google.common.base.Preconditions.checkArgument;
import static com.scalar.db.config.ConfigUtils.getBoolean;
import static com.scalar.db.config.ConfigUtils.getInt;
import static com.scalar.db.config.ConfigUtils.getLong;
import static com.scalar.db.config.ConfigUtils.getString;
import static com.scalar.db.config.ConfigUtils.getStringArray;

import com.google.common.collect.ImmutableList;
import com.scalar.db.common.error.CoreError;
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
  @Nullable private String defaultNamespaceName;
  private boolean crossPartitionScanEnabled;
  private boolean crossPartitionScanFilteringEnabled;
  private boolean crossPartitionScanOrderingEnabled;
  private boolean coordinatorGroupCommitEnabled;
  private String systemNamespaceName;

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
  public static final String DEFAULT_NAMESPACE_NAME = PREFIX + "default_namespace_name";
  public static final String SCAN_PREFIX = PREFIX + "cross_partition_scan.";
  public static final String CROSS_PARTITION_SCAN = SCAN_PREFIX + "enabled";
  public static final String CROSS_PARTITION_SCAN_FILTERING = SCAN_PREFIX + "filtering.enabled";
  public static final String CROSS_PARTITION_SCAN_ORDERING = SCAN_PREFIX + "ordering.enabled";
  public static final String SYSTEM_NAMESPACE_NAME = PREFIX + "system_namespace_name";
  // TODO: Move this into ConsensusCommitConfig
  public static final String COORDINATOR_GROUP_COMMIT_PREFIX = PREFIX + "coordinator.group_commit.";
  public static final String COORDINATOR_GROUP_COMMIT_ENABLED =
      COORDINATOR_GROUP_COMMIT_PREFIX + "enabled";

  public static final String DEFAULT_SYSTEM_NAMESPACE_NAME = "scalardb";

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
    checkArgument(contactPort >= 0, CoreError.INVALID_CONTACT_PORT.buildMessage());
    username = getString(getProperties(), USERNAME, null);
    password = getString(getProperties(), PASSWORD, null);
    transactionManager = getString(getProperties(), TRANSACTION_MANAGER, "consensus-commit");
    metadataCacheExpirationTimeSecs =
        getLong(getProperties(), METADATA_CACHE_EXPIRATION_TIME_SECS, -1);
    activeTransactionManagementExpirationTimeMillis =
        getLong(getProperties(), ACTIVE_TRANSACTION_MANAGEMENT_EXPIRATION_TIME_MILLIS, -1);
    defaultNamespaceName = getString(getProperties(), DEFAULT_NAMESPACE_NAME, null);
    crossPartitionScanEnabled = getBoolean(getProperties(), CROSS_PARTITION_SCAN, false);
    crossPartitionScanFilteringEnabled =
        getBoolean(getProperties(), CROSS_PARTITION_SCAN_FILTERING, false);
    crossPartitionScanOrderingEnabled =
        getBoolean(getProperties(), CROSS_PARTITION_SCAN_ORDERING, false);
    coordinatorGroupCommitEnabled =
        getBoolean(getProperties(), COORDINATOR_GROUP_COMMIT_ENABLED, false);

    if (!crossPartitionScanEnabled
        && (crossPartitionScanFilteringEnabled || crossPartitionScanOrderingEnabled)) {
      throw new IllegalArgumentException(
          CoreError
              .CROSS_PARTITION_SCAN_MUST_BE_ENABLED_TO_USE_CROSS_PARTITION_SCAN_WITH_FILTERING_OR_ORDERING
              .buildMessage());
    }

    systemNamespaceName =
        getString(getProperties(), SYSTEM_NAMESPACE_NAME, DEFAULT_SYSTEM_NAMESPACE_NAME);
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

  public Optional<String> getDefaultNamespaceName() {
    return Optional.ofNullable(defaultNamespaceName);
  }

  public boolean isCrossPartitionScanEnabled() {
    return crossPartitionScanEnabled;
  }

  public boolean isCrossPartitionScanFilteringEnabled() {
    return crossPartitionScanFilteringEnabled;
  }

  public boolean isCrossPartitionScanOrderingEnabled() {
    return crossPartitionScanOrderingEnabled;
  }

  public boolean isCoordinatorGroupCommitEnabled() {
    return coordinatorGroupCommitEnabled;
  }

  public String getCoordinatorGroupCommitPrefix() {
    return COORDINATOR_GROUP_COMMIT_PREFIX;
  }

  public String getSystemNamespaceName() {
    return systemNamespaceName;
  }
}
