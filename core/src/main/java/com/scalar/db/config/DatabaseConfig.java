package com.scalar.db.config;

import static com.google.common.base.Preconditions.checkArgument;
import static com.scalar.db.config.ConfigUtils.getBoolean;
import static com.scalar.db.config.ConfigUtils.getInt;
import static com.scalar.db.config.ConfigUtils.getLong;
import static com.scalar.db.config.ConfigUtils.getString;
import static com.scalar.db.config.ConfigUtils.getStringArray;

import com.google.common.collect.ImmutableList;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.storage.cassandra.Cassandra;
import com.scalar.db.storage.cassandra.CassandraAdmin;
import com.scalar.db.storage.cosmos.Cosmos;
import com.scalar.db.storage.cosmos.CosmosAdmin;
import com.scalar.db.storage.dynamo.Dynamo;
import com.scalar.db.storage.dynamo.DynamoAdmin;
import com.scalar.db.storage.jdbc.JdbcAdmin;
import com.scalar.db.storage.jdbc.JdbcDatabase;
import com.scalar.db.storage.multistorage.MultiStorage;
import com.scalar.db.storage.multistorage.MultiStorageAdmin;
import com.scalar.db.storage.rpc.GrpcAdmin;
import com.scalar.db.storage.rpc.GrpcStorage;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdmin;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitManager;
import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitManager;
import com.scalar.db.transaction.jdbc.JdbcTransactionAdmin;
import com.scalar.db.transaction.jdbc.JdbcTransactionManager;
import com.scalar.db.transaction.rpc.GrpcTransactionAdmin;
import com.scalar.db.transaction.rpc.GrpcTransactionManager;
import com.scalar.db.transaction.rpc.GrpcTwoPhaseCommitTransactionManager;
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
  private Class<? extends DistributedStorage> storageClass;
  private Class<? extends DistributedStorageAdmin> storageAdminClass;
  private Class<? extends DistributedTransactionManager> transactionManagerClass;
  private Class<? extends DistributedTransactionAdmin> transactionAdminClass;
  private Class<? extends TwoPhaseCommitTransactionManager> twoPhaseCommitTransactionManagerClass;
  private long metadataCacheExpirationTimeSecs;
  private boolean isDebugging;

  public static final String PREFIX = "scalar.db.";
  public static final String CONTACT_POINTS = PREFIX + "contact_points";
  public static final String CONTACT_PORT = PREFIX + "contact_port";
  public static final String USERNAME = PREFIX + "username";
  public static final String PASSWORD = PREFIX + "password";
  public static final String STORAGE = PREFIX + "storage";
  public static final String TRANSACTION_MANAGER = PREFIX + "transaction_manager";
  public static final String METADATA_CACHE_EXPIRATION_TIME_SECS =
      PREFIX + "metadata.cache_expiration_time_secs";
  public static final String DEBUG = PREFIX + "debug";

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

  public Properties getProperties() {
    Properties ret = new Properties();
    ret.putAll(props);
    return ret;
  }

  protected void load() {
    String storage = getString(getProperties(), STORAGE, "cassandra");
    switch (storage.toLowerCase()) {
      case "cassandra":
        storageClass = Cassandra.class;
        storageAdminClass = CassandraAdmin.class;
        break;
      case "cosmos":
        storageClass = Cosmos.class;
        storageAdminClass = CosmosAdmin.class;
        break;
      case "dynamo":
        storageClass = Dynamo.class;
        storageAdminClass = DynamoAdmin.class;
        break;
      case "jdbc":
        storageClass = JdbcDatabase.class;
        storageAdminClass = JdbcAdmin.class;
        break;
      case "multi-storage":
        storageClass = MultiStorage.class;
        storageAdminClass = MultiStorageAdmin.class;
        break;
      case "grpc":
        storageClass = GrpcStorage.class;
        storageAdminClass = GrpcAdmin.class;
        break;
      default:
        throw new IllegalArgumentException("storage '" + storage + "' isn't supported");
    }

    contactPoints =
        ImmutableList.copyOf(getStringArray(getProperties(), CONTACT_POINTS, new String[0]));
    contactPort = getInt(getProperties(), CONTACT_PORT, 0);
    checkArgument(contactPort >= 0);
    username = getString(getProperties(), USERNAME, null);
    password = getString(getProperties(), PASSWORD, null);

    String transactionManager = getString(getProperties(), TRANSACTION_MANAGER, "consensus-commit");
    switch (transactionManager.toLowerCase()) {
      case "consensus-commit":
        transactionManagerClass = ConsensusCommitManager.class;
        transactionAdminClass = ConsensusCommitAdmin.class;
        twoPhaseCommitTransactionManagerClass = TwoPhaseConsensusCommitManager.class;
        break;
      case "jdbc":
        if (storageClass != JdbcDatabase.class) {
          throw new IllegalArgumentException(
              "'jdbc' transaction manager ("
                  + TRANSACTION_MANAGER
                  + ") is supported only for 'jdbc' storage ("
                  + STORAGE
                  + ")");
        }
        transactionManagerClass = JdbcTransactionManager.class;
        transactionAdminClass = JdbcTransactionAdmin.class;
        twoPhaseCommitTransactionManagerClass = null;
        break;
      case "grpc":
        if (storageClass != GrpcStorage.class) {
          throw new IllegalArgumentException(
              "'grpc' transaction manager ("
                  + TRANSACTION_MANAGER
                  + ") is supported only for 'grpc' storage ("
                  + STORAGE
                  + ")");
        }
        transactionManagerClass = GrpcTransactionManager.class;
        transactionAdminClass = GrpcTransactionAdmin.class;
        twoPhaseCommitTransactionManagerClass = GrpcTwoPhaseCommitTransactionManager.class;
        break;
      default:
        throw new IllegalArgumentException(
            "transaction manager '" + transactionManager + "' isn't supported");
    }

    metadataCacheExpirationTimeSecs =
        getLong(getProperties(), METADATA_CACHE_EXPIRATION_TIME_SECS, -1);
    isDebugging = getBoolean(getProperties(), DEBUG, false);
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

  public Class<? extends DistributedStorage> getStorageClass() {
    return storageClass;
  }

  public Class<? extends DistributedStorageAdmin> getStorageAdminClass() {
    return storageAdminClass;
  }

  public Class<? extends DistributedTransactionManager> getTransactionManagerClass() {
    return transactionManagerClass;
  }

  public Class<? extends DistributedTransactionAdmin> getTransactionAdminClass() {
    return transactionAdminClass;
  }

  public Class<? extends TwoPhaseCommitTransactionManager>
      getTwoPhaseCommitTransactionManagerClass() {
    return twoPhaseCommitTransactionManagerClass;
  }

  public long getMetadataCacheExpirationTimeSecs() {
    return metadataCacheExpirationTimeSecs;
  }

  public boolean isDebugging() {
    return isDebugging;
  }
}
