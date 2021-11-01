package com.scalar.db.config;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Strings;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Isolation;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.storage.cassandra.Cassandra;
import com.scalar.db.storage.cassandra.CassandraAdmin;
import com.scalar.db.storage.cosmos.Cosmos;
import com.scalar.db.storage.cosmos.CosmosAdmin;
import com.scalar.db.storage.dynamo.Dynamo;
import com.scalar.db.storage.dynamo.DynamoAdmin;
import com.scalar.db.storage.jdbc.JdbcDatabase;
import com.scalar.db.storage.jdbc.JdbcDatabaseAdmin;
import com.scalar.db.storage.multistorage.MultiStorage;
import com.scalar.db.storage.multistorage.MultiStorageAdmin;
import com.scalar.db.storage.rpc.GrpcAdmin;
import com.scalar.db.storage.rpc.GrpcStorage;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitManager;
import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitManager;
import com.scalar.db.transaction.jdbc.JdbcTransactionManager;
import com.scalar.db.transaction.rpc.GrpcTransactionManager;
import com.scalar.db.transaction.rpc.GrpcTwoPhaseCommitTransactionManager;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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
@SuppressFBWarnings("JCIP_FIELD_ISNT_FINAL_IN_IMMUTABLE_CLASS")
public class DatabaseConfig {
  private final Properties props;
  private List<String> contactPoints;
  private int contactPort;
  private Optional<String> username;
  private Optional<String> password;
  private Class<? extends DistributedStorage> storageClass;
  private Class<? extends DistributedStorageAdmin> adminClass;
  private Class<? extends DistributedTransactionManager> transactionManagerClass;
  private Class<? extends TwoPhaseCommitTransactionManager> twoPhaseCommitTransactionManagerClass;
  private Isolation isolation = Isolation.SNAPSHOT;
  private long tableMetadataCacheExpirationTimeSecs = -1;

  public static final String PREFIX = "scalar.db.";
  public static final String CONTACT_POINTS = PREFIX + "contact_points";
  public static final String CONTACT_PORT = PREFIX + "contact_port";
  public static final String USERNAME = PREFIX + "username";
  public static final String PASSWORD = PREFIX + "password";
  public static final String STORAGE = PREFIX + "storage";
  public static final String TRANSACTION_MANAGER = PREFIX + "transaction_manager";
  public static final String ISOLATION_LEVEL = PREFIX + "isolation_level";
  public static final String TABLE_METADATA_CACHE_EXPIRATION_TIME_SECS =
      PREFIX + "table_metadata.cache_expiration_time_secs";

  public DatabaseConfig(File propertiesFile) throws IOException {
    this(new FileInputStream(propertiesFile));
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

  public Properties getProperties() {
    return props;
  }

  protected void load() {
    storageClass = Cassandra.class;
    adminClass = CassandraAdmin.class;
    if (!Strings.isNullOrEmpty(props.getProperty(STORAGE))) {
      switch (props.getProperty(STORAGE).toLowerCase()) {
        case "cassandra":
          storageClass = Cassandra.class;
          adminClass = CassandraAdmin.class;
          break;
        case "cosmos":
          storageClass = Cosmos.class;
          adminClass = CosmosAdmin.class;
          break;
        case "dynamo":
          storageClass = Dynamo.class;
          adminClass = DynamoAdmin.class;
          break;
        case "jdbc":
          storageClass = JdbcDatabase.class;
          adminClass = JdbcDatabaseAdmin.class;
          break;
        case "multi-storage":
          storageClass = MultiStorage.class;
          adminClass = MultiStorageAdmin.class;
          break;
        case "grpc":
          storageClass = GrpcStorage.class;
          adminClass = GrpcAdmin.class;
          break;
        default:
          throw new IllegalArgumentException(
              "storage '" + props.getProperty(STORAGE) + "' isn't supported");
      }
    }

    if (storageClass != MultiStorage.class) {
      checkNotNull(props.getProperty(CONTACT_POINTS));

      contactPoints = Arrays.asList(props.getProperty(CONTACT_POINTS).split(","));
      if (Strings.isNullOrEmpty(props.getProperty(CONTACT_PORT))) {
        contactPort = 0;
      } else {
        contactPort = Integer.parseInt(props.getProperty(CONTACT_PORT));
        checkArgument(contactPort > 0);
      }
      username = Optional.ofNullable(props.getProperty(USERNAME));
      password = Optional.ofNullable(props.getProperty(PASSWORD));
    } else {
      username = Optional.empty();
      password = Optional.empty();
    }

    transactionManagerClass = ConsensusCommitManager.class;
    twoPhaseCommitTransactionManagerClass = TwoPhaseConsensusCommitManager.class;
    if (!Strings.isNullOrEmpty(props.getProperty(TRANSACTION_MANAGER))) {
      switch (props.getProperty(TRANSACTION_MANAGER).toLowerCase()) {
        case "consensus-commit":
          transactionManagerClass = ConsensusCommitManager.class;
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
          twoPhaseCommitTransactionManagerClass = GrpcTwoPhaseCommitTransactionManager.class;
          break;
        default:
          throw new IllegalArgumentException(
              "transaction manager '"
                  + props.getProperty(TRANSACTION_MANAGER)
                  + "' isn't supported");
      }
    }

    if (!Strings.isNullOrEmpty(props.getProperty(ISOLATION_LEVEL))) {
      isolation = Isolation.valueOf(props.getProperty(ISOLATION_LEVEL).toUpperCase());
    }

    if (!Strings.isNullOrEmpty(props.getProperty(TABLE_METADATA_CACHE_EXPIRATION_TIME_SECS))) {
      tableMetadataCacheExpirationTimeSecs =
          Long.parseLong(props.getProperty(TABLE_METADATA_CACHE_EXPIRATION_TIME_SECS));
    }
  }

  public List<String> getContactPoints() {
    return contactPoints;
  }

  public int getContactPort() {
    return contactPort;
  }

  public Optional<String> getUsername() {
    return username;
  }

  public Optional<String> getPassword() {
    return password;
  }

  public Class<? extends DistributedStorage> getStorageClass() {
    return storageClass;
  }

  public Class<? extends DistributedStorageAdmin> getAdminClass() {
    return adminClass;
  }

  public Class<? extends TwoPhaseCommitTransactionManager>
      getTwoPhaseCommitTransactionManagerClass() {
    return twoPhaseCommitTransactionManagerClass;
  }

  public Class<? extends DistributedTransactionManager> getTransactionManagerClass() {
    return transactionManagerClass;
  }

  public Isolation getIsolation() {
    return isolation;
  }

  public long getTableMetadataCacheExpirationTimeSecs() {
    return tableMetadataCacheExpirationTimeSecs;
  }
}
