package com.scalar.db.storage.cassandra;

import static com.google.common.base.Preconditions.checkNotNull;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ConnectionException;
import com.scalar.db.exception.storage.StorageRuntimeException;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A cluster manager
 *
 * @author Hiroyuki Yamada
 */
@ThreadSafe
public class ClusterManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterManager.class);
  private static final int DEFAULT_CASSANDRA_PORT = 9042;
  private final DatabaseConfig config;
  private Cluster.Builder builder;
  private Cluster cluster;
  private Session session;

  /**
   * Constructs a {@code ClusterManager} with the specified {@code Config}
   *
   * @param config database configuration needed to create a cluster
   */
  public ClusterManager(DatabaseConfig config) {
    this.config = checkNotNull(config);
  }

  /**
   * Returns a session. It will create a session if it is not created yet.
   *
   * @return a {@code Session}
   */
  public synchronized Session getSession() {
    if (session != null) {
      return session;
    }
    try {
      if (builder == null) {
        build();
      }
      cluster = getCluster(config);
      session = cluster.connect();
      LOGGER.info("session to the cluster is created.");
      return session;
    } catch (RuntimeException e) {
      LOGGER.error("connecting the cluster failed.", e);
      throw new ConnectionException("connecting the cluster failed.", e);
    }
  }

  /**
   * Returns a {@link TableMetadata} with the specified keyspace and table name
   *
   * @param keyspace keyspace name
   * @param table table name
   * @return {@code TableMetadata}
   */
  @Nonnull
  public TableMetadata getMetadata(String keyspace, String table) {
    KeyspaceMetadata metadata;
    try {
      metadata = cluster.getMetadata().getKeyspace(keyspace);
    } catch (RuntimeException e) {
      throw new ConnectionException("can't get metadata from the cluster", e);
    }
    if (metadata == null || metadata.getTable(table) == null) {
      throw new StorageRuntimeException("no table information found");
    }
    return metadata.getTable(table);
  }

  /** Closes the cluster. */
  public void close() {
    cluster.close();
  }

  @VisibleForTesting
  void build() {
    builder =
        Cluster.builder()
            .withClusterName("Scalar Cluster")
            .addContactPoints(config.getContactPoints().toArray(new String[0]))
            .withPort(
                config.getContactPort() == 0 ? DEFAULT_CASSANDRA_PORT : config.getContactPort())
            .withoutJMXReporting()
            // .withCompression ?
            // .withPoolingOptions ?
            .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
            .withLoadBalancingPolicy(
                new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()));
  }

  @VisibleForTesting
  Cluster getCluster(DatabaseConfig config) {
    if (config.getUsername().isPresent() && config.getPassword().isPresent()) {
      builder.withCredentials(config.getUsername().get(), config.getPassword().get());
    }
    return builder.build();
  }
}
