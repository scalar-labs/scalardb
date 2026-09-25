package com.scalar.db.storage.cassandra;

import static com.datastax.driver.core.Metadata.quoteIfNecessary;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ServerSideTimestampGenerator;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.config.DatabaseConfig;
import java.util.Objects;
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
  private static final Logger logger = LoggerFactory.getLogger(ClusterManager.class);
  @VisibleForTesting static final int DEFAULT_CASSANDRA_PORT = 9042;
  private Cluster cluster;
  private Session session;

  /**
   * Constructs a {@code ClusterManager} with the specified {@code Config}
   *
   * @param config database configuration needed to create a cluster
   */
  public ClusterManager(DatabaseConfig config) {
    initialize(Objects.requireNonNull(config));
  }

  @VisibleForTesting
  public ClusterManager(Cluster cluster, Session session) {
    this.cluster = cluster;
    this.session = session;
  }

  /**
   * Returns a session. It will create a session if it is not created yet.
   *
   * @return a {@code Session}
   */
  public Session getSession() {
    return session;
  }

  /**
   * Returns a {@link TableMetadata} with the specified keyspace and table name
   *
   * @param keyspace keyspace name
   * @param table table name
   * @return {@code TableMetadata}
   */
  public TableMetadata getMetadata(String keyspace, String table) {
    KeyspaceMetadata metadata = cluster.getMetadata().getKeyspace(quoteIfNecessary(keyspace));
    if (metadata == null) {
      return null;
    }
    return metadata.getTable(quoteIfNecessary(table));
  }

  /** Closes the cluster. */
  public void close() {
    cluster.close();
  }

  private void initialize(DatabaseConfig config) {
    cluster = getCluster(config);
    session = cluster.connect();
    logger.info("Session to the cluster is created");
  }

  @VisibleForTesting
  Cluster getCluster(DatabaseConfig config) {
    Cluster.Builder builder =
        Cluster.builder()
            .withClusterName("Scalar Cluster")
            .addContactPoints(config.getContactPoints().toArray(new String[0]))
            .withPort(
                config.getContactPort() == 0 ? DEFAULT_CASSANDRA_PORT : config.getContactPort())
            .withoutJMXReporting()
            // Take write timestamps from the server so that non-conditional writes and lightweight
            // transactions share one monotonic source, which Cassandra keeps per node; the load
            // balancing policy below keeps each partition on one coordinator. With the driver's
            // default client-side timestamps, a lightweight transaction running in the same
            // millisecond as a preceding non-conditional write gets an older timestamp and its
            // update is silently discarded.
            .withTimestampGenerator(ServerSideTimestampGenerator.INSTANCE)
            // .withCompression ?
            // .withPoolingOptions ?
            .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
            // Send every request for a partition to the same replica, the first live local one in
            // token-ring order. Server-side write timestamps are monotonic only per coordinator, so
            // with the default random replica order two successive writes to a record could be
            // coordinated by different nodes, and the later one could get an older timestamp and be
            // silently discarded.
            .withLoadBalancingPolicy(
                new TokenAwarePolicy(
                    DCAwareRoundRobinPolicy.builder().build(),
                    TokenAwarePolicy.ReplicaOrdering.TOPOLOGICAL));
    if (config.getUsername().isPresent() && config.getPassword().isPresent()) {
      builder.withCredentials(config.getUsername().get(), config.getPassword().get());
    }
    return builder.build();
  }
}
