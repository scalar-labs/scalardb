package com.scalar.database.storage.cassandra4driver;

import static com.google.common.base.Preconditions.checkNotNull;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.google.common.annotations.VisibleForTesting;
import com.scalar.database.config.DatabaseConfig;
import com.scalar.database.exception.storage.ConnectionException;
import com.scalar.database.exception.storage.StorageRuntimeException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A cluster manager
 *
 * @author Hiroyuki Yamada, Yuji Ito
 */
@ThreadSafe
public class ClusterManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterManager.class);
  private static final int DEFAULT_CASSANDRA_PORT = 9042;
  private final DatabaseConfig config;
  private CqlSessionBuilder builder;
  private CqlSession session;

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
   * @return a {@code CqlSession}
   */
  public synchronized CqlSession getSession() {
    if (session != null) {
      return session;
    }
    try {
      if (builder == null) {
        builder = getBuilder();
      }
      session = builder.build();
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
    return session
        .getMetadata()
        .getKeyspace(keyspace)
        .orElseThrow(() -> new StorageRuntimeException("no keyspace information found"))
        .getTable(table)
        .orElseThrow(() -> new StorageRuntimeException("no table information found"));
  }

  /** Closes the session and builder. */
  public void close() {
    session.close();
    session = null;
    builder = null;
  }

  @VisibleForTesting
  CqlSessionBuilder getBuilder() {
    List<InetSocketAddress> contactPoints = new ArrayList<InetSocketAddress>();
    int port = config.getContactPort() == 0 ? DEFAULT_CASSANDRA_PORT : config.getContactPort();
    config
        .getContactPoints()
        .forEach(
            contactPoint -> {
              InetSocketAddress addr = new InetSocketAddress(contactPoint, port);
              if (addr.isUnresolved()) {
                throw new ConnectionException("the address is unresolved.");
              }
              contactPoints.add(addr);
            });

    CqlSessionBuilder builder =
        CqlSession.builder()
            .addContactPoints(contactPoints)
            .withLocalDatacenter("datacenter1"); // for SimpleSnitch

    if (config.getUsername() != null && config.getPassword() != null) {
      builder.withAuthCredentials(config.getUsername(), config.getPassword());
    }

    return builder;
  }
}
