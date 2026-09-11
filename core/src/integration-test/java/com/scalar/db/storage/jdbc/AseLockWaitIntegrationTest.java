package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Put;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.service.TransactionFactory;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Proves that a write contending with an uncommitted transaction fails rather than hanging on SAP
 * ASE. ASE waits for a lock forever by default, so contention becomes a stall that never returns
 * and that Consensus Commit cannot retry, with nothing logged anywhere. {@link
 * JdbcConfig#SYBASE_LOCK_WAIT_SECONDS} bounds the wait, and the resulting error 12205 is reported
 * as a conflict.
 *
 * <p>This is the regression test for two integration suite runs that stalled indefinitely.
 */
public class AseLockWaitIntegrationTest {

  private static final String TABLE = "lock_wait";
  private static final int LOCK_WAIT_SECONDS = 3;

  private final String namespace = System.getProperty("scalardb.ase.namespace", "n1");

  private static final TableMetadata METADATA =
      TableMetadata.newBuilder()
          .addColumn("pk", DataType.TEXT)
          .addColumn("v", DataType.TEXT)
          .addPartitionKey("pk")
          .build();

  private Properties properties() {
    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.STORAGE, "jdbc");
    properties.setProperty(
        DatabaseConfig.CONTACT_POINTS,
        System.getProperty("scalardb.jdbc.url", "jdbc:sybase:Tds:localhost:5557/scalardb"));
    properties.setProperty(
        DatabaseConfig.USERNAME, System.getProperty("scalardb.jdbc.username", "sa"));
    properties.setProperty(
        DatabaseConfig.PASSWORD, System.getProperty("scalardb.jdbc.password", "sybase"));
    properties.setProperty(DatabaseConfig.SYSTEM_NAMESPACE_NAME, "scalardb");
    properties.setProperty(DatabaseConfig.TRANSACTION_MANAGER, "jdbc");
    // Short enough to keep the test quick; the point is that it returns at all
    properties.setProperty(JdbcConfig.SYBASE_LOCK_WAIT_SECONDS, String.valueOf(LOCK_WAIT_SECONDS));
    properties.setProperty(JdbcConfig.CONNECTION_POOL_MIN_IDLE, "0");
    properties.setProperty(JdbcConfig.TABLE_METADATA_CONNECTION_POOL_MIN_IDLE, "0");
    properties.setProperty(JdbcConfig.ADMIN_CONNECTION_POOL_MIN_IDLE, "0");
    return properties;
  }

  @Test
  public void contendedWrite_ShouldFailRatherThanHang() throws Exception {
    Properties properties = properties();
    TransactionFactory factory = TransactionFactory.create(properties);

    try (DistributedTransactionAdmin admin = factory.getTransactionAdmin();
        DistributedTransactionManager manager = factory.getTransactionManager()) {
      admin.createNamespace(namespace, true);
      admin.createTable(namespace, TABLE, METADATA, true);

      // A transaction that writes and never commits, holding the row lock
      DistributedTransaction holder = manager.start();
      holder.put(
          Put.newBuilder()
              .namespace(namespace)
              .table(TABLE)
              .partitionKey(Key.ofText("pk", "contended"))
              .textValue("v", "held")
              .build());

      ExecutorService executor = Executors.newSingleThreadExecutor();
      try {
        Future<String> contender =
            executor.submit(
                () -> {
                  DistributedTransaction other = manager.start();
                  try {
                    other.put(
                        Put.newBuilder()
                            .namespace(namespace)
                            .table(TABLE)
                            .partitionKey(Key.ofText("pk", "contended"))
                            .textValue("v", "contending")
                            .build());
                    other.commit();
                    return "committed";
                  } catch (Exception e) {
                    Throwable root = e;
                    while (root.getCause() != null) {
                      root = root.getCause();
                    }
                    SQLException sqlException =
                        root instanceof SQLException
                            ? (SQLException) root
                            : new SQLException("not a SQLException", "", 0);
                    return e.getClass().getSimpleName()
                        + " / errorCode="
                        + sqlException.getErrorCode()
                        + " / conflict="
                        + new RdbEngineSybase().isConflict(sqlException);
                  } finally {
                    try {
                      other.rollback();
                    } catch (Exception ignored) {
                      // already finished
                    }
                  }
                });

        // The whole point: this returns. Before the fix it blocked forever.
        String outcome = contender.get(LOCK_WAIT_SECONDS + 25, TimeUnit.SECONDS);
        System.out.println("[ase] contended write returned: " + outcome);
        // Either ASE error 12205, the lock wait expiring, or 2601, the second writer losing the
        // race to insert the key. Which one comes back depends on how far the first writer got.
        // Both have to be reported as a conflict so the caller can retry.
        assertThat(outcome).matches(".*errorCode=(12205|2601).*").contains("conflict=true");
      } finally {
        executor.shutdownNow();
        holder.rollback();
      }

      admin.dropTable(namespace, TABLE, true);
    }
  }
}
