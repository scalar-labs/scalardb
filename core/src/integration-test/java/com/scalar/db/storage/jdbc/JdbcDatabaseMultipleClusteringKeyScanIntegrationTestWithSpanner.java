package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.service.StorageFactory;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.condition.EnabledIf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spanner-emulator specific multiple clustering key scan integration test that work around the emulator limitation;
 * The emulator only allows one read-write transaction or schema change at a time.
 *
 * <p>This class is only enabled when running against the Spanner emulator. It overrides
 * {@link #createStorageAdmin} and {@link #createStorage} to return thread-dispatching proxies that
 * route each executor thread to its own Spanner database (test-db-1..test-db-N).
 */
@EnabledIf("com.scalar.db.storage.jdbc.JdbcEnv#isSpannerEmulator")
public class JdbcDatabaseMultipleClusteringKeyScanIntegrationTestWithSpanner
    extends JdbcDatabaseMultipleClusteringKeyScanIntegrationTest {

  private static final Logger logger =
      LoggerFactory.getLogger(
          JdbcDatabaseMultipleClusteringKeyScanIntegrationTestWithSpanner.class);
  private final List<DistributedStorage> perThreadStorages = new ArrayList<>();
  private final List<DistributedStorageAdmin> perThreadAdmins = new ArrayList<>();
  private final AtomicInteger threadIndex = new AtomicInteger(0);
  private ThreadLocal<Integer> threadId;

  // System property override used by the thread-count benchmark workflow to sweep values.
  // Falls back to 18 — optimized for a standard GitHub Action runner.
  private static final String THREAD_NUM_PROP = "scalardb.test.multi_ck_scan.thread_num";

  @Override
  protected int getThreadNum() {
    String override = System.getProperty(THREAD_NUM_PROP);
    if (override != null && !override.isEmpty()) {
      return Integer.parseInt(override);
    } else {
      throw new IllegalStateException("THREAD_NUM_PROP must be set to a non-empty integer value");
    }
  }

  @Override
  protected void initialize(String testName) throws Exception {
    threadId = ThreadLocal.withInitial(() -> threadIndex.getAndIncrement() % getThreadNum());

    String baseUrl = System.getProperty("scalardb.jdbc.url");
    logger.info("Initializing {} per-thread Spanner databases", getThreadNum());
    for (int i = 0; i < getThreadNum(); i++) {
      // Real Spanner uses 1-indexed database names: test-db-1..test-db-N.
      String threadUrl = baseUrl.replaceFirst("/databases/[^;/]+", "/databases/test-db-" + (i + 1));
      logger.info("Thread {}: {}", i, threadUrl);
      Properties props = JdbcEnv.getProperties(testName);
      props.setProperty(DatabaseConfig.CONTACT_POINTS, threadUrl);
      StorageFactory factory = StorageFactory.create(props);
      perThreadAdmins.add(factory.getStorageAdmin());
      perThreadStorages.add(factory.getStorage());
    }
  }

  @Override
  protected DistributedStorageAdmin createStorageAdmin(StorageFactory factory) {
    return createAdminProxy();
  }

  @Override
  protected DistributedStorage createStorage(StorageFactory factory) {
    return createStorageProxy();
  }

  private DistributedStorage createStorageProxy() {
    InvocationHandler handler =
        (proxy, method, args) -> {
          // Intercept close() to close all per-thread instances
          if ("close".equals(method.getName()) && (args == null || args.length == 0)) {
            for (DistributedStorage s : perThreadStorages) {
              s.close();
            }
            return null;
          }
          int idx = threadId.get();
          logger.debug(
              "Storage.{}() dispatched to db-{} on thread {}",
              method.getName(),
              idx,
              Thread.currentThread().getName());
          try {
            return method.invoke(perThreadStorages.get(idx), args);
          } catch (java.lang.reflect.InvocationTargetException e) {
            throw e.getCause();
          }
        };
    return (DistributedStorage)
        Proxy.newProxyInstance(
            DistributedStorage.class.getClassLoader(),
            new Class<?>[] {DistributedStorage.class},
            handler);
  }

  private DistributedStorageAdmin createAdminProxy() {
    InvocationHandler handler =
        (proxy, method, args) -> {
          // Per-thread dispatch: truncateTable only clears data in the calling thread's database,
          // since only that thread wrote there via the storage proxy.
          if ("truncateTable".equals(method.getName())) {
            int idx = threadId.get();
            logger.debug(
                "Admin.truncateTable() dispatched to db-{} on thread {}",
                idx,
                Thread.currentThread().getName());
            try {
              return method.invoke(perThreadAdmins.get(idx), args);
            } catch (java.lang.reflect.InvocationTargetException e) {
              throw e.getCause();
            }
          }
          // Broadcast all other admin operations to every per-thread database so the schema stays
          // consistent across all databases.
          Object result = null;
          int i = 0;
          for (DistributedStorageAdmin a : perThreadAdmins) {
            try {
              logger.debug("Admin.{}() dispatched to db-{}", method.getName(), i++);
              result = method.invoke(a, args);
            } catch (java.lang.reflect.InvocationTargetException e) {
              throw e.getCause();
            }
          }
          return result;
        };
    return (DistributedStorageAdmin)
        Proxy.newProxyInstance(
            DistributedStorageAdmin.class.getClassLoader(),
            new Class<?>[] {DistributedStorageAdmin.class},
            handler);
  }
}
