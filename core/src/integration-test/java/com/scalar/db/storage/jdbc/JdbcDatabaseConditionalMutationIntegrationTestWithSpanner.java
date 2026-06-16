package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.service.StorageFactory;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.condition.EnabledIf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spanner-emulator specific conditional mutation integration test that works around the emulator
 * limitation to reduce the test execution to about 2 hours; the emulator only allows one read-write
 * transaction or schema change at a time.
 *
 * <p>This class is only enabled when running against the Spanner emulator. It overrides {@link
 * #createStorageAdmin} and {@link #createStorage} to return thread-dispatching proxies that route
 * each executor thread to its own Spanner database (test-db-1..test-db-N).
 */
@EnabledIf("com.scalar.db.storage.jdbc.JdbcEnv#isSpannerEmulator")
@SpannerPerThreadContainers
public class JdbcDatabaseConditionalMutationIntegrationTestWithSpanner
    extends JdbcDatabaseConditionalMutationIntegrationTest {

  private static final Logger logger =
      LoggerFactory.getLogger(JdbcDatabaseConditionalMutationIntegrationTestWithSpanner.class);

  private final List<DistributedStorage> perThreadStorages = new ArrayList<>();
  private final List<DistributedStorageAdmin> perThreadAdmins = new ArrayList<>();
  private final AtomicInteger threadIndex = new AtomicInteger(0);
  private ThreadLocal<Integer> threadId;

  @Override
  protected int getThreadNum() {
    // Optimized for a standard Github action runner
    // TODO revert before merging
    return 8;
  }

  @Override
  protected void initialize(String testName) throws Exception {

    threadId = ThreadLocal.withInitial(() -> threadIndex.getAndIncrement() % getThreadNum());

    String baseUrl = System.getProperty("scalardb.jdbc.url");
    boolean useContainers =
        Boolean.getBoolean(SpannerEmulatorExtension.PROP_FLAG) && JdbcEnv.isSpannerEmulator();
    List<String> containerUrls =
        useContainers
            ? SpannerEmulatorContainerSupport.startContainers(baseUrl, getThreadNum())
            : Collections.emptyList();

    logger.debug("Initializing {} per-thread databases for Spanner emulator", getThreadNum());
    for (int i = 0; i < getThreadNum(); i++) {
      // When containers are active, each thread points to its own isolated emulator.
      // When flag is unset, preserve original behavior: test-db-N rewrite of the external URL.
      // Note: JdbcEnv.getProperties() below is called for non-URL properties only;
      // CONTACT_POINTS is immediately overridden on the next line.
      String threadUrl =
          useContainers
              ? containerUrls.get(i)
              : baseUrl.replaceFirst("/databases/[^;/]+", "/databases/test-db-" + i);
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
          // Broadcast all admin operations to every per-thread database
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
