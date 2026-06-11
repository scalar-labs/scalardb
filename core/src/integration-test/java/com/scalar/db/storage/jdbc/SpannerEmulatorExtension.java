package com.scalar.db.storage.jdbc;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.SpannerEmulatorContainer;

/**
 * JUnit 5 extension that provisions one Spanner emulator container per test class when the opt-in
 * flag is set and the test is running against the Spanner emulator.
 *
 * <p>This extension is a complete no-op unless BOTH of the following conditions are true:
 *
 * <ul>
 *   <li>System property {@code scalardb.jdbc.spanner.testcontainers} is set to {@code true}
 *   <li>{@link JdbcEnv#isSpannerEmulator()} returns {@code true}
 * </ul>
 *
 * <p>Registered globally via ServiceLoader (META-INF/services) + junit-platform.properties.
 */
public class SpannerEmulatorExtension implements BeforeAllCallback, AfterAllCallback {

  private static final Logger logger = LoggerFactory.getLogger(SpannerEmulatorExtension.class);

  static final String PROP_FLAG = "scalardb.jdbc.spanner.testcontainers";
  private static final String STORE_KEY = "spannerContainer";
  private static final Namespace NAMESPACE = Namespace.create(SpannerEmulatorExtension.class);

  @Override
  public void beforeAll(ExtensionContext context) {
    if (!isEnabled()) {
      return;
    }
    if (context.getRequiredTestClass().isAnnotationPresent(SpannerPerThreadContainers.class)) {
      logger.info(
          "SpannerPerThreadContainers detected for class: {} — skipping per-class container",
          context.getRequiredTestClass().getSimpleName());
      return; // per-thread class: initialize() handles container provisioning
    }
    String baseUrl = System.getProperty("scalardb.jdbc.url");
    logger.info(
        "Starting Spanner emulator container for class: {}",
        context.getRequiredTestClass().getSimpleName());
    SpannerEmulatorContainer container = SpannerEmulatorContainerSupport.startContainer(baseUrl);
    context.getStore(NAMESPACE).put(STORE_KEY, container);
    logger.info(
        "Spanner emulator container started (gRPC: {})", container.getEmulatorGrpcEndpoint());
  }

  @Override
  public void afterAll(ExtensionContext context) {
    if (!isEnabled()) {
      return;
    }
    if (context.getRequiredTestClass().isAnnotationPresent(SpannerPerThreadContainers.class)) {
      logger.info(
          "Stopping per-thread containers for class: {}",
          context.getRequiredTestClass().getSimpleName());
      SpannerEmulatorContainerSupport.stopRegisteredContainers();
      return;
    }
    SpannerEmulatorContainer container =
        context.getStore(NAMESPACE).get(STORE_KEY, SpannerEmulatorContainer.class);
    logger.info(
        "Stopping Spanner emulator container for class: {}",
        context.getRequiredTestClass().getSimpleName());
    SpannerEmulatorContainerSupport.stopContainer(container);
  }

  private static boolean isEnabled() {
    return Boolean.getBoolean(PROP_FLAG) && JdbcEnv.isSpannerEmulator();
  }
}
