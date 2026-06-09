package com.scalar.db.storage.jdbc;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.testcontainers.containers.SpannerEmulatorContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Owns Spanner emulator container lifecycle and the per-thread active-URL registry.
 *
 * <p>Thread-safety: the ThreadLocal registry is inherently per-thread. The AtomicInteger counter is
 * thread-safe. Container start/stop are called from the JUnit extension callbacks, which run on the
 * test thread.
 */
public final class SpannerEmulatorContainerSupport {

  /** Default Spanner emulator Docker image (bundled with testcontainers:gcloud). */
  static final DockerImageName DEFAULT_IMAGE =
      DockerImageName.parse("gcr.io/cloud-spanner-emulator/emulator");

  // Pattern matches the host:port segment in: jdbc:cloudspanner://HOST:PORT/...
  private static final Pattern HOST_PATTERN = Pattern.compile("(jdbc:cloudspanner://)([^/]+)(/.*)");

  /** Per-thread active emulator JDBC URL. Null when no container is registered for this thread. */
  private static final ThreadLocal<String> ACTIVE_URL = new ThreadLocal<>();

  /** Counts the total number of container starts — used to assert smoke-test isolation. */
  private static final AtomicInteger START_COUNT = new AtomicInteger(0);

  private SpannerEmulatorContainerSupport() {}

  /**
   * Returns the emulator JDBC URL registered for the current thread, or {@code null} if none is
   * registered.
   */
  public static String getActiveUrl() {
    return ACTIVE_URL.get();
  }

  /** Returns the total number of times a container has been started in this JVM. */
  public static int getStartCount() {
    return START_COUNT.get();
  }

  /**
   * Starts a new Spanner emulator container, builds a JDBC URL pointing to its mapped gRPC port,
   * registers the URL for the current thread, and returns the running container.
   *
   * @param baseJdbcUrl the original Spanner emulator JDBC URL (from System property); used as the
   *     URL template — only the host:port segment is replaced.
   */
  public static SpannerEmulatorContainer startContainer(String baseJdbcUrl) {
    SpannerEmulatorContainer container = new SpannerEmulatorContainer(DEFAULT_IMAGE);
    container.start();
    START_COUNT.incrementAndGet();
    String containerUrl = buildContainerUrl(baseJdbcUrl, container.getEmulatorGrpcEndpoint());
    ACTIVE_URL.set(containerUrl);
    return container;
  }

  /**
   * Stops the given container and removes the URL registration for the current thread.
   *
   * @param container the container to stop; may be {@code null} (no-op)
   */
  public static void stopContainer(SpannerEmulatorContainer container) {
    ACTIVE_URL.remove();
    if (container != null) {
      container.stop();
    }
  }

  /**
   * Builds a JDBC URL pointing to the container's mapped gRPC endpoint by replacing the host:port
   * segment in the base URL.
   *
   * <p>Handles: {@code jdbc:cloudspanner://host:port/path;params}. If the URL does not contain a
   * host:port (single-slash format), the endpoint is appended as the {@code endpoint} property.
   */
  static String buildContainerUrl(String baseUrl, String grpcEndpoint) {
    Matcher m = HOST_PATTERN.matcher(baseUrl);
    if (m.matches()) {
      return m.group(1) + grpcEndpoint + m.group(3);
    }
    // Fallback: append endpoint property (single-slash URL format, no embedded host)
    return baseUrl + ";endpoint=" + grpcEndpoint;
  }
}
