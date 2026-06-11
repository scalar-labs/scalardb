package com.scalar.db.storage.jdbc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.SpannerEmulatorContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

/**
 * Owns Spanner emulator container lifecycle and the per-thread active-URL registry.
 *
 * <p>Thread-safety: the ThreadLocal registry is inherently per-thread. The AtomicInteger counter is
 * thread-safe. Container start/stop are called from the JUnit extension callbacks, which run on the
 * test thread.
 */
public final class SpannerEmulatorContainerSupport {

  private static final Logger logger =
      LoggerFactory.getLogger(SpannerEmulatorContainerSupport.class);

  /** Default Spanner emulator Docker image (bundled with testcontainers:gcloud). */
  static final DockerImageName DEFAULT_IMAGE =
      DockerImageName.parse("gcr.io/cloud-spanner-emulator/emulator");

  // Pattern matches the host:port segment in: jdbc:cloudspanner://HOST:PORT/...
  private static final Pattern HOST_PATTERN = Pattern.compile("(jdbc:cloudspanner://)([^/]+)(/.*)");

  // Canonical project/instance/database names used in the normalized container URL.
  // autoConfigEmulator=true creates these automatically; the values do not need to pre-exist.
  private static final String CANONICAL_PROJECT = "test-project";

  private static final String CANONICAL_INSTANCE = "test-instance";

  private static final String CANONICAL_DATABASE = "test-db";

  // Matches /projects/{p}/instances/{i}/databases/{d} followed by optional ;params
  private static final Pattern PATH_PATTERN =
      Pattern.compile("(/projects/)[^/]+(/instances/)[^/]+(/databases/)[^;/]+(;.*)?");

  /** Per-thread active emulator JDBC URL. Null when no container is registered for this thread. */
  private static final ThreadLocal<String> ACTIVE_URL = new ThreadLocal<>();

  /** Counts the total number of container starts — used to assert smoke-test isolation. */
  private static final AtomicInteger START_COUNT = new AtomicInteger(0);

  /**
   * Registry of containers started via {@link #startContainers(String, int)} for extension-driven
   * teardown.
   */
  private static final List<SpannerEmulatorContainer> REGISTERED_CONTAINERS =
      new CopyOnWriteArrayList<>();

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
   * Starts {@code n} Spanner emulator containers in parallel, registers them for extension-driven
   * teardown, and returns a list of {@code n} JDBC URLs (one per container, in creation order).
   *
   * <p>Does NOT set {@link #ACTIVE_URL} — URLs flow explicitly as the returned list.
   *
   * @param baseJdbcUrl the system-property JDBC URL used as a URL template (host:port replaced)
   * @param n the number of containers to start
   * @return an unmodifiable list of n JDBC URLs, index i corresponding to thread i
   */
  public static List<String> startContainers(String baseJdbcUrl, int n) {
    logger.info("Starting {} Spanner emulator containers for per-thread class", n);
    List<SpannerEmulatorContainer> containers = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      containers.add(new SpannerEmulatorContainer(DEFAULT_IMAGE));
    }
    Startables.deepStart(containers).join();
    START_COUNT.addAndGet(n);
    REGISTERED_CONTAINERS.addAll(containers);
    List<String> urls = new ArrayList<>(n);
    for (SpannerEmulatorContainer c : containers) {
      urls.add(buildContainerUrl(baseJdbcUrl, c.getEmulatorGrpcEndpoint()));
    }
    return Collections.unmodifiableList(urls);
  }

  /**
   * Stops all containers registered via {@link #startContainers(String, int)} and clears the
   * registry. Called by {@link SpannerEmulatorExtension#afterAll} for per-thread classes.
   */
  public static void stopRegisteredContainers() {
    for (SpannerEmulatorContainer c : REGISTERED_CONTAINERS) {
      c.stop();
    }
    REGISTERED_CONTAINERS.clear();
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
      String normalizedPath = normalizePath(m.group(3));
      return m.group(1) + grpcEndpoint + normalizedPath;
    }
    // Fallback: append endpoint property (single-slash URL format, no embedded host)
    return baseUrl + ";endpoint=" + grpcEndpoint;
  }

  /**
   * Rewrites the {@code /projects/.../instances/.../databases/...} path segment to canonical fixed
   * values, preserving any {@code ;params} suffix verbatim.
   *
   * <p>Using canonical names ensures every test class connects to the same logical database name
   * regardless of the developer-supplied base URL. {@code autoConfigEmulator=true} auto-creates the
   * project/instance/database on first connection, so the values do not need to pre-exist.
   *
   * <p>If the path does not match the expected shape, it is returned verbatim (defensive fallback).
   */
  private static String normalizePath(String pathAndParams) {
    Matcher p = PATH_PATTERN.matcher(pathAndParams);
    if (p.matches()) {
      String params = (p.group(4) != null) ? p.group(4) : "";
      return "/projects/"
          + CANONICAL_PROJECT
          + "/instances/"
          + CANONICAL_INSTANCE
          + "/databases/"
          + CANONICAL_DATABASE
          + params;
    }
    // Path does not match expected shape — return verbatim (defensive fallback)
    return pathAndParams;
  }
}
