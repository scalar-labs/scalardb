package com.scalar.db.server;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.codahale.metrics.jmx.JmxReporter;
import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.util.ThrowableRunnable;
import com.scalar.db.util.ThrowableSupplier;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.MetricsServlet;
import io.prometheus.client.hotspot.DefaultExports;
import javax.annotation.concurrent.ThreadSafe;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class Metrics {
  private static final Logger logger = LoggerFactory.getLogger(Metrics.class);
  private static final String PRODUCT_NAME = "scalardb";
  private static final String STATS_PREFIX = "stats";
  private static final String SUCCESS_SUFFIX = "success";
  private static final String FAILURE_SUFFIX = "failure";

  private final ServerConfig config;
  private final MetricRegistry metricRegistry;
  private final String prefix;
  private final Counter totalSuccess;
  private final Counter totalFailure;

  public Metrics(ServerConfig config) {
    this.config = config;
    metricRegistry = new MetricRegistry();
    startJmxReporter();
    startPrometheusExporter();

    prefix = name(PRODUCT_NAME, STATS_PREFIX);
    totalSuccess = metricRegistry.counter(name(prefix, "total", SUCCESS_SUFFIX));
    totalFailure = metricRegistry.counter(name(prefix, "total", FAILURE_SUFFIX));
  }

  @VisibleForTesting
  Metrics() {
    this.config = null;
    this.metricRegistry = null;
    this.prefix = null;
    this.totalSuccess = null;
    this.totalFailure = null;
  }

  private void startJmxReporter() {
    JmxReporter reporter = JmxReporter.forRegistry(metricRegistry).build();
    reporter.start();
    Runtime.getRuntime().addShutdownHook(new Thread(reporter::stop));
  }

  private void startPrometheusExporter() {
    int prometheusExporterPort = config.getPrometheusExporterPort();
    if (prometheusExporterPort < 0) {
      return;
    }

    CollectorRegistry.defaultRegistry.register(new DropwizardExports(metricRegistry));
    DefaultExports.initialize();

    Server server = new Server(prometheusExporterPort);
    ServletContextHandler context = new ServletContextHandler();
    context.setContextPath("/");
    server.setHandler(context);
    context.addServlet(new ServletHolder(new MetricsServlet()), "/stats/prometheus");
    server.setStopAtShutdown(true);
    try {
      server.start();
      logger.info("Prometheus exporter started, listening on {}", prometheusExporterPort);
    } catch (Exception e) {
      logger.error("Failed to start Jetty server", e);
    }
  }

  public void measure(String serviceName, String method, ThrowableRunnable<Throwable> runnable)
      throws Throwable {
    // For test
    if (metricRegistry == null) {
      runnable.run();
      return;
    }

    Timer timer = metricRegistry.timer(name(prefix, serviceName, method));
    try (Context unused = timer.time()) {
      runnable.run();
      onSuccess(serviceName, method);
    } catch (Throwable t) {
      onFailure(serviceName, method);
      throw t;
    }
  }

  public <T> T measure(String serviceName, String method, ThrowableSupplier<T, Throwable> supplier)
      throws Throwable {
    // For test
    if (metricRegistry == null) {
      return supplier.get();
    }

    Timer timer = metricRegistry.timer(name(prefix, serviceName, method));
    try (Context unused = timer.time()) {
      T ret = supplier.get();
      onSuccess(serviceName, method);
      return ret;
    } catch (Throwable t) {
      onFailure(serviceName, method);
      throw t;
    }
  }

  private void onSuccess(String serviceName, String method) {
    totalSuccess.inc();
    metricRegistry.counter(name(prefix, serviceName, method, SUCCESS_SUFFIX)).inc();
  }

  private void onFailure(String serviceName, String method) {
    totalFailure.inc();
    metricRegistry.counter(name(prefix, serviceName, method, FAILURE_SUFFIX)).inc();
  }
}
