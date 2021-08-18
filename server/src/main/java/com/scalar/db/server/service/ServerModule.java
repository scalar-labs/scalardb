package com.scalar.db.server.service;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.server.Metrics;
import com.scalar.db.server.Pauser;
import com.scalar.db.server.config.ServerConfig;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.service.TransactionFactory;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.MetricsServlet;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerModule extends AbstractModule {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerModule.class);

  private final ServerConfig config;
  private final StorageFactory storageFactory;
  private final TransactionFactory transactionFactory;

  public ServerModule(ServerConfig config, DatabaseConfig databaseConfig) {
    this.config = config;
    storageFactory = new StorageFactory(databaseConfig);
    transactionFactory = new TransactionFactory(databaseConfig);
  }

  @Provides
  @Singleton
  DistributedStorage provideDistributedStorage() {
    return storageFactory.getStorage();
  }

  @Provides
  @Singleton
  DistributedStorageAdmin provideDistributedStorageAdmin() {
    return storageFactory.getAdmin();
  }

  @Provides
  @Singleton
  DistributedTransactionManager provideDistributedTransactionManager() {
    return transactionFactory.getTransactionManager();
  }

  @Provides
  @Singleton
  Pauser providePauser() {
    return new Pauser();
  }

  @Provides
  @Singleton
  Metrics provideMetrics() {
    MetricRegistry metricRegistry = new MetricRegistry();
    startJmxReporter(metricRegistry);
    startPrometheusExporter(metricRegistry);
    return new Metrics(metricRegistry);
  }

  private void startJmxReporter(MetricRegistry metricRegistry) {
    JmxReporter reporter = JmxReporter.forRegistry(metricRegistry).build();
    reporter.start();
    Runtime.getRuntime().addShutdownHook(new Thread(reporter::stop));
  }

  private void startPrometheusExporter(MetricRegistry metricRegistry) {
    int prometheusExporterPort = config.getPrometheusExporterPort();
    if (prometheusExporterPort < 0) {
      return;
    }

    CollectorRegistry.defaultRegistry.register(new DropwizardExports(metricRegistry));

    Server server = new Server(prometheusExporterPort);
    ServletContextHandler context = new ServletContextHandler();
    context.setContextPath("/");
    server.setHandler(context);
    context.addServlet(new ServletHolder(new MetricsServlet()), "/stats/prometheus");
    server.setStopAtShutdown(true);
    try {
      server.start();
      LOGGER.info("Prometheus exporter started, listening on {}", prometheusExporterPort);
    } catch (Exception e) {
      LOGGER.error("failed to start Jetty server", e);
    }
  }
}
