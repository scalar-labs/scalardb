package com.scalar.db.server.service;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.server.Metrics;
import com.scalar.db.server.Pauser;
import com.scalar.db.service.StorageModule;
import com.scalar.db.service.TransactionModule;

public class ServerModule extends AbstractModule {
  private final Injector storageInjector;
  private final Injector transactionInjector;

  public ServerModule(DatabaseConfig config) {
    storageInjector = Guice.createInjector(new StorageModule(config));
    transactionInjector = Guice.createInjector(new TransactionModule(config));
  }

  @Provides
  @Singleton
  DistributedStorage provideDistributedStorage() {
    return storageInjector.getInstance(DistributedStorage.class);
  }

  @Provides
  @Singleton
  DistributedStorageAdmin provideDistributedStorageAdmin() {
    return storageInjector.getInstance(DistributedStorageAdmin.class);
  }

  @Provides
  @Singleton
  DistributedTransactionManager provideDistributedTransactionManager() {
    return transactionInjector.getInstance(DistributedTransactionManager.class);
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

    // start JMX reporter
    JmxReporter reporter = JmxReporter.forRegistry(metricRegistry).build();
    reporter.start();
    Runtime.getRuntime().addShutdownHook(new Thread(reporter::stop));

    return new Metrics(metricRegistry);
  }
}
