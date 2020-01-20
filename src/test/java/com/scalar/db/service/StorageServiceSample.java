package com.scalar.db.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.scalar.db.config.DatabaseConfig;
import java.util.Properties;

public class StorageServiceSample {
  public static void main(String[] args) {
    Properties props = new Properties();
    props.setProperty("scalar.database.contact_points", "localhost");
    props.setProperty("scalar.database.username", "cassandra");
    props.setProperty("scalar.database.password", "cassandra");

    Injector injector = Guice.createInjector(new StorageModule(new DatabaseConfig(props)));
    StorageService service = injector.getInstance(StorageService.class);
    System.out.println("service initialized: " + service);

    // it will use the same instance of DistributedStorage
    StorageService another = injector.getInstance(StorageService.class);
    System.out.println("service initialized: " + another);
  }
}
