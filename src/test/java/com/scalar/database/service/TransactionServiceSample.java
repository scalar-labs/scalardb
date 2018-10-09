package com.scalar.database.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.scalar.database.api.DistributedTransaction;
import com.scalar.database.config.DatabaseConfig;
import java.util.Properties;

public class TransactionServiceSample {
  public static void main(String[] args) {
    Properties props = new Properties();
    props.setProperty("scalar.database.contact_points", "localhost");
    props.setProperty("scalar.database.username", "cassandra");
    props.setProperty("scalar.database.password", "cassandra");

    Injector injector = Guice.createInjector(new TransactionModule(new DatabaseConfig(props)));
    TransactionService service = injector.getInstance(TransactionService.class);
    System.out.println("service initialized: " + service);
    DistributedTransaction transaction = service.start();
    System.out.println("transaction started: " + transaction);

    // it will use the same instance of DistributedStorage
    TransactionService another = injector.getInstance(TransactionService.class);
    System.out.println("service initialized: " + another);
    transaction = service.start();
    System.out.println("transaction started: " + transaction);
  }
}
