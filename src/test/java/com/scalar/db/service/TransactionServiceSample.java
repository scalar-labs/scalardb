package com.scalar.db.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.TransactionState;
import com.scalar.db.config.DatabaseConfig;
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

    TransactionState state = service.getState("9bad6079-969f-4d44-94d4-b210abc89b22");
    System.out.println(state);
    state = service.getState("77be4e36-fe5a-4146-ab95-3918479667cb");
    System.out.println(state);
  }
}
