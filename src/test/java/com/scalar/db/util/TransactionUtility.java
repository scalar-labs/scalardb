package com.scalar.db.util;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.TransactionState;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.service.StorageModule;
import com.scalar.db.service.StorageService;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class TransactionUtility {
  private static final long SLEEP_BASE_MILLIS = 100;

  public static void executeCommand(String... commandPath) {
    if (commandPath == null) {
      return;
    }
    ProcessBuilder builder = new ProcessBuilder(commandPath);
    builder.redirectErrorStream(true);

    BufferedReader reader = null;
    try {
      Process process = builder.start();
      reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      while (true) {
        String line = reader.readLine();
        if (line == null) break;
        System.out.println(line);
      }

      int ret = process.waitFor();
      if (ret != 0) {
        throw new RuntimeException("failed to execute " + commandPath);
      }
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage());
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (Exception e) {
          throw new RuntimeException(e.getMessage());
        }
      }
    }
  }

  public static void checkCoordinatorWithRetry(DatabaseConfig config, TransferContext context) {
    if (config == null) {
      Properties props = new Properties();
      props.setProperty("scalar.db.contact_points", "localhost");
      props.setProperty("scalar.db.username", "cassandra");
      props.setProperty("scalar.db.password", "cassandra");
      config = new DatabaseConfig(props);
    }

    Injector injector = Guice.createInjector(new StorageModule(config));
    DistributedStorage storage = injector.getInstance(StorageService.class);
    Coordinator coordinator = new Coordinator(storage);

    Map<String, List<Integer>> unknownTransactions = context.getUnknownTransactions();
    if (unknownTransactions.isEmpty()) {
      return;
    }

    System.out.println("reading coordinator status...");
    unknownTransactions.forEach(
        (txId, ids) -> {
          int i = 0;
          while (true) {
            if (i >= 10) {
              throw new RuntimeException("some records can't be recovered");
            }
            try {
              Optional<Coordinator.State> state = coordinator.getState(txId);
              if (state.isPresent() && state.get().getState().equals(TransactionState.COMMITTED)) {
                System.out.println("id: " + txId + " succeeded, not failed");
                // we can get the detail of the transaction by the ID if needed
                context.logSuccess(txId, ids, 0);
              }
              break;
            } catch (Exception e) {
              ++i;
              exponentialBackoff(i);
            }
          }
        });
  }

  public static void exponentialBackoff(int counter) {
    try {
      Thread.sleep((long) Math.pow(2, counter) * SLEEP_BASE_MILLIS);
    } catch (InterruptedException e) {
      // ignore
    }
  }
}
