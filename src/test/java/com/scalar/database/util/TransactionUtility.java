package com.scalar.database.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.scalar.database.api.DistributedStorage;
import com.scalar.database.api.DistributedTransactionManager;
import com.scalar.database.api.TransactionState;
import com.scalar.database.config.DatabaseConfig;
import com.scalar.database.storage.cassandra.Cassandra;
import com.scalar.database.transaction.consensuscommit.Attribute;
import com.scalar.database.transaction.consensuscommit.ConsensusCommitManager;
import com.scalar.database.transaction.consensuscommit.Coordinator;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TransactionUtility {
  private static final Map<String, String> metaAttributes =
      ImmutableMap.of(
          Attribute.ID,
          "text",
          Attribute.VERSION,
          "int",
          Attribute.STATE,
          "int",
          Attribute.PREPARED_AT,
          "bigint",
          Attribute.COMMITTED_AT,
          "bigint");
  private static final long SLEEP_BASE_MILLIS = 100;

  public static void prepareUserTable(
      DatabaseConfig config,
      String namespace,
      String table,
      List<String> partitionKeys,
      List<String> clusteringKeys,
      Map<String, String> attributes) {
    clusteringKeys = clusteringKeys == null ? Lists.newArrayList() : clusteringKeys;
    executeStatement(config, dropTableStatement(namespace, table));
    executeStatement(
        config, createTableStatement(namespace, table, partitionKeys, clusteringKeys, attributes));
  }

  public static void prepareCoordinatorTable(DatabaseConfig config) {
    executeStatement(config, dropTableStatement(Coordinator.NAMESPACE, Coordinator.TABLE));
    executeStatement(
        config, createCoordinatorTableStatement(Coordinator.NAMESPACE, Coordinator.TABLE));
  }

  public static DistributedTransactionManager prepareTransactionManager(
      DistributedStorage storage, String namespace, String table) {
    // TODO: use dependency injection
    ConsensusCommitManager manager = new ConsensusCommitManager(storage);
    manager.with(namespace, table);
    return manager;
  }

  public static DistributedStorage prepareStorage(DatabaseConfig config) {
    return new Cassandra(config);
  }

  private static String createTableStatement(
      String namespace,
      String table,
      List<String> partitionKeys,
      List<String> clusteringKeys,
      Map<String, String> attributes) {
    checkArgument(partitionKeys != null && !partitionKeys.isEmpty(), "Partition key is required.");
    checkNotNull(clusteringKeys);

    if (attributes.keySet().stream().anyMatch(k -> metaAttributes.containsKey(k))) {
      throw new IllegalArgumentException("Illegal attribute is included.");
    }

    attributes.putAll(metaAttributes);

    Map<String, String> beforeAttributes = new HashMap<>();
    attributes.forEach(
        (key, type) -> {
          if (partitionKeys.contains(key) || clusteringKeys.contains(key)) {
            return;
          }
          beforeAttributes.put(Attribute.BEFORE_PREFIX + key, type);
        });

    attributes.putAll(beforeAttributes);

    String strAttributes = Joiner.on(",").withKeyValueSeparator(" ").join(attributes);
    String strPartitionKeys = Joiner.on(",").skipNulls().join(partitionKeys);
    String strClusteringKeys = Joiner.on(",").skipNulls().join(clusteringKeys);

    String strPrimaryKey;
    if (strClusteringKeys.length() == 0) {
      strPrimaryKey = "PRIMARY KEY (" + strPartitionKeys + ")";
    } else {
      strPrimaryKey = "PRIMARY KEY ((" + strPartitionKeys + ")," + strClusteringKeys + ")";
    }

    return Joiner.on(" ")
        .skipNulls()
        .join(
            new String[] {
              "CREATE TABLE", namespace + "." + table, "(", strAttributes, ",", strPrimaryKey, ")",
            });
  }

  private static String createCoordinatorTableStatement(String namespace, String table) {
    return Joiner.on(" ")
        .skipNulls()
        .join(
            new String[] {
              "CREATE TABLE",
              namespace + "." + table,
              "(",
              Attribute.ID,
              "text,",
              Attribute.STATE,
              "int,",
              Attribute.CREATED_AT,
              "bigint,",
              "PRIMARY KEY",
              "(" + Attribute.ID + ")",
              ")",
            });
  }

  private static String dropTableStatement(String namespace, String table) {
    return Joiner.on(" ")
        .skipNulls()
        .join(
            new String[] {
              "DROP TABLE IF EXISTS ", namespace + "." + table,
            });
  }

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

  private static void executeStatement(DatabaseConfig config, String statement) {
    String host = config.getContactPoints().get(0);
    String username = config.getUsername();
    String password = config.getPassword();

    ProcessBuilder builder =
        new ProcessBuilder("cqlsh", host, "-u", username, "-p", password, "-e", statement);
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
        throw new RuntimeException("failed to execute " + statement);
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
    DistributedStorage storage = prepareStorage(config);
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
