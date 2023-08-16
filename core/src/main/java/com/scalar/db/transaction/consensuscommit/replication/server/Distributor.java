package com.scalar.db.transaction.consensuscommit.replication.server;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.Scanner;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class Distributor {
  private final ExecutorService executorService;
  private final String replicationDbNamespace;
  private final String replicationDbTransactionsTable;
  private final int replicationDbPartitionSize;
  private final int replicationDbThreadSize;
  private final int fetchTransactionSize;
  private final DistributedStorage storage;

  public Distributor(
      String replicationDbNamespace,
      String replicationDbTransactionsTable,
      int replicationDbPartitionSize,
      int replicationDbThreadSize,
      int fetchTransactionSize,
      Properties replicationDbProperties) {
    if (replicationDbPartitionSize % replicationDbThreadSize != 0) {
      throw new IllegalArgumentException(
          String.format(
              "`replicationDbPartitionSize`(%d) should be a multiple of `replicationDbThreadSize`(%d)",
              replicationDbPartitionSize, replicationDbThreadSize));
    }
    this.replicationDbNamespace = replicationDbNamespace;
    this.replicationDbTransactionsTable = replicationDbTransactionsTable;
    this.replicationDbPartitionSize = replicationDbPartitionSize;
    this.replicationDbThreadSize = replicationDbThreadSize;
    this.fetchTransactionSize = fetchTransactionSize;
    this.executorService =
        Executors.newFixedThreadPool(
            replicationDbThreadSize,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("log-distributor-%d").build());
    this.storage = StorageFactory.create(replicationDbProperties).getStorage();
  }

  public void start() {
    int deltaPartitionId = replicationDbPartitionSize / replicationDbThreadSize;

    for (int i = 0; i < replicationDbThreadSize; i++) {
      int startPartitionId = i;
      executorService.execute(
          () -> {
            for (int partitionId = startPartitionId;
                partitionId < replicationDbPartitionSize;
                partitionId += deltaPartitionId) {
              try {
                Scanner scan =
                    storage.scan(
                        Scan.newBuilder()
                            .namespace(replicationDbNamespace)
                            .table(replicationDbTransactionsTable)
                            .partitionKey(Key.ofInt("partition_id", partitionId))
                            .ordering(Ordering.asc("created_at"))
                            .limit(fetchTransactionSize)
                            .build());
                scan.all()
                    .forEach(
                        result -> {
                          // TODO: Implement the transfer from `transactions` table to `records`
                          // table
                        });
              } catch (Throwable e) {
                // FIXME: These error handlings
                e.printStackTrace();
                try {
                  TimeUnit.SECONDS.sleep(2);
                } catch (InterruptedException ex) {
                  ex.printStackTrace();
                  Thread.currentThread().interrupt();
                  break;
                }
              }
            }
          });
    }
  }

  public void stop() {}
}
