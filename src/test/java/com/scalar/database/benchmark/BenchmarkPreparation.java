package com.scalar.database.benchmark;

import com.scalar.database.config.DatabaseConfig;
import com.scalar.database.util.AccountBalanceTransferHandler;
import com.scalar.database.util.TransferContext;
import java.io.File;
import java.io.IOException;

public class BenchmarkPreparation {
  private static final String NAMESPACE = "benchmark";
  private static final String TABLE = "tx_transfer";
  private static final int DEFAULT_CONCURRENCY = 8;
  private static final int DEFAULT_NUM_ACCOUNTS = 100000;
  private static int concurrency = DEFAULT_CONCURRENCY;
  private static int numAccounts = DEFAULT_NUM_ACCOUNTS;
  private static String propertiesFile;

  public static void main(String[] args) throws IOException {
    for (int i = 0; i < args.length; ++i) {
      if ("-c".equals(args[i])) {
        concurrency = Integer.parseInt(args[++i]);
      } else if ("-n".equals(args[i])) {
        numAccounts = Integer.parseInt(args[++i]);
      } else if ("-f".equals(args[i])) {
        propertiesFile = args[++i];
      } else if ("-help".equals(args[i])) {
        printUsageAndExit();
      }
    }
    if (propertiesFile == null) {
      printUsageAndExit();
    }

    DatabaseConfig config = new DatabaseConfig(new File(propertiesFile));
    TransferContext context = new TransferContext(numAccounts, 0, 0, 0);
    AccountBalanceTransferHandler handler =
        new AccountBalanceTransferHandler(config, context, NAMESPACE, TABLE);
    handler.prepareTables();

    handler.populateRecords();

    System.exit(0);
  }

  private static void printUsageAndExit() {
    System.err.println(
        "BenchmarkPreparation -f database.properties "
            + "[-c concurrency] [-n num_accounts] [-h host_address]");
    System.exit(1);
  }
}
