package com.scalar.db.benchmark;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.util.AccountBalanceTransferHandler;
import com.scalar.db.util.TransferContext;
import java.io.File;
import java.io.IOException;

/** You can create tables for this program by using `script/sample_schema/tx_transfer.cql` */
public class BenchmarkPreparation {
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

    DatabaseConfig config = null;
    if (propertiesFile != null) {
      config = new DatabaseConfig(new File(propertiesFile));
    }

    TransferContext context = new TransferContext(numAccounts, 0, 0, 0);
    AccountBalanceTransferHandler handler = new AccountBalanceTransferHandler(config, context);

    handler.populateRecords();

    System.exit(0);
  }

  private static void printUsageAndExit() {
    System.err.println(
        "BenchmarkPreparation [-f database.properties] "
            + "[-c concurrency] [-n num_accounts] [-h host_address]");
    System.exit(1);
  }
}
