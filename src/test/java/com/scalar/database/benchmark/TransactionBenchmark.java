package com.scalar.database.benchmark;

import com.scalar.database.config.DatabaseConfig;
import com.scalar.database.util.AccountBalanceTransferHandler;
import com.scalar.database.util.TransferContext;
import java.io.File;
import java.io.IOException;

public class TransactionBenchmark {
  private static final String NAMESPACE = "benchmark";
  private static final String TABLE = "tx_transfer";
  private static final int DEFAULT_CONCURRENCY = 1;
  private static final int DEFAULT_NUM_ACCOUNTS = 100000;
  private static final int DEFAULT_RUN_FOR_SEC = 60;
  private static final int DEFAULT_RAMP_SEC = 0;
  private static int concurrency = DEFAULT_CONCURRENCY;
  private static int numAccounts = DEFAULT_NUM_ACCOUNTS;
  private static int runFor = DEFAULT_RUN_FOR_SEC;
  private static int ramp = DEFAULT_RAMP_SEC;
  private static int clientSeed = 0;
  private static String propertiesFile;

  public static void main(String[] args) throws IOException {
    for (int i = 0; i < args.length; ++i) {
      if ("-c".equals(args[i])) {
        concurrency = Integer.parseInt(args[++i]);
      } else if ("-n".equals(args[i])) {
        numAccounts = Integer.parseInt(args[++i]);
      } else if ("-t".equals(args[i])) {
        runFor = Integer.parseInt(args[++i]);
      } else if ("-r".equals(args[i])) {
        ramp = Integer.parseInt(args[++i]);
      } else if ("-s".equals(args[i])) {
        clientSeed = Integer.parseInt(args[++i]);
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
    TransferContext context = new TransferContext(numAccounts, runFor, ramp, clientSeed, true);
    AccountBalanceTransferHandler handler =
        new AccountBalanceTransferHandler(config, context, NAMESPACE, TABLE);
    handler.runTransfer(concurrency);

    System.exit(0);
  }

  private static void printUsageAndExit() {
    System.err.println(
        "TransactionBenchmark -f database.properties "
            + "[-c concurrency] [-n num_accounts] [-t run_time_sec]"
            + " [-r ramp_time] [-s client_seed]");
    System.exit(1);
  }
}
