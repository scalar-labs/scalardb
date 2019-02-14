package com.scalar.database.verification;

import com.scalar.database.api.Result;
import com.scalar.database.config.DatabaseConfig;
import com.scalar.database.transaction.consensuscommit.TransactionResult;
import com.scalar.database.util.AccountBalanceTransferHandler;
import com.scalar.database.util.TransactionUtility;
import com.scalar.database.util.TransferContext;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** You can create tables for this program by using `script/sample_schema/tx_transfer.cql` */
public class TransactionVerification {
  private static final int DEFAULT_POPULATION_CONCURRENCY = 1;
  private static final int DEFAULT_CONCURRENCY = 5;
  private static final int DEFAULT_NUM_ACCOUNTS = 10;
  private static final int DEFAULT_RUN_FOR_SEC = 60;
  private static int concurrency = DEFAULT_CONCURRENCY;
  private static int populationConcurrency = DEFAULT_POPULATION_CONCURRENCY;
  private static int numAccounts = DEFAULT_NUM_ACCOUNTS;
  private static int runFor = DEFAULT_RUN_FOR_SEC;
  private static int clientSeed = 0;
  private static String propertiesFile = null;
  private static String killerStarter = null;
  private static String killerStopper = null;

  public static void main(String[] args) throws IOException {
    for (int i = 0; i < args.length; ++i) {
      if ("-c".equals(args[i])) {
        concurrency = Integer.parseInt(args[++i]);
      } else if ("-n".equals(args[i])) {
        numAccounts = Integer.parseInt(args[++i]);
      } else if ("-t".equals(args[i])) {
        runFor = Integer.parseInt(args[++i]);
      } else if ("-s".equals(args[i])) {
        clientSeed = Integer.parseInt(args[++i]);
      } else if ("-pc".equals(args[i])) {
        populationConcurrency = Integer.parseInt(args[++i]);
      } else if ("-f".equals(args[i])) {
        propertiesFile = args[++i];
      } else if ("-killer_starter".equals(args[i])) {
        killerStarter = args[++i];
      } else if ("-killer_stopper".equals(args[i])) {
        killerStopper = args[++i];
      } else if ("-help".equals(args[i])) {
        printUsageAndExit();
      }
    }

    DatabaseConfig config = null;
    if (propertiesFile != null) {
      config = new DatabaseConfig(new File(propertiesFile));
    }

    TransferContext context = new TransferContext(numAccounts, runFor, 0, clientSeed);
    AccountBalanceTransferHandler handler = new AccountBalanceTransferHandler(config, context);

    handler.populateRecords(populationConcurrency);

    CompletableFuture.runAsync(() -> TransactionUtility.executeCommand(killerStarter));

    handler.runTransfer(concurrency);

    try {
      CompletableFuture.runAsync(() -> TransactionUtility.executeCommand(killerStopper)).get();
    } catch (Exception e) {
      e.printStackTrace();
    }

    List<Result> results = handler.readRecordsWithRetry();

    TransactionUtility.checkCoordinatorWithRetry(config, context);

    boolean isConsistent = checkConsistency(handler, results, context);

    if (isConsistent) {
      System.out.println("SUCCESS");
      System.exit(0);
    }
    System.out.println("FAIL");
    System.exit(1);
  }

  private static boolean checkConsistency(
      AccountBalanceTransferHandler handler, List<Result> results, TransferContext context) {
    int totalVersion =
        results.stream().mapToInt(r -> ((TransactionResult) r).getVersion() - 1).sum();
    int totalBalance = handler.calcTotalBalance(results);
    int expectedTotalVersion = context.getCommitted();
    int expectedTotalBalance = handler.calcTotalInitialBalance();

    System.out.println("total version: " + totalVersion);
    System.out.println("expected total version: " + expectedTotalVersion);
    System.out.println("total balance: " + totalBalance);
    System.out.println("expected total balance: " + expectedTotalBalance);

    boolean isConsistent = true;
    if (totalVersion != expectedTotalVersion) {
      System.out.println("version mismatch !");
      isConsistent = false;
    }
    if (totalBalance != expectedTotalBalance) {
      System.out.println("balance mismatch !");
      isConsistent = false;
    }
    return isConsistent;
  }

  private static void printUsageAndExit() {
    System.err.println(
        "TransactionVerification [-f database.properties] "
            + "[-c concurrency] [-n num_accounts] [-t run_time_sec] "
            + "[-s client_seed] [-pc population_concurrency]"
            + "[-killer_starter command_path] [-killer_stopper command_path]");
    System.exit(1);
  }
}
