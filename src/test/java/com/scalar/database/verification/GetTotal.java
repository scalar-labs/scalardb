package com.scalar.database.verification;

import com.scalar.database.api.Result;
import com.scalar.database.config.DatabaseConfig;
import com.scalar.database.transaction.consensuscommit.TransactionResult;
import com.scalar.database.util.AccountBalanceTransferHandler;
import com.scalar.database.util.TransferContext;
import java.io.File;
import java.io.IOException;
import java.util.List;

public class GetTotal {
  private static final String NAMESPACE = "verification";
  private static final String TABLE = "tx_simple_transfer";
  private static final int DEFAULT_NUM_ACCOUNTS = 10;
  private static int numAccounts = DEFAULT_NUM_ACCOUNTS;
  private static String propertiesFile;

  public static void main(String[] args) throws IOException {
    for (int i = 0; i < args.length; ++i) {
      if ("-n".equals(args[i])) {
        numAccounts = Integer.parseInt(args[++i]);
      } else if ("-f".equals(args[i])) {
        propertiesFile = args[++i];
      } else if ("-help".equals(args[i])) {
        printUsageAndExit();
      }
    }

    DatabaseConfig config = new DatabaseConfig(new File(propertiesFile));
    TransferContext context = new TransferContext(numAccounts, 0, 0, 0);
    AccountBalanceTransferHandler handler =
        new AccountBalanceTransferHandler(config, context, NAMESPACE, TABLE);

    List<Result> results = handler.readRecordsWithRetry();
    int totalVersion = results.stream().mapToInt(r -> ((TransactionResult) r).getVersion()).sum();
    int totalBalance = handler.calcTotalBalance(results);

    System.out.println("Total balance: " + totalBalance);
    System.out.println("Total version: " + totalVersion);

    System.exit(0);
  }

  private static void printUsageAndExit() {
    System.err.println("GetTotal -f database.properties [-n num_accounts]");
    System.exit(1);
  }
}
