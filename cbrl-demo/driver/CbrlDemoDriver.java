import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.io.Key;
import com.scalar.db.service.TransactionFactory;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Minimal standalone workload/verification driver for the CBRL end-to-end demo. It uses ONLY the
 * local ScalarDB core API (TransactionFactory / DistributedTransactionManager, Get / Put), so it
 * exercises the CBRL-enabled build in this worktree rather than any published artifact.
 *
 * <p>Data model: two user namespaces, {@code transfer} (mapped to MySQL) and {@code transfer_pg}
 * (mapped to PostgreSQL). Each holds {@code numAccounts} single-row accounts (partition key
 * account_id, clustering key account_type=0, INT column balance). A transfer moves an amount from an
 * account in one namespace to an account in the OTHER namespace within a single ScalarDB
 * transaction, so every commit is a genuine two-storage transaction whose atomic state is recorded
 * by the coordinator (PostgreSQL). The grand total across both namespaces is therefore invariant.
 *
 * <p>Subcommands:
 *
 * <ul>
 *   <li>{@code populate <props> <numAccounts> <initialBalance>} — seed both namespaces (window
 *       closed, so these are the pre-window base and the physical copy is load-bearing for them).
 *   <li>{@code open <props> <label>} — open the backup window (enableRedoLogging).
 *   <li>{@code transfer <props> <label> <numAccounts> <runSeconds> <threads>} — run the
 *       cross-storage transfer workload under the open window (re-opens idempotently first so this
 *       process's own commits are redo-logged).
 *   <li>{@code restore <copyProps> <label>} — run CbrlRestore over the restored copy.
 *   <li>{@code validate <copyProps> <numAccounts> <initialBalance> <consistent|broken>} — the oracle
 *       for both runs: read the copy through a single Consensus Commit cross-partition {@code
 *       ScanAll}, which applies lazy recovery to every in-flight record it touches, then assert the
 *       grand total and row counts. {@code consistent} (the with-CBRL run) requires a complete,
 *       conserved copy; {@code broken} (the without-CBRL run) requires the copy to remain drifted
 *       AFTER recovery alone, proving that only the CBRL redo can reconstruct the torn committed
 *       writes. {@code broken} fails loudly if the copy is already consistent, which would mean the
 *       torn-write scenario was never produced.
 * </ul>
 */
public final class CbrlDemoDriver {

  private static final String NS_MYSQL = "transfer";
  private static final String NS_PG = "transfer_pg";
  private static final String TABLE = "tx_transfer";
  private static final String ACCOUNT_ID = "account_id";
  private static final String ACCOUNT_TYPE = "account_type";
  private static final String BALANCE = "balance";
  private static final int ACCOUNT_TYPE_VALUE = 0;

  private CbrlDemoDriver() {}

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.err.println(
          "Usage: CbrlDemoDriver <populate|open|transfer|restore|validate> <args...>");
      System.exit(2);
    }
    String command = args[0];
    switch (command) {
      case "populate":
        populate(args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3]));
        break;
      case "open":
        open(args[1], args[2]);
        break;
      case "transfer":
        transfer(
            args[1],
            args[2],
            Integer.parseInt(args[3]),
            Integer.parseInt(args[4]),
            Integer.parseInt(args[5]));
        break;
      case "restore":
        CbrlRestoreMain.main(new String[] {args[1], args[2]});
        break;
      case "validate":
        validate(args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3]), args[4]);
        break;
      default:
        System.err.println("Unknown command: " + command);
        System.exit(2);
    }
  }

  private static Properties loadProperties(String path) throws Exception {
    Properties properties = new Properties();
    try (InputStream in = new FileInputStream(path)) {
      properties.load(in);
    }
    return properties;
  }

  private static Put putBalance(String namespace, int accountId, int balance) {
    return Put.newBuilder()
        .namespace(namespace)
        .table(TABLE)
        .partitionKey(Key.ofInt(ACCOUNT_ID, accountId))
        .clusteringKey(Key.ofInt(ACCOUNT_TYPE, ACCOUNT_TYPE_VALUE))
        .intValue(BALANCE, balance)
        .build();
  }

  private static Get getBalance(String namespace, int accountId) {
    return Get.newBuilder()
        .namespace(namespace)
        .table(TABLE)
        .partitionKey(Key.ofInt(ACCOUNT_ID, accountId))
        .clusteringKey(Key.ofInt(ACCOUNT_TYPE, ACCOUNT_TYPE_VALUE))
        .build();
  }

  private static void populate(String propsPath, int numAccounts, int initialBalance)
      throws Exception {
    Properties properties = loadProperties(propsPath);
    TransactionFactory factory = TransactionFactory.create(properties);
    try (DistributedTransactionManager manager = factory.getTransactionManager()) {
      for (String namespace : new String[] {NS_MYSQL, NS_PG}) {
        for (int accountId = 0; accountId < numAccounts; accountId++) {
          DistributedTransaction tx = manager.start();
          try {
            tx.put(putBalance(namespace, accountId, initialBalance));
            tx.commit();
          } catch (Exception e) {
            tx.abort();
            throw e;
          }
        }
      }
    }
    long total = 2L * numAccounts * initialBalance;
    System.out.printf(
        "Populated %d accounts in each of {%s, %s} at balance %d. Initial grand total = %d.%n",
        numAccounts, NS_MYSQL, NS_PG, initialBalance, total);
  }

  private static void open(String propsPath, String label) throws Exception {
    Properties properties = loadProperties(propsPath);
    TransactionFactory factory = TransactionFactory.create(properties);
    try (DistributedTransactionManager manager = factory.getTransactionManager()) {
      manager.enableRedoLogging(label);
    }
    System.out.printf("Opened backup window (redo logging enabled) for label '%s'.%n", label);
  }

  private static void transfer(
      String propsPath, String label, int numAccounts, int runSeconds, int threads)
      throws Exception {
    Properties properties = loadProperties(propsPath);
    TransactionFactory factory = TransactionFactory.create(properties);
    AtomicLong committed = new AtomicLong();
    AtomicLong aborted = new AtomicLong();
    AtomicBoolean stop = new AtomicBoolean(false);
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try (DistributedTransactionManager manager = factory.getTransactionManager()) {
      // The workload does NOT open the window itself: like a real application it just commits, and
      // its BackupModeDaemon picks up the window that a separate `open` step opens mid-run. Commits
      // before that are the pre-window base (carried only by the physical copy); commits after the
      // daemon observes the window log full redo.
      System.out.printf(
          "Transfer workload starting: %d threads, %ds, %d accounts/namespace, label '%s'.%n",
          threads, runSeconds, numAccounts, label);
      List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        futures.add(
            executor.submit(
                () -> {
                  while (!stop.get()) {
                    if (transferOnce(manager, numAccounts)) {
                      committed.incrementAndGet();
                    } else {
                      aborted.incrementAndGet();
                    }
                  }
                }));
      }
      Thread.sleep(runSeconds * 1000L);
      stop.set(true);
      for (Future<?> future : futures) {
        future.get();
      }
    } finally {
      executor.shutdownNow();
    }
    System.out.printf(
        "Transfer workload finished: committed=%d, aborted/retried=%d. Window left OPEN.%n",
        committed.get(), aborted.get());
  }

  /**
   * One balance-preserving cross-storage transfer between a random account in one namespace and a
   * random account in the other. Returns true if committed, false if it gave up after retries.
   */
  private static boolean transferOnce(DistributedTransactionManager manager, int numAccounts) {
    boolean mysqlToPg = ThreadLocalRandom.current().nextBoolean();
    String fromNs = mysqlToPg ? NS_MYSQL : NS_PG;
    String toNs = mysqlToPg ? NS_PG : NS_MYSQL;
    int fromId = ThreadLocalRandom.current().nextInt(numAccounts);
    int toId = ThreadLocalRandom.current().nextInt(numAccounts);
    int amount = 1 + ThreadLocalRandom.current().nextInt(100);
    for (int attempt = 0; attempt < 50; attempt++) {
      DistributedTransaction tx = null;
      try {
        tx = manager.start();
        Optional<Result> from = tx.get(getBalance(fromNs, fromId));
        Optional<Result> to = tx.get(getBalance(toNs, toId));
        int fromBalance = from.map(r -> r.getInt(BALANCE)).orElse(0);
        int toBalance = to.map(r -> r.getInt(BALANCE)).orElse(0);
        tx.put(putBalance(fromNs, fromId, fromBalance - amount));
        tx.put(putBalance(toNs, toId, toBalance + amount));
        tx.commit();
        return true;
      } catch (Exception e) {
        if (tx != null) {
          try {
            tx.abort();
          } catch (Exception ignored) {
            // Best-effort abort.
          }
        }
        try {
          Thread.sleep(5);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          return false;
        }
      }
    }
    return false;
  }

  private static Scan scanAll(String namespace) {
    return Scan.newBuilder().namespace(namespace).table(TABLE).all().build();
  }

  /**
   * Validates the restored copy through a Consensus Commit cross-partition {@code ScanAll} of both
   * user tables within a single transaction. The transactional read triggers Consensus Commit lazy
   * recovery on every in-flight record it touches (a PREPARED row rolls back if its transaction
   * expired or aborted, forward if it committed), so this observes the copy after ALL the repair
   * that recovery alone can do. It then sums the balances and counts the rows present.
   *
   * <p>The same read backs both runs; only the expectation differs:
   *
   * <ul>
   *   <li>{@code consistent} (the with-CBRL run): require every account present in each namespace
   *       and the grand total restored to the initial one.
   *   <li>{@code broken} (the without-CBRL run): require the opposite. Recovery resolves in-flight
   *       records, but it cannot reconstruct a committed write that never reached the frozen-earlier
   *       storage — such a record reads back as a perfectly valid COMMITTED row with a stale value —
   *       so those torn writes survive as a drifted total that only the CBRL redo can repair. Fails
   *       loudly if the copy is already consistent, which would mean the torn-write scenario was
   *       never produced (widen the backup window or raise the workload rate).
   * </ul>
   */
  private static void validate(String propsPath, int numAccounts, int initialBalance, String expect)
      throws Exception {
    Properties properties = loadProperties(propsPath);
    long expected = 2L * numAccounts * initialBalance;
    TransactionFactory factory = TransactionFactory.create(properties);
    long total = 0;
    int mysqlRows = 0;
    int pgRows = 0;
    try (DistributedTransactionManager manager = factory.getTransactionManager()) {
      DistributedTransaction tx = manager.start();
      try {
        List<Result> mysqlResults = tx.scan(scanAll(NS_MYSQL));
        List<Result> pgResults = tx.scan(scanAll(NS_PG));
        tx.commit();
        for (Result result : mysqlResults) {
          total += result.isNull(BALANCE) ? 0 : result.getInt(BALANCE);
          mysqlRows++;
        }
        for (Result result : pgResults) {
          total += result.isNull(BALANCE) ? 0 : result.getInt(BALANCE);
          pgRows++;
        }
      } catch (Exception e) {
        tx.abort();
        throw e;
      }
    }

    boolean complete = mysqlRows == numAccounts && pgRows == numAccounts;
    boolean conserved = total == expected;
    System.out.printf(
        "Consensus Commit ScanAll of the copy: %s=%d/%d rows, %s=%d/%d rows, grand total = %d,"
            + " expected = %d, drift = %d%n",
        NS_MYSQL, mysqlRows, numAccounts, NS_PG, pgRows, numAccounts, total, expected,
        total - expected);

    if ("consistent".equals(expect)) {
      if (complete && conserved) {
        System.out.println(
            "PASS (with CBRL): the restored copy is complete and conserved under Consensus Commit.");
      } else {
        System.out.printf(
            "FAIL (with CBRL): the copy is still broken (complete=%b, conserved=%b).%n",
            complete, conserved);
        System.exit(1);
      }
    } else if ("broken".equals(expect)) {
      if (!complete || !conserved) {
        System.out.printf(
            "PASS (without CBRL): recovery alone leaves the copy BROKEN — CBRL redo is required"
                + " (complete=%b, conserved=%b).%n",
            complete, conserved);
      } else {
        System.out.println(
            "FAIL (without CBRL): the copy is already consistent without the CBRL redo, so the"
                + " torn-write scenario was not produced. Widen the backup window"
                + " (BACKUP_GAP_SECONDS / WARMUP_SECONDS) or raise the workload rate.");
        System.exit(1);
      }
    } else {
      System.err.println("Unknown validate expectation: " + expect + " (want consistent|broken)");
      System.exit(2);
    }
  }
}
