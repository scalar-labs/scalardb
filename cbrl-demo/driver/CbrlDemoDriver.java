import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TransactionState;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
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
 *   <li>{@code check-raw <copyProps> <numAccounts> <initialBalance>} — the negative control:
 *       read the RAW physical state of the copy with the Storage API (no Consensus Commit recovery,
 *       which would mask the damage) and assert the copy is INCONSISTENT before CBRL — a grand total
 *       that drifted from the initial one, and/or in-flight (non-{@code COMMITTED}) records, and/or
 *       missing rows. Fails loudly if the raw copy is already consistent, which would mean CBRL was
 *       never exercised.
 *   <li>{@code check <copyProps> <numAccounts> <initialBalance>} — the final oracle: after CBRL,
 *       assert the copy is FIXED — every account present in EACH namespace (no missing/extra rows),
 *       every record a {@code COMMITTED} Consensus Commit record (no leftover in-flight state), and
 *       the grand total restored to the initial one. Reads the raw physical state for the presence
 *       and all-committed checks, then re-reads transactionally to confirm the copy is a queryable,
 *       conserved Consensus Commit database.
 * </ul>
 */
public final class CbrlDemoDriver {

  private static final String NS_MYSQL = "transfer";
  private static final String NS_PG = "transfer_pg";
  private static final String TABLE = "tx_transfer";
  private static final String ACCOUNT_ID = "account_id";
  private static final String ACCOUNT_TYPE = "account_type";
  private static final String BALANCE = "balance";
  // The Consensus Commit transaction-state metadata column (Attribute.STATE). Read directly off the
  // physical record via the Storage API so the raw checks can tell a COMMITTED record from an
  // in-flight (PREPARED/DELETED) one without going through the transaction layer.
  private static final String TX_STATE = "tx_state";
  private static final int ACCOUNT_TYPE_VALUE = 0;

  private CbrlDemoDriver() {}

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.err.println(
          "Usage: CbrlDemoDriver <populate|open|transfer|restore|check-raw|check> <args...>");
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
      case "check-raw":
        checkRaw(args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3]));
        break;
      case "check":
        check(args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3]));
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
      // Idempotently (re-)open the window on THIS manager so its own daemon cache is refreshed
      // synchronously and every commit below is redo-logged, regardless of daemon poll timing.
      manager.enableRedoLogging(label);
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

  /** The raw physical state of one namespace's table, read via the Storage API (no recovery). */
  private static final class RawState {
    final String namespace;
    final long sum; // Sum of the physical `balance` column across all rows.
    final int rows; // Total rows physically present.
    final int inflight; // Rows whose tx_state is not COMMITTED (PREPARED/DELETED/absent state).
    final int missing; // Accounts in [0, numAccounts) with no row.
    final int outOfRange; // Rows whose account_id is outside [0, numAccounts).
    final int duplicates; // Extra rows sharing an in-range account_id.

    RawState(
        String namespace,
        long sum,
        int rows,
        int inflight,
        int missing,
        int outOfRange,
        int duplicates) {
      this.namespace = namespace;
      this.sum = sum;
      this.rows = rows;
      this.inflight = inflight;
      this.missing = missing;
      this.outOfRange = outOfRange;
      this.duplicates = duplicates;
    }

    boolean consistent() {
      return inflight == 0 && missing == 0 && outOfRange == 0 && duplicates == 0;
    }
  }

  /**
   * Reads every physical row of {@code namespace.tx_transfer} straight off storage with the Storage
   * API. This deliberately bypasses the transaction layer: a transactional read would trigger
   * Consensus Commit lazy recovery and silently repair (mask) the very inconsistency the negative
   * control needs to observe.
   */
  private static RawState scanRaw(DistributedStorage storage, String namespace, int numAccounts)
      throws Exception {
    long sum = 0;
    int rows = 0;
    int inflight = 0;
    int outOfRange = 0;
    int duplicates = 0;
    boolean[] present = new boolean[numAccounts];
    try (Scanner scanner =
        storage.scan(Scan.newBuilder().namespace(namespace).table(TABLE).all().build())) {
      for (Result result : scanner.all()) {
        rows++;
        int accountId = result.getInt(ACCOUNT_ID);
        sum += result.isNull(BALANCE) ? 0 : result.getInt(BALANCE);
        int state = result.isNull(TX_STATE) ? -1 : result.getInt(TX_STATE);
        if (state != TransactionState.COMMITTED.get()) {
          inflight++;
        }
        if (accountId >= 0 && accountId < numAccounts) {
          if (present[accountId]) {
            duplicates++;
          }
          present[accountId] = true;
        } else {
          outOfRange++;
        }
      }
    }
    int missing = 0;
    for (boolean p : present) {
      if (!p) {
        missing++;
      }
    }
    return new RawState(namespace, sum, rows, inflight, missing, outOfRange, duplicates);
  }

  private static void printRawState(RawState state, int numAccounts) {
    System.out.printf(
        "  %-12s sum=%d, rows=%d/%d, in-flight(non-COMMITTED)=%d, missing=%d, out-of-range=%d,"
            + " duplicates=%d%n",
        state.namespace,
        state.sum,
        state.rows,
        numAccounts,
        state.inflight,
        state.missing,
        state.outOfRange,
        state.duplicates);
  }

  /**
   * Negative control: assert the RAW restored copy is inconsistent BEFORE CBRL. The two physical
   * dumps are taken mid-flight and at different instants (MySQL first, coordinator/PostgreSQL
   * later), so cross-storage transfers that commit in between are half-reflected. Read straight off
   * storage (no recovery) and require breakage; if the copy is already consistent, CBRL was never
   * exercised, so fail loudly.
   */
  private static void checkRaw(String propsPath, int numAccounts, int initialBalance)
      throws Exception {
    Properties properties = loadProperties(propsPath);
    StorageFactory factory = StorageFactory.create(properties);
    RawState mysql;
    RawState pg;
    try (DistributedStorage storage = factory.getStorage()) {
      mysql = scanRaw(storage, NS_MYSQL, numAccounts);
      pg = scanRaw(storage, NS_PG, numAccounts);
    }
    long total = mysql.sum + pg.sum;
    long expected = 2L * numAccounts * initialBalance;
    System.out.println("NEGATIVE CONTROL: raw physical state of the copy BEFORE CBRL (no recovery)");
    printRawState(mysql, numAccounts);
    printRawState(pg, numAccounts);
    System.out.printf("  grand total = %d, initial total = %d, drift = %d%n", total, expected, total - expected);

    boolean totalDrifted = total != expected;
    boolean hasInflight = mysql.inflight > 0 || pg.inflight > 0;
    boolean incomplete = !mysql.consistent() || !pg.consistent();
    if (totalDrifted || hasInflight || incomplete) {
      System.out.printf(
          "NEGATIVE CONTROL PASS: raw copy is BROKEN as expected"
              + " (total-drifted=%b, in-flight-records=%b, incomplete=%b).%n",
          totalDrifted, hasInflight, incomplete);
    } else {
      System.out.println(
          "NEGATIVE CONTROL FAIL: raw copy is already consistent — CBRL was NOT exercised."
              + " Widen the backup window (BACKUP_GAP_SECONDS / WARMUP_SECONDS) or raise the"
              + " workload rate.");
      System.exit(1);
    }
  }

  /**
   * Final oracle: assert the copy is FIXED AFTER CBRL. Reads the raw physical state (Storage API) to
   * require, in EACH namespace, exactly {@code numAccounts} rows with no missing/extra/duplicate
   * accounts and every record a {@code COMMITTED} Consensus Commit record (zero in-flight), plus a
   * grand total restored to the initial one. Then re-reads transactionally to confirm the repaired
   * copy is a queryable, conserved Consensus Commit database.
   */
  private static void check(String propsPath, int numAccounts, int initialBalance) throws Exception {
    Properties properties = loadProperties(propsPath);

    // --- Physical oracle: completeness + all-committed + conserved total, straight off storage. ---
    StorageFactory storageFactory = StorageFactory.create(properties);
    RawState mysql;
    RawState pg;
    try (DistributedStorage storage = storageFactory.getStorage()) {
      mysql = scanRaw(storage, NS_MYSQL, numAccounts);
      pg = scanRaw(storage, NS_PG, numAccounts);
    }
    long rawTotal = mysql.sum + pg.sum;
    long expected = 2L * numAccounts * initialBalance;
    System.out.println("FINAL ORACLE: raw physical state of the copy AFTER CBRL");
    printRawState(mysql, numAccounts);
    printRawState(pg, numAccounts);
    System.out.printf("  grand total = %d, expected = %d%n", rawTotal, expected);

    boolean complete =
        mysql.rows == numAccounts && pg.rows == numAccounts && mysql.consistent() && pg.consistent();
    boolean allCommitted = mysql.inflight == 0 && pg.inflight == 0;
    boolean conserved = rawTotal == expected;

    // --- Transactional confirmation: the copy reads back as a valid, conserved CC database. ---
    TransactionFactory transactionFactory = TransactionFactory.create(properties);
    long txTotal = 0;
    int txPresent = 0;
    try (DistributedTransactionManager manager = transactionFactory.getTransactionManager()) {
      for (int accountId = 0; accountId < numAccounts; accountId++) {
        DistributedTransaction tx = manager.start();
        try {
          Optional<Result> mysqlRow = tx.get(getBalance(NS_MYSQL, accountId));
          Optional<Result> pgRow = tx.get(getBalance(NS_PG, accountId));
          tx.commit();
          if (mysqlRow.isPresent()) {
            txTotal += mysqlRow.get().getInt(BALANCE);
            txPresent++;
          }
          if (pgRow.isPresent()) {
            txTotal += pgRow.get().getInt(BALANCE);
            txPresent++;
          }
        } catch (Exception e) {
          tx.abort();
          throw e;
        }
      }
    }
    System.out.printf(
        "  transactional re-read: grand total = %d (%d/%d rows present)%n",
        txTotal, txPresent, 2 * numAccounts);
    boolean txConserved = txTotal == expected && txPresent == 2 * numAccounts;

    if (complete && allCommitted && conserved && txConserved) {
      System.out.println(
          "PASS: restored copy is complete, all-COMMITTED, and conserved (raw + transactional).");
    } else {
      System.out.printf(
          "FAIL: restored copy still broken"
              + " (complete=%b, all-committed=%b, conserved=%b, tx-conserved=%b).%n",
          complete, allCommitted, conserved, txConserved);
      System.exit(1);
    }
  }
}
