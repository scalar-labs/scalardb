import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Concurrency checks for the PostgreSQL frontend through pgjdbc (prepared statements, as BenchBase
 * uses). Run with the frontend's fat jar on the classpath, which contains pgjdbc:
 *
 * <pre>java -cp frontend.jar Concurrency.java PORT NAMESPACE SERIALIZABLE|SNAPSHOT</pre>
 *
 * Invariants that must hold at every isolation level fail the run; anomalies that SNAPSHOT allows
 * (write skew, phantoms, read skew) are counted, and must be zero under SERIALIZABLE.
 */
public class Concurrency {
  static String url;
  static final Map<String, AtomicInteger> states = new ConcurrentHashMap<>();
  static final List<String> unexpected = new ArrayList<>();
  static final List<String> failures = new ArrayList<>();
  static final int THREADS = 8;

  public static void main(String[] args) throws Exception {
    url = "jdbc:postgresql://localhost:" + args[0] + "/" + args[1];
    boolean serializable = args[2].equalsIgnoreCase("SERIALIZABLE");
    setUp(args[1]);

    lostUpdateReadModifyWrite();
    lostUpdateServerSide();
    uniqueInsertRace();
    long readSkew = bank();
    long writeSkew = doctors();
    long phantoms = bookings();
    sessionRecovery();

    System.out.println("SQLSTATE histogram: " + states);
    synchronized (unexpected) {
      for (String u : unexpected.subList(0, Math.min(10, unexpected.size()))) {
        System.out.println("  unexpected: " + u);
      }
    }
    System.out.printf(
        "anomalies (%s): read skew=%d, write skew=%d, phantom inserts=%d%n",
        args[2], readSkew, writeSkew, phantoms);
    if (serializable && readSkew + writeSkew + phantoms > 0) {
      failures.add("SERIALIZABLE allowed an anomaly");
    }
    if (!unexpected.isEmpty()) {
      failures.add(unexpected.size() + " errors with an unexpected SQLSTATE");
    }
    System.out.println(failures.isEmpty() ? "PASS" : "FAIL: " + failures);
    System.exit(failures.isEmpty() ? 0 : 1);
  }

  static Connection connect() throws SQLException {
    Properties p = new Properties();
    p.setProperty("user", "postgres");
    Connection c = DriverManager.getConnection(url, p);
    c.setAutoCommit(false);
    return c;
  }

  static void setUp(String namespace) throws SQLException {
    Properties p = new Properties();
    p.setProperty("user", "postgres");
    try (Connection c = DriverManager.getConnection(url, p);
        Statement s = c.createStatement()) {
      s.execute("CREATE COORDINATOR TABLES IF NOT EXISTS");
      s.execute("CREATE SCHEMA IF NOT EXISTS " + namespace);
      String[] tables = {
        "kv (id INT, v INT, PRIMARY KEY (id))",
        "acct (id INT, bal INT, PRIMARY KEY (id))",
        "oncall (grp INT, doc INT, on_call BOOLEAN, PRIMARY KEY (grp, doc))",
        "booking (room INT, who INT, PRIMARY KEY (room, who))",
        "uniq (id INT, who INT, PRIMARY KEY (id))"
      };
      for (String t : tables) {
        try {
          s.execute("CREATE TABLE " + t);
        } catch (SQLException e) {
          s.execute("TRUNCATE " + t.substring(0, t.indexOf(' ')));
        }
      }
      s.execute("INSERT INTO kv VALUES (1, 0), (2, 0)");
      StringBuilder accounts = new StringBuilder("INSERT INTO acct VALUES ");
      for (int i = 1; i <= 10; i++) {
        accounts.append(i == 1 ? "" : ", ").append("(").append(i).append(", 100)");
      }
      s.execute(accounts.toString());
      for (int g = 0; g < 100; g++) {
        s.execute("INSERT INTO oncall VALUES (" + g + ", 1, true), (" + g + ", 2, true)");
      }
    }
  }

  /** Records a failed attempt; returns true if the client should retry (40001). */
  static boolean failed(SQLException e, String... expected) {
    String state = e.getSQLState();
    states.computeIfAbsent(state, k -> new AtomicInteger()).incrementAndGet();
    if (state.equals("40001")) {
      return true;
    }
    for (String x : expected) {
      if (x.equals(state)) {
        return false;
      }
    }
    synchronized (unexpected) {
      unexpected.add(state + " " + e.getMessage());
    }
    return false;
  }

  static void rollback(Connection c) {
    try {
      c.rollback();
    } catch (SQLException e) {
      synchronized (unexpected) {
        unexpected.add("rollback failed: " + e.getMessage());
      }
    }
  }

  interface Body {
    void run(Connection c, int thread, int i) throws Exception;
  }

  static void parallel(int threads, int iterations, Body body) throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    List<Future<?>> futures = new ArrayList<>();
    for (int t = 0; t < threads; t++) {
      int thread = t;
      futures.add(
          pool.submit(
              () -> {
                try (Connection c = connect()) {
                  for (int i = 0; i < iterations; i++) {
                    body.run(c, thread, i);
                  }
                }
                return null;
              }));
    }
    for (Future<?> f : futures) {
      f.get();
    }
    pool.shutdown();
    pool.awaitTermination(1, TimeUnit.MINUTES);
  }

  static int queryInt(Connection c, String sql, Object... params) throws SQLException {
    try (PreparedStatement ps = c.prepareStatement(sql)) {
      for (int i = 0; i < params.length; i++) {
        ps.setObject(i + 1, params[i]);
      }
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    }
  }

  static int update(Connection c, String sql, Object... params) throws SQLException {
    try (PreparedStatement ps = c.prepareStatement(sql)) {
      for (int i = 0; i < params.length; i++) {
        ps.setObject(i + 1, params[i]);
      }
      return ps.executeUpdate();
    }
  }

  static void check(String name, long expected, long actual) {
    System.out.printf("%-28s expected=%d actual=%d%n", name, expected, actual);
    if (expected != actual) {
      failures.add(name);
    }
  }

  /** Read v, write v + 1 in one transaction; every increment must survive. */
  static void lostUpdateReadModifyWrite() throws Exception {
    int perThread = 30;
    AtomicLong retries = new AtomicLong();
    parallel(
        THREADS,
        perThread,
        (c, t, i) -> {
          while (true) {
            try {
              int v = queryInt(c, "SELECT v FROM kv WHERE id = ?", 1);
              update(c, "UPDATE kv SET v = ? WHERE id = ?", v + 1, 1);
              c.commit();
              return;
            } catch (SQLException e) {
              rollback(c);
              if (!failed(e)) {
                return;
              }
              retries.incrementAndGet();
            }
          }
        });
    try (Connection c = connect()) {
      check("lost update (read+write)", THREADS * perThread, queryInt(c, "SELECT v FROM kv WHERE id = 1"));
      c.commit();
    }
    System.out.println("  retries: " + retries);
  }

  /** UPDATE ... SET v = v + 1 in auto-commit; every increment must survive. */
  static void lostUpdateServerSide() throws Exception {
    int perThread = 30;
    parallel(
        THREADS,
        perThread,
        (c, t, i) -> {
          c.setAutoCommit(true);
          while (true) {
            try {
              if (update(c, "UPDATE kv SET v = v + 1 WHERE id = ?", 2) != 1) {
                synchronized (unexpected) {
                  unexpected.add("UPDATE v = v + 1 affected a row count other than 1");
                }
              }
              return;
            } catch (SQLException e) {
              if (!failed(e)) {
                return;
              }
            }
          }
        });
    try (Connection c = connect()) {
      check("lost update (v = v + 1)", THREADS * perThread, queryInt(c, "SELECT v FROM kv WHERE id = 2"));
      c.commit();
    }
  }

  /** All threads insert the same key each round: exactly one insert may succeed. */
  static void uniqueInsertRace() throws Exception {
    int rounds = 30;
    AtomicInteger succeeded = new AtomicInteger();
    CyclicBarrier barrier = new CyclicBarrier(THREADS);
    parallel(
        THREADS,
        rounds,
        (c, t, i) -> {
          barrier.await();
          try {
            update(c, "INSERT INTO uniq VALUES (?, ?)", i, t);
            c.commit();
            succeeded.incrementAndGet();
          } catch (SQLException e) {
            rollback(c);
            failed(e, "23505");
          }
        });
    try (Connection c = connect()) {
      check("unique insert: successes", rounds, succeeded.get());
      check("unique insert: rows", rounds, queryInt(c, "SELECT count(*) FROM uniq"));
      c.commit();
    }
  }

  /**
   * Transfers between 10 accounts while readers sum all balances in read-only and read-write
   * transactions. The total must stay 1000; a committed reader that saw another total is a read
   * skew.
   */
  static long bank() throws Exception {
    AtomicLong readSkew = new AtomicLong();
    AtomicLong committedReads = new AtomicLong();
    AtomicLong readAttempts = new AtomicLong();
    long[][] byReader = new long[2][3]; // [read-only, read-write] x [attempts, committed, skewed]
    int transfersPerWriter = 40;
    AtomicInteger transfersLeft = new AtomicInteger((THREADS - 2) * transfersPerWriter);
    parallel(
        THREADS,
        100_000,
        (c, t, i) -> {
          Random random = new Random(t * 1000L + i);
          if (t < 2) {
            // readers keep reading, with retries, for as long as the writers run
            if (transfersLeft.get() <= 0) {
              return;
            }
            readAttempts.incrementAndGet();
            byReader[t][0]++;
            c.setReadOnly(t == 0); // one read-only reader, one ordinary reader
            try {
              int sum = 0;
              for (int a = 1; a <= 10; a++) {
                sum += queryInt(c, "SELECT bal FROM acct WHERE id = ?", a);
              }
              c.commit();
              committedReads.incrementAndGet();
              byReader[t][1]++;
              if (sum != 1000) {
                readSkew.incrementAndGet();
                byReader[t][2]++;
              }
            } catch (SQLException e) {
              rollback(c);
              failed(e);
            }
            return;
          }
          if (i >= transfersPerWriter) {
            return;
          }
          int from = 1 + random.nextInt(10);
          int to = 1 + (from + random.nextInt(9)) % 10;
          int amount = 1 + random.nextInt(5);
          while (true) {
            try {
              int a = queryInt(c, "SELECT bal FROM acct WHERE id = ?", from);
              int b = queryInt(c, "SELECT bal FROM acct WHERE id = ?", to);
              update(c, "UPDATE acct SET bal = ? WHERE id = ?", a - amount, from);
              update(c, "UPDATE acct SET bal = ? WHERE id = ?", b + amount, to);
              c.commit();
              transfersLeft.decrementAndGet();
              return;
            } catch (SQLException e) {
              rollback(c);
              if (!failed(e)) {
                transfersLeft.decrementAndGet();
                return;
              }
            }
          }
        });
    try (Connection c = connect()) {
      check("bank: final total", 1000, queryInt(c, "SELECT sum(bal) FROM acct"));
      c.commit();
    }
    System.out.println(
        "  reads during transfers: attempts="
            + readAttempts
            + " committed="
            + committedReads
            + " inconsistent="
            + readSkew);
    System.out.printf(
        "  read-only reader: attempts=%d committed=%d skewed=%d;"
            + " read-write reader: attempts=%d committed=%d skewed=%d%n",
        byReader[0][0], byReader[0][1], byReader[0][2],
        byReader[1][0], byReader[1][1], byReader[1][2]);
    if (committedReads.get() == 0) {
      System.out.println("  WARNING: no read committed while transfers ran; read skew not tested");
    }
    return readSkew.get();
  }

  /**
   * Classic write skew: two doctors on call, each goes off call if the other is still on. Both
   * read before either writes. Ending with nobody on call is the anomaly.
   */
  static long doctors() throws Exception {
    int groups = 100;
    CyclicBarrier read = new CyclicBarrier(2);
    parallel(
        2,
        groups,
        (c, t, g) -> {
          int on = -1;
          try {
            on = queryInt(c, "SELECT count(*) FROM oncall WHERE grp = ? AND on_call", g);
          } catch (SQLException e) {
            rollback(c);
            failed(e);
          }
          read.await(); // both transactions have read; reached once per round even on error
          if (on < 0) {
            return;
          }
          try {
            if (on >= 2) {
              update(c, "UPDATE oncall SET on_call = false WHERE grp = ? AND doc = ?", g, t + 1);
            }
            c.commit();
          } catch (SQLException e) {
            rollback(c);
            failed(e);
          }
        });
    try (Connection c = connect()) {
      int nobody =
          queryInt(
              c,
              "SELECT count(*) FROM (SELECT grp FROM oncall GROUP BY grp"
                  + " HAVING sum(CASE WHEN on_call THEN 1 ELSE 0 END) = 0) x");
      c.commit();
      return nobody;
    }
  }

  /** Book a room only if it has no booking: two concurrent bookers may not both succeed. */
  static long bookings() throws Exception {
    int rooms = 100;
    CyclicBarrier read = new CyclicBarrier(2);
    parallel(
        2,
        rooms,
        (c, t, room) -> {
          int taken = -1;
          try {
            taken = queryInt(c, "SELECT count(*) FROM booking WHERE room = ?", room);
          } catch (SQLException e) {
            rollback(c);
            failed(e);
          }
          read.await();
          if (taken < 0) {
            return;
          }
          try {
            if (taken == 0) {
              update(c, "INSERT INTO booking VALUES (?, ?)", room, t);
            }
            c.commit();
          } catch (SQLException e) {
            rollback(c);
            failed(e);
          }
        });
    try (Connection c = connect()) {
      int doubled =
          queryInt(
              c,
              "SELECT count(*) FROM (SELECT room FROM booking GROUP BY room HAVING count(*) > 1) x");
      c.commit();
      return doubled;
    }
  }

  /** After an error inside a transaction the session must refuse work until ROLLBACK, then work. */
  static void sessionRecovery() throws Exception {
    try (Connection c = connect()) {
      update(c, "UPDATE kv SET v = v WHERE id = ?", 1);
      try {
        queryInt(c, "SELECT nosuch FROM kv");
      } catch (SQLException e) {
        failed(e, "42703");
      }
      try {
        queryInt(c, "SELECT 1");
        failures.add("session accepted a statement after an error");
      } catch (SQLException e) {
        failed(e, "25P02");
      }
      c.rollback();
      check("session usable after rollback", 1, queryInt(c, "SELECT 1"));
      c.commit();
    }
  }
}
