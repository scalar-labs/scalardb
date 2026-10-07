import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Put;
import com.scalar.db.api.Update;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.service.TransactionFactory;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * One point operation through one layer, from the same Java client, with the CPU it costs:
 *
 * <pre>
 * java -cp JAR Breakdown.java PROPERTIES PG_JDBC_URL FRONTEND_JDBC_URL get|put|insert pg|storage|tx|frontend
 *      THREADS SECONDS POSTMASTER_PID FRONTEND_PID [ROWS]
 * </pre>
 *
 * Layers: pg = PostgreSQL over pgjdbc; storage = ScalarDB DistributedStorage (no transaction);
 * tx = ScalarDB Consensus Commit one-shot get/update; frontend = the PostgreSQL frontend on
 * ScalarDB, over pgjdbc. get reads customers by id; put sets kv.v by id; insert adds an order with
 * a random id for a random customer other than 1. Prints one TSV line:
 * op, layer, threads, ops, tps, p50/p95/p99 ms, then CPU microseconds per operation spent by the
 * client threads, by the PostgreSQL processes and by the frontend process (sampled with ps while
 * the connections are still open).
 */
public class Breakdown {
  interface Op {
    void run(int id) throws Exception;
  }

  public static void main(String[] args) throws Exception {
    String properties = args[0];
    String pgUrl = args[1];
    String frontendUrl = args[2];
    String op = args[3];
    String layer = args[4];
    int threads = Integer.parseInt(args[5]);
    int seconds = Integer.parseInt(args[6]);
    int postmaster = Integer.parseInt(args[7]);
    int frontendPid = Integer.parseInt(args[8]);
    int rows = args.length > 9 ? Integer.parseInt(args[9]) : 20000;

    DistributedStorage storage = null;
    DistributedTransactionManager manager = null;
    List<Connection> connections = new ArrayList<>();
    List<Op> ops = new ArrayList<>();
    for (int t = 0; t < threads; t++) {
      switch (layer) {
        case "pg":
        case "frontend":
          {
            Connection c = DriverManager.getConnection(layer.equals("pg") ? pgUrl : frontendUrl);
            connections.add(c);
            PreparedStatement s =
                c.prepareStatement(
                    op.equals("get")
                        ? "SELECT name, region, tier FROM customers WHERE id = ?"
                        : op.equals("put")
                            ? "UPDATE kv SET v = ? WHERE id = ?"
                            : "INSERT INTO orders (customer_id, order_id, item_id, amount, status,"
                                + " created) VALUES (?, ?, ?, ?, 'new', 1700000000)");
            Random orderIds = new Random(); // unseeded: ids must not repeat across runs
            ops.add(
                id -> {
                  if (op.equals("get")) {
                    s.setInt(1, id);
                    try (ResultSet rs = s.executeQuery()) {
                      while (rs.next()) {
                        rs.getString(1);
                      }
                    }
                  } else if (op.equals("put")) {
                    s.setInt(1, id);
                    s.setInt(2, id);
                    s.executeUpdate();
                  } else {
                    s.setInt(1, Math.max(id, 2));
                    s.setInt(2, 1_000_000 + orderIds.nextInt(1_000_000_000));
                    s.setInt(3, 1 + orderIds.nextInt(10000));
                    s.setDouble(4, 9.5);
                    s.executeUpdate();
                  }
                });
            break;
          }
        case "storage":
          {
            if (storage == null) {
              storage = StorageFactory.create(properties).getStorage();
            }
            DistributedStorage st = storage;
            Random orderIds = new Random(); // unseeded: ids must not repeat across runs
            ops.add(
                id -> {
                  if (op.equals("get")) {
                    st.get(get(id));
                  } else if (op.equals("put")) {
                    st.put(
                        Put.newBuilder()
                            .namespace("bench")
                            .table("kv")
                            .partitionKey(Key.ofInt("id", id))
                            .intValue("v", id)
                            .build());
                  } else {
                    st.put(
                        Put.newBuilder()
                            .namespace("bench")
                            .table("orders")
                            .partitionKey(Key.ofInt("customer_id", Math.max(id, 2)))
                            .clusteringKey(
                                Key.ofInt("order_id", 1_000_000 + orderIds.nextInt(1_000_000_000)))
                            .intValue("item_id", 1 + orderIds.nextInt(10000))
                            .doubleValue("amount", 9.5)
                            .textValue("status", "new")
                            .bigIntValue("created", 1700000000L)
                            .condition(ConditionBuilder.putIfNotExists())
                            .build());
                  }
                });
            break;
          }
        case "tx":
          {
            if (manager == null) {
              manager = TransactionFactory.create(properties).getTransactionManager();
            }
            DistributedTransactionManager m = manager;
            Random orderIds = new Random(); // unseeded: ids must not repeat across runs
            ops.add(
                id -> {
                  if (op.equals("get")) {
                    m.get(get(id));
                  } else if (op.equals("put")) {
                    m.update(
                        Update.newBuilder()
                            .namespace("bench")
                            .table("kv")
                            .partitionKey(Key.ofInt("id", id))
                            .intValue("v", id)
                            .build());
                  } else {
                    m.insert(
                        Insert.newBuilder()
                            .namespace("bench")
                            .table("orders")
                            .partitionKey(Key.ofInt("customer_id", Math.max(id, 2)))
                            .clusteringKey(
                                Key.ofInt("order_id", 1_000_000 + orderIds.nextInt(1_000_000_000)))
                            .intValue("item_id", 1 + orderIds.nextInt(10000))
                            .doubleValue("amount", 9.5)
                            .textValue("status", "new")
                            .bigIntValue("created", 1700000000L)
                            .build());
                  }
                });
            break;
          }
        default:
          throw new IllegalArgumentException(layer);
      }
    }

    // warm up untimed, so the JIT, the connection pools and the server caches settle; the
    // Consensus Commit path is deep, so give it WARMUP seconds (default 10) to get compiled
    int warmup = Integer.parseInt(System.getenv().getOrDefault("WARMUP", "10"));
    runFor(ops, warmup, rows, null);
    List<Integer> pgPids = children(postmaster);
    pgPids.add(postmaster);
    List<Integer> frontendPids =
        frontendPid > 0 ? Collections.singletonList(frontendPid) : Collections.<Integer>emptyList();
    double pgBefore = cpuSeconds(pgPids);
    double feBefore = cpuSeconds(frontendPids);
    long[] clientCpu = new long[1];
    List<Long> times = runFor(ops, seconds, rows, clientCpu);
    double pgCpu = cpuSeconds(pgPids) - pgBefore;
    double feCpu = cpuSeconds(frontendPids) - feBefore;

    Collections.sort(times);
    int n = times.size();
    System.out.printf(
        Locale.ROOT,
        "%s\t%s\t%d\t%d\t%.0f\t%.3f\t%.3f\t%.3f\t%.1f\t%.1f\t%.1f%n",
        op,
        layer,
        threads,
        n,
        n / (double) seconds,
        times.get(n / 2) / 1e6,
        times.get((int) (n * 0.95)) / 1e6,
        times.get((int) (n * 0.99)) / 1e6,
        clientCpu[0] / 1e3 / n,
        pgCpu * 1e6 / n,
        feCpu * 1e6 / n);
    for (Connection c : connections) {
      c.close();
    }
    if (manager != null) {
      manager.close();
    }
    if (storage != null) {
      storage.close();
    }
    System.exit(0);
  }

  private static Get get(int id) {
    return Get.newBuilder()
        .namespace("bench")
        .table("customers")
        .partitionKey(Key.ofInt("id", id))
        .projections("name", "region", "tier")
        .build();
  }

  /** Runs every op in its own thread for the given seconds; returns the latencies in nanos. */
  private static List<Long> runFor(List<Op> ops, int seconds, int rows, long[] clientCpu)
      throws InterruptedException {
    ThreadMXBean mx = ManagementFactory.getThreadMXBean();
    long end = System.nanoTime() + seconds * 1_000_000_000L;
    List<List<Long>> all = new ArrayList<>();
    long[] cpu = new long[ops.size()];
    Throwable[] failure = new Throwable[1];
    CountDownLatch done = new CountDownLatch(ops.size());
    for (int t = 0; t < ops.size(); t++) {
      List<Long> times = new ArrayList<>();
      all.add(times);
      Op o = ops.get(t);
      Random rng = new Random(t * 7919L + seconds);
      int index = t;
      new Thread(
              () -> {
                try {
                  long cpu0 = mx.getCurrentThreadCpuTime();
                  while (System.nanoTime() < end) {
                    int id = 1 + rng.nextInt(rows);
                    long t0 = System.nanoTime();
                    o.run(id);
                    times.add(System.nanoTime() - t0);
                  }
                  cpu[index] = mx.getCurrentThreadCpuTime() - cpu0;
                } catch (Exception e) {
                  failure[0] = e;
                } finally {
                  done.countDown();
                }
              })
          .start();
    }
    done.await();
    if (failure[0] != null) {
      // partial numbers would be misleading, so a failing operation fails the run
      failure[0].printStackTrace();
      System.exit(1);
    }
    List<Long> times = new ArrayList<>();
    for (List<Long> l : all) {
      times.addAll(l);
    }
    if (clientCpu != null) {
      for (long c : cpu) {
        clientCpu[0] += c;
      }
    }
    return times;
  }

  private static List<Integer> children(int pid) throws Exception {
    List<Integer> pids = new ArrayList<>();
    for (String line : lines("pgrep", "-P", String.valueOf(pid))) {
      pids.add(Integer.parseInt(line.trim()));
    }
    return pids;
  }

  /** Total CPU seconds of the processes, from ps (resolution 10 ms). */
  private static double cpuSeconds(List<Integer> pids) throws Exception {
    if (pids.isEmpty()) {
      return 0;
    }
    StringBuilder list = new StringBuilder();
    for (int p : pids) {
      list.append(list.length() == 0 ? "" : ",").append(p);
    }
    double total = 0;
    for (String line : lines("ps", "-o", "time=", "-p", list.toString())) {
      String[] parts = line.trim().split(":");
      double seconds = 0;
      for (String part : parts) {
        seconds = seconds * 60 + Double.parseDouble(part);
      }
      total += seconds;
    }
    return total;
  }

  private static List<String> lines(String... command) throws Exception {
    Process p = new ProcessBuilder(command).redirectErrorStream(true).start();
    List<String> out = new ArrayList<>();
    try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
      for (String line = r.readLine(); line != null; line = r.readLine()) {
        if (!line.trim().isEmpty()) {
          out.add(line);
        }
      }
    }
    p.waitFor();
    return out;
  }
}
