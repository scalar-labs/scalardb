import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Scan;
import com.scalar.db.io.Key;
import com.scalar.db.service.TransactionFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * The third configuration: the same reads as point_get and range_scan issued through the ScalarDB
 * Java API directly, without SQL, to measure what the frontend adds on top of ScalarDB itself.
 *
 * <pre>java -cp ../build/libs/scalardb-postgres-frontend-*.jar DirectBaseline.java PROPERTIES get|scan [threads] [seconds] [ncust]</pre>
 */
public class DirectBaseline {
  public static void main(String[] args) throws Exception {
    String op = args[1];
    int threads = args.length > 2 ? Integer.parseInt(args[2]) : 4;
    int seconds = args.length > 3 ? Integer.parseInt(args[3]) : 5;
    int ncust = args.length > 4 ? Integer.parseInt(args[4]) : 20000;
    DistributedTransactionManager manager =
        TransactionFactory.create(args[0]).getTransactionManager();
    List<List<Long>> all = new ArrayList<>();
    List<Thread> workers = new ArrayList<>();
    long end = System.nanoTime() + seconds * 1_000_000_000L;
    for (int t = 0; t < threads; t++) {
      List<Long> times = new ArrayList<>();
      all.add(times);
      Random rng = new Random(t);
      Thread w =
          new Thread(
              () -> {
                try {
                  while (System.nanoTime() < end) {
                    int id = 1 + rng.nextInt(ncust);
                    long t0 = System.nanoTime();
                    if (op.equals("get")) {
                      manager.get(
                          Get.newBuilder()
                              .namespace("bench")
                              .table("customers")
                              .partitionKey(Key.ofInt("id", id))
                              .projections("name", "region", "tier")
                              .build());
                    } else {
                      manager.scan(
                          Scan.newBuilder()
                              .namespace("bench")
                              .table("orders")
                              .partitionKey(Key.ofInt("customer_id", id))
                              .start(Key.ofInt("order_id", 1 + rng.nextInt(10)))
                              .limit(5)
                              .projections("order_id", "amount")
                              .build());
                    }
                    times.add(System.nanoTime() - t0);
                  }
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });
      workers.add(w);
      w.start();
    }
    for (Thread w : workers) {
      w.join();
    }
    List<Long> times = new ArrayList<>();
    for (List<Long> l : all) {
      times.addAll(l);
    }
    Collections.sort(times);
    System.out.printf(
        "%s: %d threads, %d s: tps=%.0f p50=%.3f ms p95=%.3f ms p99=%.3f ms%n",
        op,
        threads,
        seconds,
        times.size() / (double) seconds,
        times.get(times.size() / 2) / 1e6,
        times.get((int) (times.size() * 0.95)) / 1e6,
        times.get((int) (times.size() * 0.99)) / 1e6);
    manager.close();
    System.exit(0);
  }
}
