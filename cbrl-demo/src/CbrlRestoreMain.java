import com.scalar.db.transaction.consensuscommit.cbrl.CbrlRestore;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * Thin CLI wrapper around {@link CbrlRestore}, which ships with no {@code main} of its own — it
 * exposes only the public constructor {@code CbrlRestore(Properties, backupLabel)} plus {@code
 * restore()} (it is {@link AutoCloseable}). The demo needs an entrypoint, so this class supplies
 * one.
 *
 * <p>Preconditions (the caller's responsibility, per CbrlRestore's contract): the coordinator table
 * and every user table have already been physically restored from backup into the tables this
 * properties file points at. {@code restore()} then reads the redo off the restored coordinator,
 * recovers each in-flight copy record, replays the committed redo, and writes the result back via
 * the Storage API. It never runs a transaction and adds no coordinator bookkeeping.
 *
 * <p>Usage:
 *   java -cp "<core-and-deps>:cbrl-demo/out" CbrlRestoreMain <scalardb.properties> <backup-label>
 *
 * <p>Build/run recipe: see cbrl-demo/scripts/run-demo.sh step 8 and cbrl-demo/README.md. This is
 * compiled against the built ScalarDB core jar (and its runtime dependencies).
 */
public final class CbrlRestoreMain {

  private CbrlRestoreMain() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: CbrlRestoreMain <scalardb.properties> <backup-label>");
      System.exit(2);
    }
    String propertiesPath = args[0];
    String backupLabel = args[1];

    Properties properties = new Properties();
    try (InputStream in = new FileInputStream(propertiesPath)) {
      properties.load(in);
    }

    System.out.printf("CBRL restore: label=%s, properties=%s%n", backupLabel, propertiesPath);
    try (CbrlRestore restore = new CbrlRestore(properties, backupLabel)) {
      restore.restore();
    }
    System.out.println("CBRL restore finished.");
  }
}
