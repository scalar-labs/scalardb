import com.scalar.database.api.*;
import com.scalar.database.config.DatabaseConfig;
import com.scalar.database.exception.storage.ExecutionException;
import com.scalar.database.io.*;
import com.scalar.database.storage.cassandra.Cassandra;
import java.util.Optional;
import java.util.Properties;

public class TestStorage {
  private static final String NAMESPACE = "sample";
  private static final String TABLE = "children";
  private static final String PARENT_ID = "parent_id";
  private static final String CHILD_ID = "child_id";
  private static final String NAME = "name";
  private static final String HEIGHT = "height";

  public static void main(String[] args) throws ExecutionException {
    Properties properties = new Properties();
    properties.setProperty("scalar.database.contact_points", "localhost");
    properties.setProperty("scalar.database.username", "cassandra");
    properties.setProperty("scalar.database.password", "cassandra");

    DatabaseConfig config = new DatabaseConfig(properties);
    DistributedStorage storage = new Cassandra(config);

    // set default namespace and table name in this storage instance.
    storage.with(NAMESPACE, TABLE);

    // Inserts new entries
    Put put1 =
        new Put(new Key(new BigIntValue(PARENT_ID, 1)), new Key(new IntValue(CHILD_ID, 1)))
            .withValue(new TextValue(NAME, "ichiro"))
            .withValue(new DoubleValue(HEIGHT, 50.5));
    storage.put(put1);

    Put put2 =
        new Put(new Key(new BigIntValue(PARENT_ID, 1)), new Key(new IntValue(CHILD_ID, 2)))
            .withValue(new TextValue(NAME, "jiro"))
            .withValue(new DoubleValue(HEIGHT, 48.0));
    storage.put(put2);

    // Retrieves an entry with the specified primary key
    Get get = new Get(new Key(new BigIntValue(PARENT_ID, 1)), new Key(new IntValue(CHILD_ID, 2)));
    Optional<Result> result = storage.get(get);

    result.ifPresent(
        r -> {
          System.out.println(r);
          r.getValue(NAME).ifPresent(v -> System.out.println(((TextValue) v).getString().get()));
        });

    // Retrieves all the entries with the specified parent_id (partition key)
    Scan scan = new Scan(new Key(new BigIntValue(PARENT_ID, 1)));
    Scanner scanner = storage.scan(scan);
    scanner.forEach(
        r -> {
          System.out.println(r);
          r.getValue(NAME).ifPresent(v -> System.out.println(((TextValue) v).getString().get()));
        });

    storage.close();

    // System.exit(0);
  }
}
