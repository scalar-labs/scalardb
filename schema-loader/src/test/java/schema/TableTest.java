package schema;

import com.google.gson.JsonObject;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;

public class TableTest {

  public static final String NAMESPACE = "ns";
  public static final String TABLE = "tb";

  public static final String PARTITION_KEY1 = "pKey1";
  public static final String PARTITION_KEY2 = "pKey2";
  public static final String CLUSTERING_KEY1 = "cKey1";
  public static final String CLUSTERING_KEY2 = "cKey2";
  public static final List<String> SECONDARY_INDEXES = Arrays.asList("si1", "si2");
  public static final HashMap<String, String> COLUMNS = new HashMap<String, String>() {{
    put("pKey1", "INT");
    put("pKey2", "TEXT");
    put("cKey1", "TEXT");
    put("cKey2", "INT");
    put("si1", "INT");
    put("si2", "TEXT");
    put("c1", "BLOB");
    put("pKey2", "BOOLEAN");
  }};

  public static final Map<String, String> TABLE_OPTIONS = new HashMap<String, String>() {{
    put("to1", "vto1");
    put("to2", "vto2");
  }};
  public static final Map<String, String> META_OPTIONS = new HashMap<String, String>() {{
    put("mo1", "vmo1");
    put("mo2", "vmo2");
  }};

  public JsonObject tableDefinition;

  @Before
  public void setUpTableDefinition() {
    tableDefinition = new JsonObject().add("partition-key", new JsonObject().);
  }

}
