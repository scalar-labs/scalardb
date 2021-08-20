package utils;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TableMetadata.Builder;
import com.scalar.db.io.DataType;
import java.util.HashMap;
import java.util.Map;

public class CoordinatorSchema {
  private TableMetadata tableMetadata;

  private static final String NAMESPACE = "coordinator";
  private static final String TABLE = "state";
  private static final String PARTITION_KEY = "tx_id";

  private static final Map<String, DataType> columns =
      new HashMap<String, DataType>() {
        {
          put("tx_id", DataType.TEXT);
          put("tx_state", DataType.INT);
          put("tx_created_at", DataType.BIGINT);
        }
      };

  public CoordinatorSchema() {
    Builder tableBuilder = TableMetadata.newBuilder();
    tableBuilder.addPartitionKey(PARTITION_KEY);
    for (Map.Entry<String, DataType> col : columns.entrySet()) {
      tableBuilder.addColumn(col.getKey(), col.getValue());
    }
    tableMetadata = tableBuilder.build();
  }

  public String getNamespace() {
    return NAMESPACE;
  }

  public String getTable() {
    return TABLE;
  }

  public TableMetadata getTableMetadata() {
    return tableMetadata;
  }
}
