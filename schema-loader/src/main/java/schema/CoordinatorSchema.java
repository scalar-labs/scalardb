package schema;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TableMetadata.Builder;
import com.scalar.db.io.DataType;
import com.scalar.db.transaction.consensuscommit.Attribute;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import java.util.HashMap;
import java.util.Map;

public class CoordinatorSchema {

  private static final String PARTITION_KEY = "tx_id";
  private static final Map<String, DataType> columns =
      new HashMap<String, DataType>() {
        {
          put(Attribute.ID, DataType.TEXT);
          put(Attribute.STATE, DataType.INT);
          put(Attribute.CREATED_AT, DataType.BIGINT);
        }
      };
  private TableMetadata tableMetadata;

  public CoordinatorSchema() {
    Builder tableBuilder = TableMetadata.newBuilder();
    tableBuilder.addPartitionKey(PARTITION_KEY);
    for (Map.Entry<String, DataType> col : columns.entrySet()) {
      tableBuilder.addColumn(col.getKey(), col.getValue());
    }
    tableMetadata = tableBuilder.build();
  }

  public String getNamespace() {
    return Coordinator.NAMESPACE;
  }

  public String getTable() {
    return Coordinator.TABLE;
  }

  public TableMetadata getTableMetadata() {
    return tableMetadata;
  }
}
