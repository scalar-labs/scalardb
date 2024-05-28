package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import org.junit.jupiter.api.Test;

class RdbEnginePostgresqlTest {

  @Test
  void createTableInternalSqlsAfterCreateTable_GivenSameClusteringOrders_ShouldNotCreateIndex() {
    // Arrange
    RdbEngineStrategy rdbEngine = new RdbEnginePostgresql();
    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk")
            .addClusteringKey("ck1", Order.ASC)
            .addClusteringKey("ck2", Order.DESC)
            .addColumn("pk", DataType.INT)
            .addColumn("ck1", DataType.INT)
            .addColumn("ck2", DataType.INT)
            .build();

    // Act
    String[] sqls =
        rdbEngine.createTableInternalSqlsAfterCreateTable(
            false, "myschema", "mytable", metadata, false);

    // Assert
    assertThat(sqls).hasSize(0);
  }

  @Test
  void createTableInternalSqlsAfterCreateTable_GivenDifferentClusteringOrders_ShouldCreateIndex() {
    // Arrange
    RdbEngineStrategy rdbEngine = new RdbEnginePostgresql();
    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk")
            .addClusteringKey("ck1", Order.ASC)
            .addClusteringKey("ck2", Order.DESC)
            .addColumn("pk", DataType.INT)
            .addColumn("ck1", DataType.INT)
            .addColumn("ck2", DataType.INT)
            .build();

    // Act
    String[] sqls =
        rdbEngine.createTableInternalSqlsAfterCreateTable(
            true, "myschema", "mytable", metadata, false);

    // Assert
    assertThat(sqls).hasSize(1);
    assertThat(sqls[0]).startsWith("CREATE UNIQUE INDEX ");
  }
}
