package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import java.sql.SQLException;
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

  /**
   * The AWS Advanced JDBC Wrapper reports failover with SQLState 08001, 08S02, and 08007. These
   * must never be treated as conflicts. {@link com.scalar.db.storage.jdbc.JdbcDatabase} converts a
   * conflict into a {@code RetriableExecutionException}, which tells the caller the operation
   * definitely did not apply and is safe to retry. That guarantee does not hold for a failover,
   * where the outcome may be unknown or the write may already have been applied. Adding a failover
   * SQLState here would silently break the storage layer's safety.
   */
  @Test
  void isConflict_GivenFailoverSqlStates_ShouldReturnFalse() {
    // Arrange
    RdbEngineStrategy rdbEngine = new RdbEnginePostgresql();

    // Act
    // Assert
    assertThat(rdbEngine.isConflict(new SQLException("failover failed", "08001"))).isFalse();
    assertThat(rdbEngine.isConflict(new SQLException("communication link changed", "08S02")))
        .isFalse();
    assertThat(rdbEngine.isConflict(new SQLException("transaction resolution unknown", "08007")))
        .isFalse();
  }

  @Test
  void isConflict_GivenSerializationFailureOrDeadlock_ShouldReturnTrue() {
    // Arrange
    RdbEngineStrategy rdbEngine = new RdbEnginePostgresql();

    // Act
    // Assert
    assertThat(rdbEngine.isConflict(new SQLException("serialization failure", "40001"))).isTrue();
    assertThat(rdbEngine.isConflict(new SQLException("deadlock detected", "40P01"))).isTrue();
  }

  @Test
  void isConflict_GivenNullSqlState_ShouldReturnFalse() {
    // Arrange
    RdbEngineStrategy rdbEngine = new RdbEnginePostgresql();

    // Act
    // Assert
    assertThat(rdbEngine.isConflict(new SQLException("no sql state"))).isFalse();
  }
}
