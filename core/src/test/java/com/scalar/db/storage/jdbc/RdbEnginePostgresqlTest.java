package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
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

  @Test
  void getCollationClause_Default_ShouldReturnCollateDefault() {
    // Arrange
    RdbEngineStrategy rdbEngine = new RdbEnginePostgresql();

    // Act
    String clause = rdbEngine.getCollationClause(new TextColumnCollation("default", null));

    // Assert
    assertThat(clause).isEqualTo("COLLATE \"default\"");
  }

  @Test
  void getCollationClause_SchemaQualifiedCollation_ShouldReturnItAsIs() {
    // Arrange
    RdbEngineStrategy rdbEngine = new RdbEnginePostgresql();

    // Act
    String clause =
        rdbEngine.getCollationClause(new TextColumnCollation("\"pg_catalog\".\"en_US\"", null));

    // Assert
    assertThat(clause).isEqualTo("COLLATE \"pg_catalog\".\"en_US\"");
  }

  @Test
  void getTextColumnCollations_ShouldReturnCollationByColumnName() throws SQLException {
    // Arrange
    RdbEngineStrategy rdbEngine = new RdbEnginePostgresql();
    Connection connection = mock(Connection.class);
    PreparedStatement ps = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);
    when(connection.prepareStatement(anyString())).thenReturn(ps);
    when(ps.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(true, true, false);
    when(rs.getString(1)).thenReturn("c1", "c2");
    when(rs.getString(2)).thenReturn("default", "\"pg_catalog\".\"C\"");

    // Act
    Map<String, TextColumnCollation> collations =
        rdbEngine.getTextColumnCollations(connection, "ns", "tbl");

    // Assert
    assertThat(collations)
        .containsOnly(
            entry("c1", new TextColumnCollation("default", null)),
            entry("c2", new TextColumnCollation("\"pg_catalog\".\"C\"", null)));
    verify(ps).setString(1, "ns");
    verify(ps).setString(2, "tbl");
  }
}
