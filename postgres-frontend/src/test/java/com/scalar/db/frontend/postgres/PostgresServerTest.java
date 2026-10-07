package com.scalar.db.frontend.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.ResultImpl;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Drives the wire-protocol server with the PostgreSQL JDBC driver as a client. */
public class PostgresServerTest {
  private static final TableMetadata METADATA =
      TableMetadata.newBuilder()
          .addColumn("id", DataType.INT)
          .addColumn("name", DataType.TEXT)
          .addPartitionKey("id")
          .build();

  private DistributedTransactionManager manager;
  private DistributedTransaction transaction;
  private PostgresServer server;

  private static Result row(int id, String name) {
    Map<String, Column<?>> columns = new LinkedHashMap<>();
    columns.put("id", IntColumn.of("id", id));
    columns.put("name", TextColumn.of("name", name));
    return new ResultImpl(columns, METADATA);
  }

  @BeforeEach
  public void setUp() throws Exception {
    manager = mock(DistributedTransactionManager.class);
    transaction = mock(DistributedTransaction.class);
    when(manager.begin()).thenReturn(transaction);
    TestScanners.stub(transaction);
    TestScanners.stub(manager);
    when(manager.scan(any())).thenReturn(Arrays.asList(row(1, "a"), row(2, "b")));
    when(transaction.scan(any())).thenReturn(Arrays.asList(row(1, "a"), row(2, "b")));
    DistributedTransactionAdmin admin = mock(DistributedTransactionAdmin.class);
    when(admin.getTableMetadata("ns", "t")).thenReturn(METADATA);
    server = new PostgresServer(manager, admin, 0);
    server.start();
  }

  @AfterEach
  public void tearDown() throws Exception {
    server.close();
  }

  private Connection connect(String options) throws SQLException {
    Properties properties = new Properties();
    properties.setProperty("user", "test"); // the server does not authenticate
    return DriverManager.getConnection(
        "jdbc:postgresql://localhost:" + server.getPort() + "/ns" + options, properties);
  }

  @Test
  public void extendedProtocol_PreparedStatementWithParameters_ShouldReturnRows() throws Exception {
    try (Connection connection = connect("");
        PreparedStatement statement =
            connection.prepareStatement(
                "SELECT id, UPPER(name) AS u FROM t WHERE id >= ? ORDER BY id DESC")) {
      statement.setInt(1, 1);
      try (ResultSet rs = statement.executeQuery()) {
        assertThat(rs.getMetaData().getColumnCount()).isEqualTo(2);
        assertThat(rs.getMetaData().getColumnName(1)).isEqualTo("id");
        assertThat(rs.getMetaData().getColumnName(2)).isEqualTo("u");
        assertThat(rs.next()).isTrue();
        assertThat(rs.getInt(1)).isEqualTo(2);
        assertThat(rs.getString(2)).isEqualTo("B");
        assertThat(rs.next()).isTrue();
        assertThat(rs.getInt("id")).isEqualTo(1);
        assertThat(rs.getString("u")).isEqualTo("A");
        assertThat(rs.next()).isFalse();
      }
    }
  }

  @Test
  public void simpleProtocol_SelectAndMultiRowInsert_ShouldWork() throws Exception {
    try (Connection connection = connect("?preferQueryMode=simple");
        Statement statement = connection.createStatement()) {
      try (ResultSet rs = statement.executeQuery("SELECT * FROM t WHERE name = 'a' OR id = 2")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getInt("id")).isEqualTo(1);
        assertThat(rs.getString("name")).isEqualTo("a");
        assertThat(rs.next()).isTrue();
        assertThat(rs.getInt("id")).isEqualTo(2);
        assertThat(rs.next()).isFalse();
      }
      assertThat(statement.executeUpdate("INSERT INTO t (id, name) VALUES (3, 'c'), (4, 'd')"))
          .isEqualTo(2);
      verify(manager)
          .mutate(
              Arrays.asList(
                  Insert.newBuilder()
                      .namespace("ns")
                      .table("t")
                      .partitionKey(Key.ofInt("id", 3))
                      .textValue("name", "c")
                      .build(),
                  Insert.newBuilder()
                      .namespace("ns")
                      .table("t")
                      .partitionKey(Key.ofInt("id", 4))
                      .textValue("name", "d")
                      .build()));
    }
  }

  @Test
  public void transaction_CommitThroughJdbc_ShouldUseOneScalarDbTransaction() throws Exception {
    try (Connection connection = connect("")) {
      connection.setAutoCommit(false);
      try (Statement statement = connection.createStatement()) {
        assertThat(statement.executeUpdate("DELETE FROM t WHERE id = 1")).isEqualTo(1);
        try (ResultSet rs = statement.executeQuery("SELECT COUNT(*) AS n FROM t")) {
          assertThat(rs.next()).isTrue();
          assertThat(rs.getLong("n")).isEqualTo(2);
        }
      }
      connection.commit();
    }
    verify(transaction)
        .mutate(
            Collections.singletonList(
                Delete.newBuilder()
                    .namespace("ns")
                    .table("t")
                    .partitionKey(Key.ofInt("id", 1))
                    .condition(ConditionBuilder.deleteIfExists())
                    .build()));
    verify(transaction).commit();
  }

  @Test
  public void error_ShouldBeReportedAndConnectionStayUsable() throws Exception {
    try (Connection connection = connect("");
        Statement statement = connection.createStatement()) {
      assertThatThrownBy(() -> statement.executeQuery("SELECT nope FROM t"))
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("Unknown column");
      try (ResultSet rs = statement.executeQuery("SELECT 1 AS one")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getInt("one")).isEqualTo(1);
      }
    }
  }

  @Test
  public void error_ShouldBeReportedAsInternalErrorAndConnectionStayUsable() throws Exception {
    when(manager.begin()).thenThrow(new AssertionError("boom"));
    try (Connection connection = connect("");
        Statement statement = connection.createStatement()) {
      assertThatThrownBy(() -> statement.execute("BEGIN"))
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("boom")
          .extracting(e -> ((SQLException) e).getSQLState())
          .isEqualTo("XX000");
      try (ResultSet rs = statement.executeQuery("SELECT 1 AS one")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getInt("one")).isEqualTo(1);
      }
    }
  }

  @Test
  public void failedStatementInTransaction_ShouldAbortItUntilRollback() throws Exception {
    try (Connection connection = connect("");
        Statement statement = connection.createStatement()) {
      connection.setAutoCommit(false);
      assertThatThrownBy(() -> statement.execute("SELECT nope FROM t"))
          .isInstanceOf(SQLException.class);
      assertThatThrownBy(() -> statement.execute("SELECT id FROM t"))
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("Current transaction is aborted")
          .extracting(e -> ((SQLException) e).getSQLState())
          .isEqualTo("25P02");
      assertThat(connection.unwrap(org.postgresql.jdbc.PgConnection.class).getTransactionState())
          .isEqualTo(org.postgresql.core.TransactionState.FAILED);
      connection.rollback();
      assertThat(connection.unwrap(org.postgresql.jdbc.PgConnection.class).getTransactionState())
          .isEqualTo(org.postgresql.core.TransactionState.IDLE);
      try (ResultSet rs = statement.executeQuery("SELECT id FROM t")) {
        assertThat(rs.next()).isTrue();
      }
      connection.commit();
    }
  }

  @Test
  public void insertReturning_ShouldComeBackAsAResultSet() throws Exception {
    try (Connection connection = connect("");
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery("INSERT INTO t (id, name) VALUES (3, 'c') RETURNING id, name")) {
      assertThat(rs.next()).isTrue();
      assertThat(rs.getInt("id")).isEqualTo(3);
      assertThat(rs.getString("name")).isEqualTo("c");
      assertThat(rs.next()).isFalse();
    }
  }

  @Test
  public void typed_ShouldGiveParametersTheValueOfTheirLiteral() {
    assertThat(PostgresServer.typed("5", 0)).isEqualTo(5L);
    // a decimal or exponent literal is numeric, as in PostgreSQL; 1.5e3 has no fraction digits
    assertThat(PostgresServer.typed("-1.5e3", 0)).isEqualTo(new java.math.BigDecimal("-1500"));
    assertThat(PostgresServer.typed("true", 0)).isEqualTo(true);
    assertThat(PostgresServer.typed("x", 0)).isEqualTo("x");
    assertThat(PostgresServer.typed("7", 1043)).isEqualTo("7"); // declared varchar
    assertThat(PostgresServer.typed("t", 16)).isEqualTo(true);
    assertThat(PostgresServer.typed(null, 23)).isNull();
  }

  @Test
  public void extendedProtocol_ServerPreparedStatement_ShouldReturnBinaryResults()
      throws Exception {
    // pgjdbc asks for binary results once a statement is server-prepared and its types are known
    try (Connection connection = connect("?prepareThreshold=1");
        PreparedStatement statement =
            connection.prepareStatement("SELECT id, name, id * 1.5 AS d FROM t WHERE id >= ?")) {
      for (int round = 0; round < 3; round++) {
        statement.setInt(1, 2);
        // the stubbed scan returns both rows whatever the condition
        try (ResultSet rs = statement.executeQuery()) {
          assertThat(rs.next()).isTrue();
          assertThat(rs.getInt("id")).isEqualTo(1);
          assertThat(rs.getString("name")).isEqualTo("a");
          assertThat(rs.getDouble("d")).isEqualTo(1.5);
          assertThat(rs.next()).isTrue();
          assertThat(rs.getInt("id")).isEqualTo(2);
          assertThat(rs.getString("name")).isEqualTo("b");
          assertThat(rs.getDouble("d")).isEqualTo(3.0);
          assertThat(rs.next()).isFalse();
        }
      }
    }
  }

  @Test
  public void binary_ShouldDecodeNumericParameters() {
    // 1234.5678: two base-10000 groups, weight 0, positive, scale 4
    assertThat(PostgresServer.binary(1700, numeric(2, 0, 0, 4, 1234, 5678))).isEqualTo("1234.5678");
    // -0.05: one group (0500) at weight -1, negative, scale 2
    assertThat(PostgresServer.binary(1700, numeric(1, -1, 0x4000, 2, 500))).isEqualTo("-0.05");
    // 12000000: one group (1200) at weight 1, scale 0
    assertThat(PostgresServer.binary(1700, numeric(1, 1, 0, 0, 1200))).isEqualTo("12000000");
    assertThat(PostgresServer.binary(1700, numeric(0, 0, 0, 2))).isEqualTo("0.00");
    assertThat(PostgresServer.binary(1700, numeric(0, 0, 0xC000, 0))).isEqualTo("NaN");
  }

  private static byte[] numeric(int groups, int weight, int sign, int scale, int... digits) {
    java.nio.ByteBuffer b = java.nio.ByteBuffer.allocate(8 + 2 * digits.length);
    b.putShort((short) groups).putShort((short) weight).putShort((short) sign);
    b.putShort((short) scale);
    for (int d : digits) {
      b.putShort((short) d);
    }
    return b.array();
  }

  @Test
  public void binary_ShouldRoundTripUuidAndJsonb() {
    String uuid = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
    byte[] raw = PostgresServer.binary(2950, uuid);
    assertThat(raw).hasSize(16);
    assertThat(PostgresServer.binary(2950, raw)).isEqualTo(uuid);
    byte[] jsonb = PostgresServer.binary(3802, Json.cast("{\"b\":1,\"a\":2}", true));
    assertThat(jsonb[0]).isEqualTo((byte) 1);
    assertThat(PostgresServer.binary(3802, jsonb)).isEqualTo("{\"a\": 2, \"b\": 1}");
  }

  @Test
  public void binary_ShouldDecodeArrayParametersToArrayText() {
    // int4[] {1,NULL,3}: 1 dimension, has nulls, element oid 23, 3 elements from index 1
    java.nio.ByteBuffer b = java.nio.ByteBuffer.allocate(20 + 8 + 4 + 8);
    b.putInt(1).putInt(1).putInt(23).putInt(3).putInt(1);
    b.putInt(4).putInt(1).putInt(-1).putInt(4).putInt(3);
    assertThat(PostgresServer.binary(1007, b.array())).isEqualTo("{1,NULL,3}");
    // text[] {"a b",c}
    byte[] ab = "a b".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    java.nio.ByteBuffer t = java.nio.ByteBuffer.allocate(20 + 4 + ab.length + 4 + 1);
    t.putInt(1).putInt(0).putInt(25).putInt(2).putInt(1);
    t.putInt(ab.length).put(ab).putInt(1).put((byte) 'c');
    assertThat(PostgresServer.binary(1009, t.array())).isEqualTo("{\"a b\",c}");
    assertThat(PostgresServer.typed("{1,2}", 1007)).isEqualTo("{1,2}");
  }

  @Test
  public void binary_ShouldEncodeNumericLikePostgres() {
    assertThat(PostgresServer.binary(1700, new java.math.BigDecimal("1234.5678")))
        .isEqualTo(numeric(2, 0, 0, 4, 1234, 5678));
    assertThat(PostgresServer.binary(1700, new java.math.BigDecimal("-0.05")))
        .isEqualTo(numeric(1, -1, 0x4000, 2, 500));
    assertThat(PostgresServer.binary(1700, new java.math.BigDecimal("12000000")))
        .isEqualTo(numeric(1, 1, 0, 0, 1200));
    assertThat(PostgresServer.binary(1700, new java.math.BigDecimal("2500.0")))
        .isEqualTo(numeric(1, 0, 0, 1, 2500));
    assertThat(PostgresServer.binary(1700, new java.math.BigDecimal("0.00")))
        .isEqualTo(numeric(0, 0, 0, 2));
    // decoding what was encoded gives the text back
    for (String s : new String[] {"3.5000000000000000", "-180414.25", "0.0001", "1000"}) {
      assertThat(
              PostgresServer.binary(1700, PostgresServer.binary(1700, new java.math.BigDecimal(s))))
          .isEqualTo(s);
    }
    assertThat(PostgresServer.typed("1.5", 0)).isEqualTo(new java.math.BigDecimal("1.5"));
    assertThat(PostgresServer.typed("1.5", 701)).isEqualTo(1.5);
    assertThat(PostgresServer.typed("NaN", 1700)).isEqualTo(Double.NaN);
  }

  @Test
  public void binary_ShouldEncodeLikePostgres() {
    assertThat(PostgresServer.binary(23, 7)).containsExactly(0, 0, 0, 7);
    assertThat(PostgresServer.binary(20, 1L)).containsExactly(0, 0, 0, 0, 0, 0, 0, 1);
    assertThat(PostgresServer.binary(16, true)).containsExactly(1);
    assertThat(PostgresServer.binary(701, 1.5)).containsExactly(0x3f, 0xf8, 0, 0, 0, 0, 0, 0);
    assertThat(PostgresServer.binary(1082, java.time.LocalDate.of(2000, 1, 3)))
        .containsExactly(0, 0, 0, 2);
    assertThat(PostgresServer.binary(1114, java.time.LocalDateTime.of(2000, 1, 1, 0, 0, 1)))
        .containsExactly(0, 0, 0, 0, 0, 0x0f, 0x42, 0x40);
    assertThat(PostgresServer.binary(25, "ab")).containsExactly('a', 'b');
    // time: microseconds since midnight; timestamptz: microseconds since 2000-01-01 UTC; bytea: raw
    assertThat(PostgresServer.binary(1083, java.time.LocalTime.of(0, 0, 1)))
        .containsExactly(0, 0, 0, 0, 0, 0x0f, 0x42, 0x40);
    assertThat(PostgresServer.binary(1184, java.time.Instant.parse("2000-01-01T00:00:01Z")))
        .containsExactly(0, 0, 0, 0, 0, 0x0f, 0x42, 0x40);
    assertThat(PostgresServer.binary(17, java.nio.ByteBuffer.wrap(new byte[] {(byte) 0xde, 1})))
        .containsExactly(0xde, 1);
    assertThat(PostgresServer.binary(17, (Object) new byte[] {7})).containsExactly(7);
  }

  @Test
  public void text_Floats_ShouldUsePostgresFormat() {
    assertThat(PostgresServer.text(2500.0)).isEqualTo("2500");
    assertThat(PostgresServer.text(0.1)).isEqualTo("0.1");
    assertThat(PostgresServer.text(-0.0)).isEqualTo("-0");
    assertThat(PostgresServer.text(1e14)).isEqualTo("100000000000000");
    assertThat(PostgresServer.text(1e15)).isEqualTo("1e+15");
    assertThat(PostgresServer.text(1.5e-5)).isEqualTo("1.5e-05");
    assertThat(PostgresServer.text(0.0001)).isEqualTo("0.0001");
    assertThat(PostgresServer.text(Double.NaN)).isEqualTo("NaN");
    assertThat(PostgresServer.text(Double.NEGATIVE_INFINITY)).isEqualTo("-Infinity");
    assertThat(PostgresServer.text(100000f)).isEqualTo("100000");
    assertThat(PostgresServer.text(1e6f)).isEqualTo("1e+06");
    assertThat(PostgresServer.text(0.1f)).isEqualTo("0.1");
  }

  @Test
  public void errors_ShouldCarryPostgresSqlStates() throws Exception {
    doThrow(
            new CrudConflictException(
                "DB-CORE-20013: The record being prepared already exists", null, "tx"))
        .when(manager)
        .mutate(any());
    try (Connection connection = connect("");
        Statement statement = connection.createStatement()) {
      assertSqlState(statement, "SELECT 1 / 0", "22012");
      assertSqlState(statement, "SELECT 2147483647 + 1", "22003");
      assertSqlState(statement, "SELECT nosuch FROM t", "42703");
      assertSqlState(statement, "SELECT * FROM nosuch", "42P01");
      assertSqlState(statement, "SELECT CAST('x' AS INT)", "22P02");
      assertSqlState(statement, "SELECT (SELECT id FROM t), 1", "21000");
      assertSqlState(statement, "INSERT INTO t VALUES (1, 'a')", "23505");
      // The same ScalarDB error on an upsert is a write-write conflict the client should retry
      assertSqlState(
          statement,
          "INSERT INTO t VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
          "40001");
    }
  }

  @Test
  public void select_NaN_ShouldCompareAsPostgres() throws Exception {
    try (Connection connection = connect("");
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                "SELECT CAST('NaN' AS DOUBLE PRECISION) > CAST('Infinity' AS DOUBLE PRECISION),"
                    + " CAST('NaN' AS DOUBLE PRECISION) = CAST('NaN' AS DOUBLE PRECISION)")) {
      assertThat(rs.next()).isTrue();
      assertThat(rs.getBoolean(1)).isTrue();
      assertThat(rs.getBoolean(2)).isTrue();
    }
  }

  private static void assertSqlState(Statement statement, String sql, String sqlState) {
    assertThatThrownBy(() -> statement.execute(sql))
        .isInstanceOfSatisfying(
            SQLException.class, e -> assertThat(e.getSQLState()).as(sql).isEqualTo(sqlState));
  }

  @Test
  public void rowDescription_ShouldUsePostgresColumnNames() throws Exception {
    try (Connection connection = connect("");
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                "SELECT a.id + 1, UPPER(a.name), a.*, b.id FROM t a JOIN t b ON a.id = b.id")) {
      java.sql.ResultSetMetaData md = rs.getMetaData();
      assertThat(md.getColumnCount()).isEqualTo(5);
      assertThat(md.getColumnName(1)).isEqualTo("?column?");
      assertThat(md.getColumnName(2)).isEqualTo("upper");
      assertThat(md.getColumnName(3)).isEqualTo("id");
      assertThat(md.getColumnName(4)).isEqualTo("name");
      assertThat(md.getColumnName(5)).isEqualTo("id");
    }
    try (Connection connection = connect("");
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT s.count FROM (SELECT COUNT(*) FROM t) s")) {
      assertThat(rs.getMetaData().getColumnName(1)).isEqualTo("count");
      assertThat(rs.next()).isTrue();
      assertThat(rs.getLong("count")).isEqualTo(2);
    }
  }
}
