package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.Update;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.service.TransactionFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Runs ScalarDB on top of a SAP ASE database that already existed, without changing the tables in
 * it. The {@code erp} database, the {@code sales} owner, and the {@code customers} and {@code
 * orders} tables in it are all created outside ScalarDB by plain SQL, as an existing application
 * would have left them. See docs/sap-ase-poc.md for how to set that up.
 *
 * <p>The table is imported with transaction metadata decoupling, so ScalarDB adds a transaction
 * metadata table and a view over the two rather than adding its metadata columns to the existing
 * table.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledIfSystemProperty(named = "scalardb.ase.erp", matches = "true")
public class AseExistingDatabaseIntegrationTest {

  private static final String URL = "jdbc:sybase:Tds:localhost:5557/erp";
  private static final String USERNAME = "sa";
  private static final String PASSWORD = "sybase";

  private static final String NAMESPACE = "sales";
  private static final String TABLE = "customers";
  /** The view the import creates. Reads and writes go through this. */
  private static final String IMPORTED_TABLE = "customers_scalardb";
  /** The transaction metadata table the import creates alongside the existing table. */
  private static final String METADATA_TABLE = "customers_tx_metadata";

  private static final Map<String, String> DECOUPLING_OPTIONS =
      ImmutableMap.of("transaction-metadata-decoupling", "true");

  private DistributedTransactionAdmin admin;
  private DistributedTransactionManager manager;

  private static Properties properties() {
    Properties properties = new Properties();
    properties.setProperty("scalar.db.storage", "jdbc");
    properties.setProperty("scalar.db.contact_points", URL);
    properties.setProperty("scalar.db.username", USERNAME);
    properties.setProperty("scalar.db.password", PASSWORD);
    properties.setProperty("scalar.db.transaction_manager", "consensus-commit");
    // Transaction metadata decoupling reads a view that spans the existing table and the
    // transaction metadata table, and ScalarDB refuses it unless the isolation level is at least
    // what the engine reports as enough to read the two consistently. On ASE that is REPEATABLE
    // READ, which holds shared locks until the transaction completes.
    properties.setProperty("scalar.db.jdbc.isolation_level", "REPEATABLE_READ");
    return properties;
  }

  @BeforeAll
  public void setUp() throws Exception {
    TransactionFactory factory = TransactionFactory.create(properties());
    admin = factory.getTransactionAdmin();
    manager = factory.getTransactionManager();
    admin.createCoordinatorTables(true);
  }

  @AfterAll
  public void tearDown() {
    if (manager != null) {
      manager.close();
    }
    if (admin != null) {
      admin.close();
    }
  }

  @Test
  public void existingTable_ShouldRunTransactionsWithoutBeingChanged() throws Exception {
    // Arrange: the shape and the contents of the existing table, before ScalarDB touches it
    List<String> columnsBefore = columnsOf(NAMESPACE, TABLE);
    List<String> rowsBefore = rowsOf();
    assertThat(columnsBefore)
        .containsExactly(
            "customer_id int not-null",
            "name varchar not-null",
            "email varchar null",
            "region varchar null",
            "credit_limit float null",
            "active tinyint not-null",
            "updated_at bigdatetime null");
    assertThat(rowsBefore).hasSize(5);

    // Act: import it with transaction metadata decoupling
    admin.importTable(NAMESPACE, TABLE, DECOUPLING_OPTIONS);

    // Assert: the existing table is untouched. Its columns and its rows are what they were, and in
    // particular it has none of the tx_ metadata columns a plain import would have added to it.
    assertThat(columnsOf(NAMESPACE, TABLE)).isEqualTo(columnsBefore);
    assertThat(rowsOf()).isEqualTo(rowsBefore);

    // What ScalarDB added instead: a metadata table and a view, both new objects
    assertThat(objectsOwnedBy(NAMESPACE))
        .contains(TABLE + " [table]", METADATA_TABLE + " [table]", IMPORTED_TABLE + " [view]");

    // Assert: a transaction reads the data that was already there
    DistributedTransaction transaction = manager.start();
    Optional<Result> result = transaction.get(get(1));
    assertThat(result).isPresent();
    assertThat(result.get().getText("name")).isEqualTo("Acme Corp");
    assertThat(result.get().getDouble("credit_limit")).isEqualTo(50000.0);
    transaction.commit();

    // Assert: a committed transaction updates it, and the existing table is still the same shape
    transaction = manager.start();
    transaction.update(
        Update.newBuilder()
            .namespace(NAMESPACE)
            .table(IMPORTED_TABLE)
            .partitionKey(Key.ofInt("customer_id", 1))
            .doubleValue("credit_limit", 61000.0)
            .build());
    transaction.commit();

    transaction = manager.start();
    assertThat(transaction.get(get(1)).get().getDouble("credit_limit")).isEqualTo(61000.0);
    transaction.commit();

    assertThat(columnsOf(NAMESPACE, TABLE)).isEqualTo(columnsBefore);

    // Assert: the existing application, reading the table directly, sees the committed value
    assertThat(queryScalar("SELECT credit_limit FROM sales.customers WHERE customer_id = 1"))
        .isEqualTo("61000.0");

    // Assert: an aborted transaction leaves the existing table as it was
    transaction = manager.start();
    transaction.update(
        Update.newBuilder()
            .namespace(NAMESPACE)
            .table(IMPORTED_TABLE)
            .partitionKey(Key.ofInt("customer_id", 1))
            .doubleValue("credit_limit", 999.0)
            .build());
    transaction.abort();

    assertThat(queryScalar("SELECT credit_limit FROM sales.customers WHERE customer_id = 1"))
        .isEqualTo("61000.0");

    // Assert: a row the existing application inserts directly is visible to ScalarDB. The view is a
    // LEFT OUTER JOIN, so a row with no transaction metadata still reads.
    // Owner qualified rather than setuser: ASE resolves object names when it compiles the batch, so
    // a setuser in the same batch has not taken effect by the time the insert is resolved
    execute(
        "insert into sales.customers values"
            + " (6,'Soylent','ap@soylent.example','EMEA',15000.0,1,null)");
    transaction = manager.start();
    Optional<Result> inserted = transaction.get(get(6));
    assertThat(inserted).isPresent();
    assertThat(inserted.get().getText("name")).isEqualTo("Soylent");
    transaction.commit();
  }

  @Test
  public void existingTable_WithADecimalColumn_ShouldImportOnlyWithAnExplicitOverride()
      throws Exception {
    // orders.amount is numeric(12,2) and orders.ledger_ref is numeric(18,0). ScalarDB has no
    // decimal type, so a scaled column is refused rather than quietly losing precision.
    assertThatThrownBy(() -> admin.importTable(NAMESPACE, "orders", DECOUPLING_OPTIONS))
        .hasMessageContaining("DB-CORE-10307")
        .hasMessageContaining("amount");

    // Asking for DOUBLE accepts the loss deliberately. ledger_ref has no scale, so it maps to
    // BIGINT exactly without being asked.
    admin.importTable(
        NAMESPACE, "orders", DECOUPLING_OPTIONS, ImmutableMap.of("amount", DataType.DOUBLE));

    TableMetadata metadata = admin.getTableMetadata(NAMESPACE, "orders_scalardb");
    assertThat(metadata).isNotNull();
    assertThat(metadata.getColumnDataType("amount")).isEqualTo(DataType.DOUBLE);
    assertThat(metadata.getColumnDataType("ledger_ref")).isEqualTo(DataType.BIGINT);

    // The existing table is unchanged, and both columns read back through ScalarDB
    assertThat(columnsOf(NAMESPACE, "orders"))
        .containsExactly(
            "order_id int not-null",
            "customer_id int not-null",
            "amount numeric not-null",
            "ledger_ref numeric null",
            "status varchar not-null");

    DistributedTransaction transaction = manager.start();
    Optional<Result> order =
        transaction.get(
            Get.newBuilder()
                .namespace(NAMESPACE)
                .table("orders_scalardb")
                .partitionKey(Key.ofInt("order_id", 100))
                .build());
    assertThat(order).isPresent();
    assertThat(order.get().getDouble("amount")).isEqualTo(1250.75);
    // 18 digits, which BIGINT holds exactly and a DOUBLE would not
    assertThat(order.get().getBigInt("ledger_ref")).isEqualTo(900000000000000001L);
    transaction.commit();

    // A committed write goes back into the numeric columns
    transaction = manager.start();
    transaction.update(
        Update.newBuilder()
            .namespace(NAMESPACE)
            .table("orders_scalardb")
            .partitionKey(Key.ofInt("order_id", 100))
            .doubleValue("amount", 1999.99)
            .bigIntValue("ledger_ref", 900000000000000009L)
            .build());
    transaction.commit();

    assertThat(
            queryScalar(
                "SELECT convert(varchar(20), amount) FROM sales.orders WHERE order_id = 100"))
        .isEqualTo("1999.99");
    assertThat(
            queryScalar(
                "SELECT convert(varchar(20), ledger_ref) FROM sales.orders WHERE order_id = 100"))
        .isEqualTo("900000000000000009");
  }

  private Get get(int customerId) {
    return Get.newBuilder()
        .namespace(NAMESPACE)
        .table(IMPORTED_TABLE)
        .partitionKey(Key.ofInt("customer_id", customerId))
        .build();
  }

  /** The columns of a table as ASE itself reports them, so an added column would show up here. */
  private List<String> columnsOf(String owner, String table) throws SQLException {
    return query(
        "SELECT c.name + ' ' + t.name + ' ' + CASE WHEN c.status & 8 = 8 THEN 'null' ELSE"
            + " 'not-null' END FROM syscolumns c, systypes t, sysobjects o, sysusers u WHERE c.id ="
            + " o.id AND o.uid = u.uid AND c.usertype = t.usertype AND u.name = '"
            + owner
            + "' AND o.name = '"
            + table
            + "' ORDER BY c.colid");
  }

  private List<String> objectsOwnedBy(String owner) throws SQLException {
    return query(
        "SELECT o.name + CASE o.type WHEN 'U' THEN ' [table]' ELSE ' [view]' END FROM sysobjects o,"
            + " sysusers u WHERE o.uid = u.uid AND o.type IN ('U','V') AND u.name = '"
            + owner
            + "' ORDER BY o.name");
  }

  private List<String> rowsOf() throws SQLException {
    return query(
        "SELECT convert(varchar(4),customer_id) + '|' + name + '|' + isnull(email,'<null>') + '|'"
            + " + isnull(region,'<null>') + '|' + convert(varchar(20),credit_limit) + '|' +"
            + " convert(varchar(2),active) FROM sales.customers ORDER BY customer_id");
  }

  private String queryScalar(String sql) throws SQLException {
    List<String> rows = query(sql);
    assertThat(rows).hasSize(1);
    return rows.get(0);
  }

  /** Runs SQL on a connection of its own, so it does not go through ScalarDB. */
  private List<String> query(String sql) throws SQLException {
    try (Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      List<String> rows = new ArrayList<>();
      while (resultSet.next()) {
        rows.add(resultSet.getString(1));
      }
      return rows;
    }
  }

  private void execute(String sql) throws SQLException {
    try (Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
        Statement statement = connection.createStatement()) {
      statement.execute("SET QUOTED_IDENTIFIER ON");
      statement.execute(sql);
    }
  }
}
