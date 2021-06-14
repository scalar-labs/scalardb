package com.scalar.db.storage.hbase;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.IntegrationTestBase;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.Properties;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class HBaseIntegrationTest extends IntegrationTestBase {

  private static HBase hbase;
  private static HBaseConnection hbaseConnection;

  @Before
  public void setUp() throws Exception {
    hbase.with(NAMESPACE, TABLE);
    setUp(hbase);
  }

  @After
  public void tearDown() throws Exception {
    try (Connection connection = hbaseConnection.getConnection();
        Statement statement = connection.createStatement()) {
      deleteTableData(statement, NAMESPACE, TABLE);
    }
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    String jdbcUrl =
        System.getProperty("scalardb.hbase.jdbc_url", "jdbc:phoenix:localhost:2181:/hbase");
    Optional<String> namespacePrefix =
        Optional.ofNullable(System.getProperty("scalardb.namespace_prefix"));

    hbaseConnection = new HBaseConnection(jdbcUrl);

    try (PhoenixConnection connection = hbaseConnection.getConnection();
        Statement statement = connection.createStatement()) {
      createMetadataTable(statement);
      createTestTable(connection, statement);
    }

    Properties props = new Properties();
    props.setProperty(DatabaseConfig.STORAGE, "hbase");
    props.setProperty(DatabaseConfig.CONTACT_POINTS, jdbcUrl);
    namespacePrefix.ifPresent(n -> props.setProperty(DatabaseConfig.NAMESPACE_PREFIX, n));
    hbase = new HBase(new DatabaseConfig(props));
  }

  private static void createTestTable(PhoenixConnection connection, Statement statement)
      throws SQLException {
    statement.execute("CREATE SCHEMA \"" + NAMESPACE + "\"");
    statement.execute(
        "CREATE TABLE \""
            + NAMESPACE
            + "\".\""
            + TABLE
            + "\" (\"hash\" INTEGER NOT NULL, \"c1\" INTEGER NOT NULL, \"c2\" VARCHAR, "
            + "\"c3\" INTEGER, \"c4\" INTEGER NOT NULL, \"c5\" BOOLEAN, "
            + "CONSTRAINT pk PRIMARY KEY (\"hash\", \"c1\", \"c4\"))");

    int ordinalPosition = 1;
    upsertMetadata(
        statement,
        NAMESPACE,
        TABLE,
        COL_NAME1,
        DataType.INT,
        "PARTITION",
        null,
        false,
        ordinalPosition++);
    upsertMetadata(
        statement,
        NAMESPACE,
        TABLE,
        COL_NAME2,
        DataType.TEXT,
        null,
        null,
        false,
        ordinalPosition++);
    upsertMetadata(
        statement, NAMESPACE, TABLE, COL_NAME3, DataType.INT, null, null, true, ordinalPosition++);
    upsertMetadata(
        statement,
        NAMESPACE,
        TABLE,
        COL_NAME4,
        DataType.INT,
        "CLUSTERING",
        "ASC",
        false,
        ordinalPosition++);
    upsertMetadata(
        statement,
        NAMESPACE,
        TABLE,
        COL_NAME5,
        DataType.BOOLEAN,
        null,
        null,
        false,
        ordinalPosition);

    statement.execute("CREATE LOCAL INDEX idx ON \"" + NAMESPACE + "\".\"" + TABLE + "\" (\"c3\")");

    addEndpoint(connection, NAMESPACE, TABLE);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    hbase.close();
    try (Connection connection = hbaseConnection.getConnection();
        Statement statement = connection.createStatement()) {
      dropTable(statement, NAMESPACE, TABLE);
      dropSchema(statement, NAMESPACE);
      dropMetadataTable(statement);
    }
  }

  public static void createMetadataTable(Statement statement) throws SQLException {
    statement.execute("CREATE SCHEMA \"scalardb\"");
    statement.execute(
        "CREATE TABLE \"scalardb\".\"metadata\"("
            + "\"full_table_name\" VARCHAR,"
            + "\"column_name\" VARCHAR,"
            + "\"data_type\" VARCHAR,"
            + "\"key_type\" VARCHAR,"
            + "\"clustering_order\" VARCHAR,"
            + "\"indexed\" BOOLEAN,"
            + "\"ordinal_position\" INTEGER,"
            + "CONSTRAINT pk PRIMARY KEY (\"full_table_name\", \"column_name\"))");
  }

  public static void dropMetadataTable(Statement statement) throws SQLException {
    statement.execute("DROP TABLE \"scalardb\".\"metadata\"");
    statement.execute("DROP SCHEMA \"scalardb\"");
  }

  public static void dropTable(Statement statement, String namespace, String table)
      throws SQLException {
    statement.execute("DROP TABLE \"" + namespace + "\".\"" + table + "\"");
  }

  public static void dropSchema(Statement statement, String namespace) throws SQLException {
    statement.execute("DROP SCHEMA \"" + namespace + "\"");
  }

  public static void upsertMetadata(
      Statement statement,
      String schema,
      String table,
      String column,
      DataType dataType,
      String keyType,
      String keyOrder,
      boolean indexed,
      int ordinalPosition)
      throws SQLException {
    statement.execute(
        String.format(
            "UPSERT INTO \"scalardb\".\"metadata\" VALUES('%s','%s','%s',%s,%s,%s,%d)",
            schema + "." + table,
            column,
            dataType,
            keyType != null ? "'" + keyType + "'" : "NULL",
            keyOrder != null ? "'" + keyOrder + "'" : "NULL",
            indexed,
            ordinalPosition));
  }

  public static void addEndpoint(PhoenixConnection connection, String namespace, String table)
      throws SQLException {
    try (Admin admin = connection.getQueryServices().getAdmin()) {
      TableName tableName = TableName.valueOf(namespace, table);
      TableDescriptor descriptor = admin.getDescriptor(tableName);
      admin.modifyTable(
          TableDescriptorBuilder.newBuilder(descriptor)
              .setCoprocessor("org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint")
              .build());
    } catch (IOException e) {
      throw new SQLException(e);
    }
  }

  public static void deleteTableData(Statement statement, String namespace, String table)
      throws SQLException {
    statement.execute("DELETE FROM \"" + namespace + "\".\"" + table + "\"");
  }
}
