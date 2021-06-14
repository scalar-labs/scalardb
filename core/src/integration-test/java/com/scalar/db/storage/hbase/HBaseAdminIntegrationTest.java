package com.scalar.db.storage.hbase;

import static com.scalar.db.storage.hbase.HBaseIntegrationTest.createMetadataTable;
import static com.scalar.db.storage.hbase.HBaseIntegrationTest.dropMetadataTable;
import static com.scalar.db.storage.hbase.HBaseIntegrationTest.upsertMetadata;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.AdminIntegrationTestBase;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.Properties;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class HBaseAdminIntegrationTest extends AdminIntegrationTestBase {

  private static Optional<String> namespacePrefix;
  private static DistributedStorageAdmin admin;
  private static HBaseConnection hbaseConnection;

  @Before
  public void setUp() throws Exception {
    setUp(admin);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    String jdbcUrl =
        System.getProperty("scalardb.hbase.jdbc_url", "jdbc:phoenix:localhost:2181:/hbase");
    namespacePrefix = Optional.ofNullable(System.getProperty("scalardb.namespace_prefix"));

    hbaseConnection = new HBaseConnection(jdbcUrl);

    try (Connection connection = hbaseConnection.getConnection();
        Statement statement = connection.createStatement()) {
      createMetadataTable(statement);
      createMetadatas(statement);
    }

    Properties props = new Properties();
    props.setProperty(DatabaseConfig.STORAGE, "hbase");
    props.setProperty(DatabaseConfig.CONTACT_POINTS, jdbcUrl);
    namespacePrefix.ifPresent(n -> props.setProperty(DatabaseConfig.NAMESPACE_PREFIX, n));
    admin = new HBaseAdmin(new DatabaseConfig(props));
  }

  private static void createMetadatas(Statement statement) throws SQLException {
    int ordinalPosition = 1;
    upsertMetadata(
        statement,
        NAMESPACE,
        TABLE,
        COL_NAME2,
        DataType.TEXT,
        "PARTITION",
        null,
        false,
        ordinalPosition++);
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
        COL_NAME3,
        DataType.TEXT,
        "CLUSTERING",
        "DESC",
        false,
        ordinalPosition++);
    upsertMetadata(
        statement, NAMESPACE, TABLE, COL_NAME5, DataType.INT, null, null, true, ordinalPosition++);
    upsertMetadata(
        statement, NAMESPACE, TABLE, COL_NAME6, DataType.TEXT, null, null, true, ordinalPosition++);
    upsertMetadata(
        statement,
        NAMESPACE,
        TABLE,
        COL_NAME7,
        DataType.BIGINT,
        null,
        null,
        false,
        ordinalPosition++);
    upsertMetadata(
        statement,
        NAMESPACE,
        TABLE,
        COL_NAME8,
        DataType.FLOAT,
        null,
        null,
        false,
        ordinalPosition++);
    upsertMetadata(
        statement,
        NAMESPACE,
        TABLE,
        COL_NAME9,
        DataType.DOUBLE,
        null,
        null,
        false,
        ordinalPosition++);
    upsertMetadata(
        statement,
        NAMESPACE,
        TABLE,
        COL_NAME10,
        DataType.BOOLEAN,
        null,
        null,
        false,
        ordinalPosition++);
    upsertMetadata(
        statement, NAMESPACE, TABLE, COL_NAME11, DataType.BLOB, null, null, false, ordinalPosition);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    admin.close();
    try (Connection connection = hbaseConnection.getConnection();
        Statement statement = connection.createStatement()) {
      dropMetadataTable(statement);
    }
  }
}
