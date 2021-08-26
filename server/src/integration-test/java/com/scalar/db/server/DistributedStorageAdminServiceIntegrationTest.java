package com.scalar.db.server;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.server.config.ServerConfig;
import com.scalar.db.storage.AdminIntegrationTestBase;
import com.scalar.db.storage.jdbc.test.TestEnv;
import com.scalar.db.storage.rpc.GrpcAdmin;
import com.scalar.db.storage.rpc.GrpcConfig;
import java.util.Optional;
import java.util.Properties;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class DistributedStorageAdminServiceIntegrationTest extends AdminIntegrationTestBase {

  private static final String CONTACT_POINT = "jdbc:mysql://localhost:3306/";
  private static final String USERNAME = "root";
  private static final String PASSWORD = "mysql";

  private static TestEnv testEnv;
  private static ScalarDbServer server;
  private static DistributedStorageAdmin admin;

  @Before
  public void setUp() {
    setUp(admin);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    testEnv = new TestEnv(CONTACT_POINT, USERNAME, PASSWORD, Optional.empty());
    testEnv.createTable(
        NAMESPACE,
        TABLE,
        TableMetadata.newBuilder()
            .addColumn(COL_NAME1, DataType.INT)
            .addColumn(COL_NAME2, DataType.TEXT)
            .addColumn(COL_NAME3, DataType.TEXT)
            .addColumn(COL_NAME4, DataType.INT)
            .addColumn(COL_NAME5, DataType.INT)
            .addColumn(COL_NAME6, DataType.TEXT)
            .addColumn(COL_NAME7, DataType.BIGINT)
            .addColumn(COL_NAME8, DataType.FLOAT)
            .addColumn(COL_NAME9, DataType.DOUBLE)
            .addColumn(COL_NAME10, DataType.BOOLEAN)
            .addColumn(COL_NAME11, DataType.BLOB)
            .addPartitionKey(COL_NAME2)
            .addPartitionKey(COL_NAME1)
            .addClusteringKey(COL_NAME4, Scan.Ordering.Order.ASC)
            .addClusteringKey(COL_NAME3, Scan.Ordering.Order.DESC)
            .addSecondaryIndex(COL_NAME5)
            .addSecondaryIndex(COL_NAME6)
            .build());

    Properties serverProperties = new Properties(testEnv.getJdbcConfig().getProperties());
    serverProperties.setProperty(ServerConfig.PROMETHEUS_EXPORTER_PORT, "-1");
    server = new ScalarDbServer(serverProperties);
    server.start();

    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, "localhost");
    properties.setProperty(DatabaseConfig.CONTACT_PORT, "60051");
    properties.setProperty(DatabaseConfig.STORAGE, "grpc");
    admin = new GrpcAdmin(new GrpcConfig(properties));
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    testEnv.deleteTables();
    testEnv.close();
    admin.close();
    server.shutdown();
    server.blockUntilShutdown();
  }
}
