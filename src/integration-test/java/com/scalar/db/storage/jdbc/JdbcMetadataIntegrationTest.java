package com.scalar.db.storage.jdbc;

import com.scalar.db.api.Scan;
import com.scalar.db.storage.jdbc.metadata.DataType;
import com.scalar.db.storage.jdbc.metadata.JdbcTableMetadata;
import com.scalar.db.storage.jdbc.metadata.KeyType;
import com.scalar.db.storage.jdbc.metadata.TableMetadataManager;
import com.scalar.db.storage.jdbc.test.BaseStatements;
import com.scalar.db.storage.jdbc.test.JdbcConnectionInfo;
import com.scalar.db.storage.jdbc.test.TestEnv;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.scalar.db.storage.jdbc.test.BaseStatements.insertMetadataStatement;
import static com.scalar.db.storage.jdbc.test.TestEnv.MY_SQL_INFO;
import static com.scalar.db.storage.jdbc.test.TestEnv.ORACLE_INFO;
import static com.scalar.db.storage.jdbc.test.TestEnv.POSTGRE_SQL_INFO;
import static com.scalar.db.storage.jdbc.test.TestEnv.SQL_SERVER_INFO;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Parameterized.class)
public class JdbcMetadataIntegrationTest {

  private static final Optional<String> NAMESPACE_PREFIX = Optional.empty();
  private static final String NAMESPACE = "integration_testing";
  private static final String TABLE = "test_table";
  private static final String COL_NAME1 = "c1";
  private static final String COL_NAME2 = "c2";
  private static final String COL_NAME3 = "c3";
  private static final String COL_NAME4 = "c4";
  private static final String COL_NAME5 = "c5";

  private static String getSchema(Optional<String> schemaPrefix) {
    return schemaPrefix.orElse("") + NAMESPACE;
  }

  private static String getFullTableName(Optional<String> schemaPrefix) {
    return getSchema(schemaPrefix) + "." + TABLE;
  }

  @Parameterized.Parameters(name = "RDB={0}")
  public static Collection<JdbcConnectionInfo> jdbcConnectionInfos() {
    return Arrays.asList(MY_SQL_INFO, POSTGRE_SQL_INFO, ORACLE_INFO, SQL_SERVER_INFO);
  }

  @Parameterized.Parameter public JdbcConnectionInfo jdbcConnectionInfo;

  private TestEnv testEnv;

  @Before
  public void setUp() throws Exception {
    testEnv =
        new TestEnv(
            jdbcConnectionInfo,
            new BaseStatements() {
              @Override
              public List<String> insertMetadataStatements(Optional<String> schemaPrefix) {
                return Arrays.asList(
                    insertMetadataStatement(
                        schemaPrefix,
                        getFullTableName(schemaPrefix),
                        COL_NAME1,
                        DataType.INT,
                        KeyType.PARTITION,
                        null,
                        false,
                        1),
                    insertMetadataStatement(
                        schemaPrefix,
                        getFullTableName(schemaPrefix),
                        COL_NAME2,
                        DataType.TEXT,
                        null,
                        null,
                        false,
                        2),
                    insertMetadataStatement(
                        schemaPrefix,
                        getFullTableName(schemaPrefix),
                        COL_NAME3,
                        DataType.INT,
                        null,
                        null,
                        true,
                        3),
                    insertMetadataStatement(
                        schemaPrefix,
                        getFullTableName(schemaPrefix),
                        COL_NAME4,
                        DataType.INT,
                        KeyType.CLUSTERING,
                        Scan.Ordering.Order.ASC,
                        false,
                        4),
                    insertMetadataStatement(
                        schemaPrefix,
                        getFullTableName(schemaPrefix),
                        COL_NAME5,
                        DataType.BOOLEAN,
                        null,
                        null,
                        false,
                        5));
              }

              @Override
              public List<String> schemas(Optional<String> schemaPrefix) {
                return Collections.singletonList(getSchema(schemaPrefix));
              }

              @Override
              public List<String> tables(Optional<String> schemaPrefix) {
                return Collections.singletonList(getFullTableName(schemaPrefix));
              }

              @Override
              public List<String> createTableStatements(Optional<String> schemaPrefix) {
                return Collections.singletonList(
                    "CREATE TABLE "
                        + getFullTableName(schemaPrefix)
                        + " ("
                        + COL_NAME1
                        + " INT,"
                        + COL_NAME2
                        + " VARCHAR(100),"
                        + COL_NAME3
                        + " INT,"
                        + COL_NAME4
                        + " INT,"
                        + COL_NAME5
                        + " BOOLEAN,"
                        + "PRIMARY KEY("
                        + COL_NAME1
                        + ","
                        + COL_NAME4
                        + "))");
              }
            },
            NAMESPACE_PREFIX);
    testEnv.createMetadataTableAndInsertMetadata();
  }

  @After
  public void tearDown() throws Exception {
    testEnv.dropAllTablesAndSchemas();
    testEnv.close();
  }

  @Test
  public void testMetadata() throws Exception {
    TableMetadataManager tableMetadataManager = new TableMetadataManager(testEnv.getDataSource());
    JdbcTableMetadata tableMetadata =
        tableMetadataManager.getTableMetadata(getFullTableName(NAMESPACE_PREFIX));

    assertThat(tableMetadata).isNotNull();

    assertThat(tableMetadata.getfullTableName()).isEqualTo(getFullTableName(NAMESPACE_PREFIX));

    assertThat(tableMetadata.getPartitionKeys().size()).isEqualTo(1);
    assertThat(tableMetadata.getPartitionKeys().get(0)).isEqualTo(COL_NAME1);

    assertThat(tableMetadata.getClusteringKeys().size()).isEqualTo(1);
    assertThat(tableMetadata.getClusteringKeys().get(0)).isEqualTo(COL_NAME4);

    assertThat(tableMetadata.getColumns().size()).isEqualTo(5);
    assertThat(tableMetadata.getColumns().get(0)).isEqualTo(COL_NAME1);
    assertThat(tableMetadata.getColumns().get(1)).isEqualTo(COL_NAME2);
    assertThat(tableMetadata.getColumns().get(2)).isEqualTo(COL_NAME3);
    assertThat(tableMetadata.getColumns().get(3)).isEqualTo(COL_NAME4);
    assertThat(tableMetadata.getColumns().get(4)).isEqualTo(COL_NAME5);

    assertThat(tableMetadata.getDataType(COL_NAME1)).isEqualTo(DataType.INT);
    assertThat(tableMetadata.getDataType(COL_NAME2)).isEqualTo(DataType.TEXT);
    assertThat(tableMetadata.getDataType(COL_NAME3)).isEqualTo(DataType.INT);
    assertThat(tableMetadata.getDataType(COL_NAME4)).isEqualTo(DataType.INT);
    assertThat(tableMetadata.getDataType(COL_NAME5)).isEqualTo(DataType.BOOLEAN);

    assertThat(tableMetadata.getClusteringKeyOrder(COL_NAME1)).isNull();
    assertThat(tableMetadata.getClusteringKeyOrder(COL_NAME2)).isNull();
    assertThat(tableMetadata.getClusteringKeyOrder(COL_NAME3)).isNull();
    assertThat(tableMetadata.getClusteringKeyOrder(COL_NAME4)).isEqualTo(Scan.Ordering.Order.ASC);
    assertThat(tableMetadata.getClusteringKeyOrder(COL_NAME5)).isNull();

    assertThat(tableMetadata.indexedColumn(COL_NAME1)).isFalse();
    assertThat(tableMetadata.indexedColumn(COL_NAME2)).isFalse();
    assertThat(tableMetadata.indexedColumn(COL_NAME3)).isTrue();
    assertThat(tableMetadata.indexedColumn(COL_NAME4)).isFalse();
    assertThat(tableMetadata.indexedColumn(COL_NAME5)).isFalse();
  }
}
