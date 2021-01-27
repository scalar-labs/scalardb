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
import static com.scalar.db.storage.jdbc.test.TestEnv.MYSQL_INFO;
import static com.scalar.db.storage.jdbc.test.TestEnv.ORACLE_INFO;
import static com.scalar.db.storage.jdbc.test.TestEnv.POSTGRESQL_INFO;
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
  @Parameterized.Parameter public JdbcConnectionInfo jdbcConnectionInfo;
  private TestEnv testEnv;

  private static String getFullNamespace(Optional<String> namespacePrefix) {
    return namespacePrefix.orElse("") + NAMESPACE;
  }

  private static String getFullTableName(Optional<String> namespacePrefix) {
    return getFullNamespace(namespacePrefix) + "." + TABLE;
  }

  @Parameterized.Parameters(name = "RDB={0}")
  public static Collection<JdbcConnectionInfo> jdbcConnectionInfos() {
    return Arrays.asList(MYSQL_INFO, POSTGRESQL_INFO, ORACLE_INFO, SQL_SERVER_INFO);
  }

  @Before
  public void setUp() throws Exception {
    testEnv =
        new TestEnv(
            jdbcConnectionInfo,
            new BaseStatements() {
              @Override
              public List<String> insertMetadataStatements(Optional<String> namespacePrefix) {
                return Arrays.asList(
                    insertMetadataStatement(
                        namespacePrefix,
                        getFullTableName(namespacePrefix),
                        COL_NAME1,
                        DataType.INT,
                        KeyType.PARTITION,
                        null,
                        false,
                        null,
                        1),
                    insertMetadataStatement(
                        namespacePrefix,
                        getFullTableName(namespacePrefix),
                        COL_NAME2,
                        DataType.TEXT,
                        null,
                        null,
                        false,
                        null,
                        2),
                    insertMetadataStatement(
                        namespacePrefix,
                        getFullTableName(namespacePrefix),
                        COL_NAME3,
                        DataType.INT,
                        null,
                        null,
                        true,
                        Scan.Ordering.Order.ASC,
                        3),
                    insertMetadataStatement(
                        namespacePrefix,
                        getFullTableName(namespacePrefix),
                        COL_NAME4,
                        DataType.INT,
                        KeyType.CLUSTERING,
                        Scan.Ordering.Order.ASC,
                        false,
                        null,
                        4),
                    insertMetadataStatement(
                        namespacePrefix,
                        getFullTableName(namespacePrefix),
                        COL_NAME5,
                        DataType.BOOLEAN,
                        null,
                        null,
                        false,
                        null,
                        5));
              }

              @Override
              public List<String> schemas(Optional<String> namespacePrefix, RdbEngine rdbEngine) {
                return Collections.emptyList();
              }

              @Override
              public List<String> tables(Optional<String> namespacePrefix, RdbEngine rdbEngine) {
                return Collections.emptyList();
              }

              @Override
              public List<String> createTableStatements(
                  Optional<String> namespacePrefix, RdbEngine rdbEngine) {
                return Collections.emptyList();
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

    assertThat(tableMetadata.isIndexedColumn(COL_NAME1)).isFalse();
    assertThat(tableMetadata.isIndexedColumn(COL_NAME2)).isFalse();
    assertThat(tableMetadata.isIndexedColumn(COL_NAME3)).isTrue();
    assertThat(tableMetadata.isIndexedColumn(COL_NAME4)).isFalse();
    assertThat(tableMetadata.isIndexedColumn(COL_NAME5)).isFalse();

    assertThat(tableMetadata.getIndexOrder(COL_NAME1)).isNull();
    assertThat(tableMetadata.getIndexOrder(COL_NAME2)).isNull();
    assertThat(tableMetadata.getIndexOrder(COL_NAME3)).isEqualTo(Scan.Ordering.Order.ASC);
    assertThat(tableMetadata.getIndexOrder(COL_NAME4)).isNull();
    assertThat(tableMetadata.getIndexOrder(COL_NAME5)).isNull();
  }
}
