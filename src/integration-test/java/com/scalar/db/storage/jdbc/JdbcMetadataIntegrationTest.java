package com.scalar.db.storage.jdbc;

import com.scalar.db.api.Scan;
import com.scalar.db.storage.jdbc.metadata.DataType;
import com.scalar.db.storage.jdbc.metadata.JdbcTableMetadata;
import com.scalar.db.storage.jdbc.metadata.TableMetadataManager;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Optional;

import static com.scalar.db.storage.jdbc.test.TestEnv.MYSQL_INFO;
import static com.scalar.db.storage.jdbc.test.TestEnv.ORACLE_INFO;
import static com.scalar.db.storage.jdbc.test.TestEnv.POSTGRESQL_INFO;
import static com.scalar.db.storage.jdbc.test.TestEnv.SQL_SERVER_INFO;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Parameterized.class)
public class JdbcMetadataIntegrationTest {

  private static final String NAMESPACE = "integration_testing";
  private static final String TABLE = "test_table";
  private static final String COL_NAME1 = "c1";
  private static final String COL_NAME2 = "c2";
  private static final String COL_NAME3 = "c3";
  private static final String COL_NAME4 = "c4";
  private static final String COL_NAME5 = "c5";

  private TestEnv testEnv;

  @Parameterized.Parameter public JdbcConnectionInfo jdbcConnectionInfo;

  @Parameterized.Parameters(name = "RDB={0}")
  public static Collection<JdbcConnectionInfo> jdbcConnectionInfos() {
    return Arrays.asList(MYSQL_INFO, POSTGRESQL_INFO, ORACLE_INFO, SQL_SERVER_INFO);
  }

  @Before
  public void setUp() throws Exception {
    testEnv = new TestEnv(jdbcConnectionInfo, Optional.empty());
    testEnv.register(
        NAMESPACE,
        TABLE,
        new LinkedHashMap<String, DataType>() {
          {
            put(COL_NAME1, DataType.INT);
            put(COL_NAME2, DataType.TEXT);
            put(COL_NAME3, DataType.INT);
            put(COL_NAME4, DataType.INT);
            put(COL_NAME5, DataType.BOOLEAN);
          }
        },
        Collections.singletonList(COL_NAME1),
        Collections.singletonList(COL_NAME4),
        new HashMap<String, Scan.Ordering.Order>() {
          {
            put(COL_NAME4, Scan.Ordering.Order.ASC);
          }
        },
        new HashSet<String>() {
          {
            add(COL_NAME3);
          }
        },
        new HashMap<String, Scan.Ordering.Order>() {
          {
            put(COL_NAME3, Scan.Ordering.Order.ASC);
          }
        });
    testEnv.createTables();
  }

  @After
  public void tearDown() throws Exception {
    testEnv.dropTables();
    testEnv.close();
  }

  @Test
  public void testMetadata() throws Exception {
    String fullTableName = NAMESPACE + "." + TABLE;

    TableMetadataManager tableMetadataManager =
        new TableMetadataManager(testEnv.getDataSource(), Optional.empty(), testEnv.getRdbEngine());
    JdbcTableMetadata tableMetadata = tableMetadataManager.getTableMetadata(fullTableName);

    assertThat(tableMetadata).isNotNull();

    assertThat(tableMetadata.getFullTableName()).isEqualTo(fullTableName);

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
