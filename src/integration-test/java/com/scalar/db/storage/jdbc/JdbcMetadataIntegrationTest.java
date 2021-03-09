package com.scalar.db.storage.jdbc;

import com.scalar.db.api.Scan;
import com.scalar.db.storage.MetadataIntegrationTestBase;
import com.scalar.db.storage.common.metadata.DataType;
import com.scalar.db.storage.common.metadata.TableMetadata;
import com.scalar.db.storage.jdbc.metadata.JdbcTableMetadata;
import com.scalar.db.storage.jdbc.metadata.TableMetadataManager;
import com.scalar.db.storage.jdbc.test.TestEnv;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class JdbcMetadataIntegrationTest extends MetadataIntegrationTestBase {

  private static TestEnv testEnv;
  private static TableMetadataManager tableMetadataManager;

  @Override
  protected TableMetadata getTableMetadata() {
    try {
      Optional<String> namespacePrefix = testEnv.getJdbcDatabaseConfig().getNamespacePrefix();
      String fullTableName = namespacePrefix.orElse("") + NAMESPACE + "." + TABLE;
      return tableMetadataManager.getTableMetadata(fullTableName);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testSchemaAndTableName() {
    JdbcTableMetadata jdbcTableMetadata = (JdbcTableMetadata) tableMetadata;

    Optional<String> namespacePrefix = testEnv.getJdbcDatabaseConfig().getNamespacePrefix();
    String schema = namespacePrefix.orElse("") + NAMESPACE;
    assertThat(jdbcTableMetadata.getSchema()).isEqualTo(schema);
    assertThat(jdbcTableMetadata.getTable()).isEqualTo(TABLE);
    String fullTableName = schema + "." + TABLE;
    assertThat(jdbcTableMetadata.getFullTableName()).isEqualTo(fullTableName);
  }

  @Test
  public void testSecondaryIndexOrder() {
    JdbcTableMetadata jdbcTableMetadata = (JdbcTableMetadata) tableMetadata;
    assertThat(jdbcTableMetadata.getSecondaryIndexOrder(COL_NAME1)).isNull();
    assertThat(jdbcTableMetadata.getSecondaryIndexOrder(COL_NAME2)).isNull();
    assertThat(jdbcTableMetadata.getSecondaryIndexOrder(COL_NAME3)).isNull();
    assertThat(jdbcTableMetadata.getSecondaryIndexOrder(COL_NAME4)).isNull();
    assertThat(jdbcTableMetadata.getSecondaryIndexOrder(COL_NAME5))
        .isEqualTo(Scan.Ordering.Order.ASC);
    assertThat(jdbcTableMetadata.getSecondaryIndexOrder(COL_NAME6))
        .isEqualTo(Scan.Ordering.Order.DESC);
    assertThat(jdbcTableMetadata.getSecondaryIndexOrder(COL_NAME7)).isNull();
    assertThat(jdbcTableMetadata.getSecondaryIndexOrder(COL_NAME8)).isNull();
    assertThat(jdbcTableMetadata.getSecondaryIndexOrder(COL_NAME9)).isNull();
    assertThat(jdbcTableMetadata.getSecondaryIndexOrder(COL_NAME10)).isNull();
    assertThat(jdbcTableMetadata.getSecondaryIndexOrder(COL_NAME11)).isNull();
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    testEnv = new TestEnv();
    testEnv.register(
        NAMESPACE,
        TABLE,
        Arrays.asList(COL_NAME2, COL_NAME1),
        Arrays.asList(COL_NAME4, COL_NAME3),
        new HashMap<String, Scan.Ordering.Order>() {
          {
            put(COL_NAME4, Scan.Ordering.Order.ASC);
            put(COL_NAME3, Scan.Ordering.Order.DESC);
          }
        },
        new HashMap<String, DataType>() {
          {
            put(COL_NAME1, DataType.INT);
            put(COL_NAME2, DataType.TEXT);
            put(COL_NAME3, DataType.TEXT);
            put(COL_NAME4, DataType.INT);
            put(COL_NAME5, DataType.INT);
            put(COL_NAME6, DataType.TEXT);
            put(COL_NAME7, DataType.BIGINT);
            put(COL_NAME8, DataType.FLOAT);
            put(COL_NAME9, DataType.DOUBLE);
            put(COL_NAME10, DataType.BOOLEAN);
            put(COL_NAME11, DataType.BLOB);
          }
        },
        Arrays.asList(COL_NAME5, COL_NAME6),
        new HashMap<String, Scan.Ordering.Order>() {
          {
            put(COL_NAME5, Scan.Ordering.Order.ASC);
            put(COL_NAME6, Scan.Ordering.Order.DESC);
          }
        });
    testEnv.createMetadataTable();
    testEnv.insertMetadata();

    Optional<String> namespacePrefix = testEnv.getJdbcDatabaseConfig().getNamespacePrefix();
    tableMetadataManager =
        new TableMetadataManager(testEnv.getDataSource(), namespacePrefix, testEnv.getRdbEngine());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    testEnv.dropMetadataTable();
    testEnv.close();
  }
}
