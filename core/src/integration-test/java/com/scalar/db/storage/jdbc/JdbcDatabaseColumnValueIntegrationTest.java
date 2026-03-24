package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.DistributedStorageColumnValueIntegrationTestBase;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import com.scalar.db.util.TestUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.provider.Arguments;

public class JdbcDatabaseColumnValueIntegrationTest
    extends DistributedStorageColumnValueIntegrationTestBase {

  private RdbEngineStrategy rdbEngine;

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = JdbcEnv.getProperties(testName);
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    rdbEngine = RdbEngineFactory.create(config);
    return properties;
  }

  @Override
  protected Column<?> getColumnWithRandomValue(
      Random random, String columnName, DataType dataType) {
    if (JdbcTestUtils.isOracle(rdbEngine)) {
      if (dataType == DataType.DOUBLE) {
        return JdbcTestUtils.getRandomOracleDoubleValue(random, columnName);
      }
      // don't allow empty value since Oracle treats empty value as NULL
      return TestUtils.getColumnWithRandomValue(random, columnName, dataType, false);
    }
    return super.getColumnWithRandomValue(random, columnName, dataType);
  }

  @Override
  protected Column<?> getColumnWithMinValue(String columnName, DataType dataType) {
    if (JdbcTestUtils.isOracle(rdbEngine)) {
      if (dataType == DataType.DOUBLE) {
        return JdbcTestUtils.getMinOracleDoubleValue(columnName);
      }
      // don't allow empty value since Oracle treats empty value as NULL
      return TestUtils.getColumnWithMinValue(columnName, dataType, false);
    }
    if (JdbcTestUtils.isDb2(rdbEngine)) {
      if (dataType == DataType.FLOAT) {
        return JdbcTestUtils.getMinDb2FloatValue(columnName);
      }
      if (dataType == DataType.DOUBLE) {
        return JdbcTestUtils.getMinDb2DoubleValue(columnName);
      }
    }
    return super.getColumnWithMinValue(columnName, dataType);
  }

  @Override
  protected Column<?> getColumnWithMaxValue(String columnName, DataType dataType) {
    if (JdbcTestUtils.isOracle(rdbEngine)) {
      if (dataType == DataType.DOUBLE) {
        return JdbcTestUtils.getMaxOracleDoubleValue(columnName);
      }
    }
    if (JdbcTestUtils.isSqlServer(rdbEngine)) {
      if (dataType == DataType.TEXT) {
        return JdbcTestUtils.getMaxSqlServerTextValue(columnName);
      }
    }
    return super.getColumnWithMaxValue(columnName, dataType);
  }

  @Override
  protected Stream<Arguments> provideLargeBlobSizes() {
    List<Arguments> args = new ArrayList<>();
    if (JdbcTestUtils.isOracle(rdbEngine)) {
      // As explained in
      // `com.scalar.db.storage.jdbc.RdbEngineOracle.bindBlobColumnToPreparedStatement()`,
      // handing a BLOB size bigger than 32,766 bytes requires a workaround so we particularly test
      // values around it.
      args.add(Arguments.of(32_766, "32.766 KB"));
      args.add(Arguments.of(32_767, "32.767 KB"));
      args.add(Arguments.of(100_000_000, "100 MB"));
    } else if (JdbcTestUtils.isTidb(rdbEngine)) {
      // TiDB default row max size is 6MB
      args.add(Arguments.of(6_000_000, "6 MB"));
    } else if (JdbcTestUtils.isMariaDB(rdbEngine)) {
      // MariaDB default maximum packet size is 16MB, but the Put operation is translated to an
      // INSERT ... ON DUPLICATE KEY UPDATE SQL statement, which contains the non-key columns twice.
      // So the maximum size of a BLOB value is around half the maximum packet size, about 8MB.
      args.add(Arguments.of(8_000_000, "8 MB"));
    } else if (JdbcTestUtils.isMysql(rdbEngine)) {
      // MySQL default maximum packet size is 4MB for Mysql 5.7 (64 MB for newer versions), but the
      // Put operation is translated to an INSERT ... ON DUPLICATE KEY UPDATE SQL statement, which
      // contains the non-key columns twice. So the maximum size of a BLOB value is around half
      // the maximum packet size, about 2MB.
      args.add(Arguments.of(2_000_000, "2 MB"));
    } else if (JdbcTestUtils.isSqlServer(rdbEngine)) {
      // A BLOB column is mapped to SQLServer `varbinary(8000)` which accepts a maximum size of
      // 8,000 bytes.
      args.add(Arguments.of(8_000, "8 KB"));
    } else {
      return super.provideLargeBlobSizes();
    }
    return args.stream();
  }

  @EnabledIf("com.scalar.db.storage.jdbc.JdbcEnv#isOracle")
  @Test
  public void put_largeBlobData_WithMultipleBlobColumnsShouldWorkCorrectly()
      throws ExecutionException {
    String tableName = TABLE + "_large_multiples_blob";
    try {
      // Arrange
      TableMetadata.Builder metadata =
          TableMetadata.newBuilder()
              .addColumn(COL_NAME1, DataType.INT)
              .addColumn(COL_NAME2, DataType.BLOB)
              .addColumn(COL_NAME3, DataType.BLOB)
              .addPartitionKey(COL_NAME1);

      admin.createTable(namespace, tableName, metadata.build(), true, getCreationOptions());
      byte[] blobDataCol2 = createLargeBlob(32_766);
      byte[] blobDataCol3 = createLargeBlob(5000);
      Put put =
          Put.newBuilder()
              .namespace(namespace)
              .table(tableName)
              .partitionKey(Key.ofInt(COL_NAME1, 1))
              .blobValue(COL_NAME2, blobDataCol2)
              .blobValue(COL_NAME3, blobDataCol3)
              .build();

      // Act
      storage.put(put);

      // Assert
      Optional<Result> optionalResult =
          storage.get(
              Get.newBuilder()
                  .namespace(namespace)
                  .table(tableName)
                  .partitionKey(Key.ofInt(COL_NAME1, 1))
                  .build());
      assertThat(optionalResult).isPresent();
      Result result = optionalResult.get();
      assertThat(result.getColumns().get(COL_NAME2).getBlobValueAsBytes()).isEqualTo(blobDataCol2);
      assertThat(result.getColumns().get(COL_NAME3).getBlobValueAsBytes()).isEqualTo(blobDataCol3);
    } finally {
      admin.dropTable(namespace, tableName, true);
    }
  }

  @EnabledIf("com.scalar.db.storage.jdbc.JdbcEnv#isOracle")
  @Test
  public void put_largeBlobData_WithAllColumnsTypesShouldWorkCorrectly() throws ExecutionException {
    // Arrange
    IntColumn partitionKeyValue = (IntColumn) getColumnWithMaxValue(PARTITION_KEY, DataType.INT);
    BooleanColumn col1Value = (BooleanColumn) getColumnWithMaxValue(COL_NAME1, DataType.BOOLEAN);
    IntColumn col2Value = (IntColumn) getColumnWithMaxValue(COL_NAME2, DataType.INT);
    BigIntColumn col3Value = (BigIntColumn) getColumnWithMaxValue(COL_NAME3, DataType.BIGINT);
    FloatColumn col4Value = (FloatColumn) getColumnWithMaxValue(COL_NAME4, DataType.FLOAT);
    DoubleColumn col5Value = (DoubleColumn) getColumnWithMaxValue(COL_NAME5, DataType.DOUBLE);
    TextColumn col6Value = (TextColumn) getColumnWithMaxValue(COL_NAME6, DataType.TEXT);
    BlobColumn col7Value = BlobColumn.of(COL_NAME7, createLargeBlob(32_766));
    DateColumn col8Value = (DateColumn) getColumnWithMaxValue(COL_NAME8, DataType.DATE);
    TimeColumn col9Value = (TimeColumn) getColumnWithMaxValue(COL_NAME9, DataType.TIME);
    TimestampTZColumn col10Value =
        (TimestampTZColumn) getColumnWithMaxValue(COL_NAME10, DataType.TIMESTAMPTZ);
    TimestampColumn column11Value = null;
    if (isTimestampTypeSupported()) {
      column11Value = (TimestampColumn) getColumnWithMaxValue(COL_NAME11, DataType.TIMESTAMP);
    }

    PutBuilder.Buildable put =
        Put.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(Key.newBuilder().add(partitionKeyValue).build())
            .value(col1Value)
            .value(col2Value)
            .value(col3Value)
            .value(col4Value)
            .value(col5Value)
            .value(col6Value)
            .value(col7Value)
            .value(col8Value)
            .value(col9Value)
            .value(col10Value);
    if (isTimestampTypeSupported()) {
      put.value(column11Value);
    }
    // Act
    storage.put(put.build());

    // Assert
    assertResult(
        partitionKeyValue,
        col1Value,
        col2Value,
        col3Value,
        col4Value,
        col5Value,
        col6Value,
        col7Value,
        col8Value,
        col9Value,
        col10Value,
        column11Value);
  }
}
