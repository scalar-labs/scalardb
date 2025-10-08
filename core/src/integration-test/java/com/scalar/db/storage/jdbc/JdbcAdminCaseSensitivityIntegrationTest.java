package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdminCaseSensitivityIntegrationTestBase;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.condition.EnabledIf;

public class JdbcAdminCaseSensitivityIntegrationTest
    extends DistributedStorageAdminCaseSensitivityIntegrationTestBase {
  private RdbEngineStrategy rdbEngine;

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = JdbcEnv.getProperties(testName);
    rdbEngine = RdbEngineFactory.create(new JdbcConfig(new DatabaseConfig(properties)));
    return properties;
  }

  @Override
  protected String getSystemNamespaceName(Properties properties) {
    return new JdbcConfig(new DatabaseConfig(properties))
        .getTableMetadataSchema()
        .orElse(DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME);
  }

  // Since SQLite doesn't have persistent namespaces, some behaviors around the namespace are
  // different from the other adapters. So disable several tests that check such behaviors.

  @SuppressWarnings("unused")
  private boolean isSqlite() {
    return JdbcEnv.isSqlite();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void createNamespace_ForNonExistingNamespace_ShouldCreateNamespaceProperly()
      throws ExecutionException {
    super.createNamespace_ForNonExistingNamespace_ShouldCreateNamespaceProperly();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void createNamespace_ForExistingNamespace_ShouldThrowIllegalArgumentException() {
    super.createNamespace_ForExistingNamespace_ShouldThrowIllegalArgumentException();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void createNamespace_IfNotExists_ForExistingNamespace_ShouldNotThrowAnyException() {
    super.createNamespace_IfNotExists_ForExistingNamespace_ShouldNotThrowAnyException();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void dropNamespace_ForNonExistingNamespace_ShouldDropNamespaceProperly()
      throws ExecutionException {
    super.dropNamespace_ForNonExistingNamespace_ShouldDropNamespaceProperly();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void dropNamespace_ForNonExistingNamespace_ShouldThrowIllegalArgumentException() {
    super.dropNamespace_ForNonExistingNamespace_ShouldThrowIllegalArgumentException();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void dropNamespace_ForNonEmptyNamespace_ShouldThrowIllegalArgumentException()
      throws ExecutionException {
    super.dropNamespace_ForNonEmptyNamespace_ShouldThrowIllegalArgumentException();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void dropNamespace_IfExists_ForNonExistingNamespace_ShouldNotThrowAnyException() {
    super.dropNamespace_IfExists_ForNonExistingNamespace_ShouldNotThrowAnyException();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void namespaceExists_ShouldReturnCorrectResults() throws ExecutionException {
    super.namespaceExists_ShouldReturnCorrectResults();
  }

  @Test
  @Override
  @DisabledIf("isSqlite")
  public void createTable_ForNonExistingNamespace_ShouldThrowIllegalArgumentException() {
    super.createTable_ForNonExistingNamespace_ShouldThrowIllegalArgumentException();
  }

  @Override
  protected boolean isCreateIndexOnTextColumnEnabled() {
    // "admin.createIndex()" for TEXT column fails (the "create index" query runs
    // indefinitely) on Db2 community edition version but works on Db2 hosted on IBM Cloud.
    // So we disable these tests until the issue is resolved.
    return !JdbcTestUtils.isDb2(rdbEngine);
  }

  @SuppressWarnings("unused")
  private boolean isOracle() {
    return JdbcEnv.isOracle();
  }

  @SuppressWarnings("unused")
  private boolean isDb2() {
    return JdbcEnv.isDb2();
  }

  @SuppressWarnings("unused")
  private boolean isColumnTypeConversionToTextNotFullySupported() {
    return JdbcTestUtils.isDb2(rdbEngine)
        || JdbcTestUtils.isOracle(rdbEngine)
        || JdbcTestUtils.isSqlite(rdbEngine);
  }

  @SuppressWarnings("unused")
  private boolean isWideningColumnTypeConversionNotFullySupported() {
    return JdbcTestUtils.isOracle(rdbEngine) || JdbcTestUtils.isSqlite(rdbEngine);
  }

  @Test
  @Override
  @DisabledIf("isDb2")
  public void renameColumn_ForPrimaryKeyColumn_ShouldRenameColumnCorrectly()
      throws ExecutionException {
    super.renameColumn_ForPrimaryKeyColumn_ShouldRenameColumnCorrectly();
  }

  @Test
  @Override
  @DisabledIf("isDb2")
  public void renameColumn_ForIndexKeyColumn_ShouldRenameColumnAndIndexCorrectly()
      throws ExecutionException {
    super.renameColumn_ForIndexKeyColumn_ShouldRenameColumnAndIndexCorrectly();
  }

  @Test
  @EnabledIf("isDb2")
  public void renameColumn_Db2_ForPrimaryOrIndexKeyColumn_ShouldThrowUnsupportedOperationException()
      throws ExecutionException {
    try {
      // Arrange
      Map<String, String> options = getCreationOptions();
      TableMetadata currentTableMetadata =
          TableMetadata.newBuilder()
              .addColumn(getColumnName1(), DataType.INT)
              .addColumn(getColumnName2(), DataType.INT)
              .addColumn(getColumnName3(), DataType.TEXT)
              .addPartitionKey(getColumnName1())
              .addClusteringKey(getColumnName2())
              .addSecondaryIndex(getColumnName3())
              .build();
      admin.createTable(getNamespace1(), getTable4(), currentTableMetadata, options);

      // Act Assert
      assertThatCode(
              () ->
                  admin.renameColumn(
                      getNamespace1(), getTable4(), getColumnName1(), getColumnName4()))
          .isInstanceOf(UnsupportedOperationException.class);
      assertThatCode(
              () ->
                  admin.renameColumn(
                      getNamespace1(), getTable4(), getColumnName2(), getColumnName4()))
          .isInstanceOf(UnsupportedOperationException.class);
      assertThatCode(
              () ->
                  admin.renameColumn(
                      getNamespace1(), getTable4(), getColumnName3(), getColumnName4()))
          .isInstanceOf(UnsupportedOperationException.class);
    } finally {
      admin.dropTable(getNamespace1(), getTable4(), true);
    }
  }

  @Test
  @Override
  @DisabledIf("isColumnTypeConversionToTextNotFullySupported")
  public void
      alterColumnType_AlterColumnTypeFromEachExistingDataTypeToText_ShouldAlterColumnTypesCorrectly()
          throws ExecutionException, IOException {
    super
        .alterColumnType_AlterColumnTypeFromEachExistingDataTypeToText_ShouldAlterColumnTypesCorrectly();
  }

  @Test
  @EnabledIf("isOracle")
  public void
      alterColumnType_Oracle_AlterColumnTypeFromEachExistingDataTypeToText_ShouldThrowUnsupportedOperationException()
          throws ExecutionException {
    try (DistributedStorage storage = storageFactory.getStorage()) {
      // Arrange
      Map<String, String> options = getCreationOptions();
      TableMetadata.Builder currentTableMetadataBuilder =
          TableMetadata.newBuilder()
              .addColumn(getColumnName1(), DataType.INT)
              .addColumn(getColumnName2(), DataType.INT)
              .addColumn(getColumnName3(), DataType.INT)
              .addColumn(getColumnName4(), DataType.BIGINT)
              .addColumn(getColumnName5(), DataType.FLOAT)
              .addColumn(getColumnName6(), DataType.DOUBLE)
              .addColumn(getColumnName7(), DataType.TEXT)
              .addColumn(getColumnName8(), DataType.BLOB)
              .addColumn(getColumnName9(), DataType.DATE)
              .addColumn(getColumnName10(), DataType.TIME)
              .addPartitionKey(getColumnName1())
              .addClusteringKey(getColumnName2(), Scan.Ordering.Order.ASC);
      if (isTimestampTypeSupported()) {
        currentTableMetadataBuilder
            .addColumn(getColumnName11(), DataType.TIMESTAMP)
            .addColumn(getColumnName12(), DataType.TIMESTAMPTZ);
      }
      TableMetadata currentTableMetadata = currentTableMetadataBuilder.build();
      admin.createTable(getNamespace1(), getTable4(), currentTableMetadata, options);
      PutBuilder.Buildable put =
          Put.newBuilder()
              .namespace(getNamespace1())
              .table(getTable4())
              .partitionKey(Key.ofInt(getColumnName1(), 1))
              .clusteringKey(Key.ofInt(getColumnName2(), 2))
              .intValue(getColumnName3(), 1)
              .bigIntValue(getColumnName4(), 2L)
              .floatValue(getColumnName5(), 3.0f)
              .doubleValue(getColumnName6(), 4.0d)
              .textValue(getColumnName7(), "5")
              .blobValue(getColumnName8(), "6".getBytes(StandardCharsets.UTF_8))
              .dateValue(getColumnName9(), LocalDate.now(ZoneId.of("UTC")))
              .timeValue(getColumnName10(), LocalTime.now(ZoneId.of("UTC")));
      if (isTimestampTypeSupported()) {
        put.timestampValue(getColumnName11(), LocalDateTime.now(ZoneOffset.UTC));
        put.timestampTZValue(getColumnName12(), Instant.now());
      }
      storage.put(put.build());
      storage.close();

      // Act Assert
      assertThatCode(
              () ->
                  admin.alterColumnType(
                      getNamespace1(), getTable4(), getColumnName3(), DataType.TEXT))
          .isInstanceOf(UnsupportedOperationException.class);
      assertThatCode(
              () ->
                  admin.alterColumnType(
                      getNamespace1(), getTable4(), getColumnName4(), DataType.TEXT))
          .isInstanceOf(UnsupportedOperationException.class);
      assertThatCode(
              () ->
                  admin.alterColumnType(
                      getNamespace1(), getTable4(), getColumnName5(), DataType.TEXT))
          .isInstanceOf(UnsupportedOperationException.class);
      assertThatCode(
              () ->
                  admin.alterColumnType(
                      getNamespace1(), getTable4(), getColumnName6(), DataType.TEXT))
          .isInstanceOf(UnsupportedOperationException.class);
      assertThatCode(
              () ->
                  admin.alterColumnType(
                      getNamespace1(), getTable4(), getColumnName7(), DataType.TEXT))
          .doesNotThrowAnyException();
      assertThatCode(
              () ->
                  admin.alterColumnType(
                      getNamespace1(), getTable4(), getColumnName8(), DataType.TEXT))
          .isInstanceOf(UnsupportedOperationException.class);
      assertThatCode(
              () ->
                  admin.alterColumnType(
                      getNamespace1(), getTable4(), getColumnName9(), DataType.TEXT))
          .isInstanceOf(UnsupportedOperationException.class);
      assertThatCode(
              () ->
                  admin.alterColumnType(
                      getNamespace1(), getTable4(), getColumnName10(), DataType.TEXT))
          .isInstanceOf(UnsupportedOperationException.class);
      if (isTimestampTypeSupported()) {
        assertThatCode(
                () ->
                    admin.alterColumnType(
                        getNamespace1(), getTable4(), getColumnName11(), DataType.TEXT))
            .isInstanceOf(UnsupportedOperationException.class);
        assertThatCode(
                () ->
                    admin.alterColumnType(
                        getNamespace1(), getTable4(), getColumnName12(), DataType.TEXT))
            .isInstanceOf(UnsupportedOperationException.class);
      }
    } finally {
      admin.dropTable(getNamespace1(), getTable4(), true);
    }
  }

  @Test
  @EnabledIf("isDb2")
  public void
      alterColumnType_Db2_AlterColumnTypeFromEachExistingDataTypeToText_ShouldAlterColumnTypesCorrectlyIfSupported()
          throws ExecutionException {
    try (DistributedStorage storage = storageFactory.getStorage()) {
      // Arrange
      Map<String, String> options = getCreationOptions();
      TableMetadata.Builder currentTableMetadataBuilder =
          TableMetadata.newBuilder()
              .addColumn(getColumnName1(), DataType.INT)
              .addColumn(getColumnName2(), DataType.INT)
              .addColumn(getColumnName3(), DataType.INT)
              .addColumn(getColumnName4(), DataType.BIGINT)
              .addColumn(getColumnName5(), DataType.FLOAT)
              .addColumn(getColumnName6(), DataType.DOUBLE)
              .addColumn(getColumnName7(), DataType.TEXT)
              .addColumn(getColumnName8(), DataType.BLOB)
              .addColumn(getColumnName9(), DataType.DATE)
              .addColumn(getColumnName10(), DataType.TIME)
              .addPartitionKey(getColumnName1())
              .addClusteringKey(getColumnName2(), Scan.Ordering.Order.ASC);
      if (isTimestampTypeSupported()) {
        currentTableMetadataBuilder
            .addColumn(getColumnName11(), DataType.TIMESTAMP)
            .addColumn(getColumnName12(), DataType.TIMESTAMPTZ);
      }
      TableMetadata currentTableMetadata = currentTableMetadataBuilder.build();
      admin.createTable(getNamespace1(), getTable4(), currentTableMetadata, options);
      PutBuilder.Buildable put =
          Put.newBuilder()
              .namespace(getNamespace1())
              .table(getTable4())
              .partitionKey(Key.ofInt(getColumnName1(), 1))
              .clusteringKey(Key.ofInt(getColumnName2(), 2))
              .intValue(getColumnName3(), 1)
              .bigIntValue(getColumnName4(), 2L)
              .floatValue(getColumnName5(), 3.0f)
              .doubleValue(getColumnName6(), 4.0d)
              .textValue(getColumnName7(), "5")
              .blobValue(getColumnName8(), "6".getBytes(StandardCharsets.UTF_8))
              .dateValue(getColumnName9(), LocalDate.now(ZoneId.of("UTC")))
              .timeValue(getColumnName10(), LocalTime.now(ZoneId.of("UTC")));
      if (isTimestampTypeSupported()) {
        put.timestampValue(getColumnName11(), LocalDateTime.now(ZoneOffset.UTC));
        put.timestampTZValue(getColumnName12(), Instant.now());
      }
      storage.put(put.build());
      storage.close();

      // Act Assert
      assertThatCode(
              () ->
                  admin.alterColumnType(
                      getNamespace1(), getTable4(), getColumnName3(), DataType.TEXT))
          .doesNotThrowAnyException();
      assertThatCode(
              () ->
                  admin.alterColumnType(
                      getNamespace1(), getTable4(), getColumnName4(), DataType.TEXT))
          .doesNotThrowAnyException();
      assertThatCode(
              () ->
                  admin.alterColumnType(
                      getNamespace1(), getTable4(), getColumnName5(), DataType.TEXT))
          .doesNotThrowAnyException();
      assertThatCode(
              () ->
                  admin.alterColumnType(
                      getNamespace1(), getTable4(), getColumnName6(), DataType.TEXT))
          .doesNotThrowAnyException();
      assertThatCode(
              () ->
                  admin.alterColumnType(
                      getNamespace1(), getTable4(), getColumnName7(), DataType.TEXT))
          .doesNotThrowAnyException();
      assertThatCode(
              () ->
                  admin.alterColumnType(
                      getNamespace1(), getTable4(), getColumnName8(), DataType.TEXT))
          .isInstanceOf(UnsupportedOperationException.class);
      assertThatCode(
              () ->
                  admin.alterColumnType(
                      getNamespace1(), getTable4(), getColumnName9(), DataType.TEXT))
          .doesNotThrowAnyException();
      assertThatCode(
              () ->
                  admin.alterColumnType(
                      getNamespace1(), getTable4(), getColumnName10(), DataType.TEXT))
          .doesNotThrowAnyException();
      if (isTimestampTypeSupported()) {
        assertThatCode(
                () ->
                    admin.alterColumnType(
                        getNamespace1(), getTable4(), getColumnName11(), DataType.TEXT))
            .doesNotThrowAnyException();
        assertThatCode(
                () ->
                    admin.alterColumnType(
                        getNamespace1(), getTable4(), getColumnName12(), DataType.TEXT))
            .doesNotThrowAnyException();
      }

      TableMetadata.Builder expectedTableMetadataBuilder =
          TableMetadata.newBuilder()
              .addColumn(getColumnName1(), DataType.INT)
              .addColumn(getColumnName2(), DataType.INT)
              .addColumn(getColumnName3(), DataType.TEXT)
              .addColumn(getColumnName4(), DataType.TEXT)
              .addColumn(getColumnName5(), DataType.TEXT)
              .addColumn(getColumnName6(), DataType.TEXT)
              .addColumn(getColumnName7(), DataType.TEXT)
              .addColumn(getColumnName8(), DataType.BLOB)
              .addColumn(getColumnName9(), DataType.TEXT)
              .addColumn(getColumnName10(), DataType.TEXT)
              .addPartitionKey(getColumnName1())
              .addClusteringKey(getColumnName2(), Scan.Ordering.Order.ASC);
      if (isTimestampTypeSupported()) {
        expectedTableMetadataBuilder
            .addColumn(getColumnName11(), DataType.TEXT)
            .addColumn(getColumnName12(), DataType.TEXT);
      }
      TableMetadata expectedTableMetadata = expectedTableMetadataBuilder.build();
      assertThat(admin.getTableMetadata(getNamespace1(), getTable4()))
          .isEqualTo(expectedTableMetadata);
    } finally {
      admin.dropTable(getNamespace1(), getTable4(), true);
    }
  }

  @Test
  @Override
  @DisabledIf("isWideningColumnTypeConversionNotFullySupported")
  public void alterColumnType_WideningConversion_ShouldAlterColumnTypesCorrectly()
      throws ExecutionException, IOException {
    super.alterColumnType_WideningConversion_ShouldAlterColumnTypesCorrectly();
  }

  @Test
  @EnabledIf("isOracle")
  public void alterColumnType_Oracle_WideningConversion_ShouldAlterColumnTypesCorrectly()
      throws ExecutionException, IOException {
    try {
      // Arrange
      Map<String, String> options = getCreationOptions();
      TableMetadata.Builder currentTableMetadataBuilder =
          TableMetadata.newBuilder()
              .addColumn(getColumnName1(), DataType.INT)
              .addColumn(getColumnName2(), DataType.INT)
              .addColumn(getColumnName3(), DataType.INT)
              .addColumn(getColumnName4(), DataType.FLOAT)
              .addPartitionKey(getColumnName1())
              .addClusteringKey(getColumnName2(), Scan.Ordering.Order.ASC);
      TableMetadata currentTableMetadata = currentTableMetadataBuilder.build();
      admin.createTable(getNamespace1(), getTable4(), currentTableMetadata, options);

      int expectedColumn3Value = 1;
      float expectedColumn4Value = 4.0f;
      try (DistributedStorage storage = storageFactory.getStorage()) {
        PutBuilder.Buildable put =
            Put.newBuilder()
                .namespace(getNamespace1())
                .table(getTable4())
                .partitionKey(Key.ofInt(getColumnName1(), 1))
                .clusteringKey(Key.ofInt(getColumnName2(), 2))
                .intValue(getColumnName3(), expectedColumn3Value)
                .floatValue(getColumnName4(), expectedColumn4Value);
        storage.put(put.build());
      }

      // Act
      admin.alterColumnType(getNamespace1(), getTable4(), getColumnName3(), DataType.BIGINT);
      Throwable exception =
          catchThrowable(
              () ->
                  admin.alterColumnType(
                      getNamespace1(), getTable4(), getColumnName4(), DataType.DOUBLE));

      // Assert
      assertThat(exception).isInstanceOf(UnsupportedOperationException.class);
      TableMetadata.Builder expectedTableMetadataBuilder =
          TableMetadata.newBuilder()
              .addColumn(getColumnName1(), DataType.INT)
              .addColumn(getColumnName2(), DataType.INT)
              .addColumn(getColumnName3(), DataType.BIGINT)
              .addColumn(getColumnName4(), DataType.FLOAT)
              .addPartitionKey(getColumnName1())
              .addClusteringKey(getColumnName2(), Scan.Ordering.Order.ASC);
      TableMetadata expectedTableMetadata = expectedTableMetadataBuilder.build();
      assertThat(admin.getTableMetadata(getNamespace1(), getTable4()))
          .isEqualTo(expectedTableMetadata);

      try (DistributedStorage storage = storageFactory.getStorage()) {
        Scan scan =
            Scan.newBuilder()
                .namespace(getNamespace1())
                .table(getTable4())
                .partitionKey(Key.ofInt(getColumnName1(), 1))
                .build();
        try (Scanner scanner = storage.scan(scan)) {
          List<Result> results = scanner.all();
          assertThat(results).hasSize(1);
          Result result = results.get(0);
          assertThat(result.getBigInt(getColumnName3())).isEqualTo(expectedColumn3Value);
        }
      }
    } finally {
      admin.dropTable(getNamespace1(), getTable4(), true);
    }
  }

  @Test
  @EnabledIf("isSqlite")
  public void alterColumnType_Sqlite_AlterColumnType_ShouldThrowUnsupportedOperationException()
      throws ExecutionException {
    try {
      // Arrange
      Map<String, String> options = getCreationOptions();
      TableMetadata.Builder currentTableMetadataBuilder =
          TableMetadata.newBuilder()
              .addColumn(getColumnName1(), DataType.INT)
              .addColumn(getColumnName2(), DataType.INT)
              .addColumn(getColumnName3(), DataType.INT)
              .addPartitionKey(getColumnName1())
              .addClusteringKey(getColumnName2(), Scan.Ordering.Order.ASC);
      TableMetadata currentTableMetadata = currentTableMetadataBuilder.build();
      admin.createTable(getNamespace1(), getTable4(), currentTableMetadata, options);

      // Act Assert
      assertThatCode(
              () ->
                  admin.alterColumnType(
                      getNamespace1(), getTable4(), getColumnName3(), DataType.TEXT))
          .isInstanceOf(UnsupportedOperationException.class);
    } finally {
      admin.dropTable(getNamespace1(), getTable4(), true);
    }
  }

  @Override
  protected boolean isIndexOnBlobColumnSupported() {
    return !JdbcTestUtils.isDb2(rdbEngine);
  }
}
