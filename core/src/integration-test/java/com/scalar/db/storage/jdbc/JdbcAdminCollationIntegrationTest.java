package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.VirtualTableJoinType;
import com.scalar.db.common.CoreError;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.condition.EnabledIf;

public class JdbcAdminCollationIntegrationTest {
  private static final String NAMESPACE = "collation_ns";
  private static final String TABLE = "collation_tbl";
  private static final String TEXT_PK = "pk";
  private static final String TEXT_CK = "ck";
  private static final String TEXT_INDEX = "idx";
  private static final String TEXT_PLAIN = "plain";
  private static final String OLD_SOURCE_TABLE = "old_source";
  private static final String NEW_SOURCE_TABLE = "new_source";
  private static final String VIRTUAL_TABLE = "virtual_tbl";

  private Properties properties;
  private RdbEngineStrategy rdbEngine;
  private StorageFactory storageFactory;
  private DistributedStorageAdmin admin;
  private JdbcAdminTestUtils testUtils;

  @SuppressWarnings("unused")
  private static boolean isDb2OrSpanner() {
    return JdbcEnv.isDb2() || JdbcEnv.isSpanner();
  }

  @SuppressWarnings("unused")
  private static boolean isDb2SqliteOrSpanner() {
    return JdbcEnv.isDb2() || JdbcEnv.isSqlite() || JdbcEnv.isSpanner();
  }

  @SuppressWarnings("unused")
  private static boolean isOracleOrSqlServer() {
    return JdbcEnv.isOracle() || JdbcEnv.isSqlServer();
  }

  @SuppressWarnings("unused")
  private static boolean isNotMysqlOrPostgresqlFamily() {
    return JdbcEnv.isOracle()
        || JdbcEnv.isSqlServer()
        || JdbcEnv.isSqlite()
        || JdbcEnv.isDb2()
        || JdbcEnv.isSpanner();
  }

  @SuppressWarnings("unused")
  private static boolean isSqliteOrSpanner() {
    return JdbcEnv.isSqlite() || JdbcEnv.isSpanner();
  }

  @BeforeEach
  void setUp() throws Exception {
    properties = JdbcEnv.getProperties("collation");
    rdbEngine = RdbEngineFactory.create(new JdbcConfig(new DatabaseConfig(properties)));
    storageFactory = StorageFactory.create(properties);
    admin = storageFactory.getStorageAdmin();
    testUtils = new JdbcAdminTestUtils(properties);
    cleanUp();
  }

  private void createTableOnDefaultCollation() throws Exception {
    createTableWithColumnCollationDisabled(
        TABLE,
        TableMetadata.newBuilder()
            .addColumn(TEXT_PK, DataType.TEXT)
            .addColumn(TEXT_PLAIN, DataType.TEXT)
            .addPartitionKey(TEXT_PK)
            .build());
  }

  // With the column collation setting on, createTable declares a collation on every TEXT column
  private void createTableWithColumnCollationDisabled(String table, TableMetadata metadata)
      throws Exception {
    Properties props = new Properties();
    props.putAll(properties);
    props.setProperty(JdbcConfig.COLUMN_COLLATION_ENABLED, "false");
    DistributedStorageAdmin adminWithoutCollation = StorageFactory.create(props).getStorageAdmin();
    try {
      adminWithoutCollation.createNamespace(NAMESPACE, true);
      adminWithoutCollation.createTable(NAMESPACE, table, metadata, true);
    } finally {
      adminWithoutCollation.close();
    }
  }

  @AfterEach
  void tearDown() throws Exception {
    cleanUp();
    admin.close();
    testUtils.close();
  }

  private void cleanUp() throws Exception {
    if (JdbcTestUtils.isOracle(rdbEngine)) {
      String schema = new JdbcConfig(new DatabaseConfig(properties)).getMetadataSchema();
      for (String user : Arrays.asList(NAMESPACE, schema)) {
        if (testUtils.namespaceExists(user)) {
          testUtils.executeSql("DROP USER " + rdbEngine.enclose(user) + " CASCADE");
        }
      }
    }
    if (testUtils.tableExists(NAMESPACE, TABLE)) {
      admin.dropTable(NAMESPACE, TABLE);
    }
    if (JdbcTestUtils.isMysql(rdbEngine)) {
      testUtils.executeSql("DROP DATABASE IF EXISTS `" + NAMESPACE + "`");
    } else {
      admin.dropNamespace(NAMESPACE, true);
    }
    String metadataSchema = new JdbcConfig(new DatabaseConfig(properties)).getMetadataSchema();
    if (testUtils.tableExists(metadataSchema, TableMetadataService.TABLE_NAME)) {
      testUtils.dropMetadataTable();
    }
    if (testUtils.tableExists(metadataSchema, NamespaceMetadataService.TABLE_NAME)) {
      testUtils.dropNamespacesTable();
    }
  }

  private String expectedCollation() {
    String clause = rdbEngine.getBinaryTextCollationClause();
    String last = clause.substring(clause.lastIndexOf(' ') + 1);
    return last.replace("\"", "");
  }

  private TableMetadata textTableMetadata() {
    return TableMetadata.newBuilder()
        .addColumn(TEXT_PK, DataType.TEXT)
        .addColumn(TEXT_CK, DataType.TEXT)
        .addColumn(TEXT_INDEX, DataType.TEXT)
        .addColumn(TEXT_PLAIN, DataType.TEXT)
        .addPartitionKey(TEXT_PK)
        .addClusteringKey(TEXT_CK)
        .addSecondaryIndex(TEXT_INDEX)
        .build();
  }

  @Test
  @DisabledIf("isDb2OrSpanner")
  void createTable_TextColumns_ShouldUseBinaryCollation() throws Exception {
    // Arrange
    admin.createNamespace(NAMESPACE, true);

    // Act
    admin.createTable(NAMESPACE, TABLE, textTableMetadata(), true);

    // Assert
    for (String column : Arrays.asList(TEXT_PK, TEXT_CK, TEXT_INDEX, TEXT_PLAIN)) {
      assertThat(testUtils.getColumnCollation(NAMESPACE, TABLE, column))
          .as(column)
          .isEqualTo(expectedCollation());
    }
  }

  @Test
  @DisabledIf("isDb2OrSpanner")
  void createTable_InNamespaceWithCaseInsensitiveDefault_ShouldUseBinaryCollation()
      throws Exception {
    // Arrange
    assumeTrue(JdbcTestUtils.isMysql(rdbEngine));
    testUtils.executeSql(
        "CREATE DATABASE `" + NAMESPACE + "` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci");
    admin.repairNamespace(NAMESPACE, Collections.emptyMap());

    // Act
    admin.createTable(NAMESPACE, TABLE, textTableMetadata(), true);

    // Assert
    for (String column : Arrays.asList(TEXT_PK, TEXT_CK, TEXT_INDEX, TEXT_PLAIN)) {
      assertThat(testUtils.getColumnCollation(NAMESPACE, TABLE, column))
          .as(column)
          .isEqualTo(expectedCollation());
    }
  }

  @Test
  @DisabledIf("isDb2OrSpanner")
  void createNamespace_SystemTables_ShouldUseBinaryCollation() throws Exception {
    // Arrange
    String metadataSchema = new JdbcConfig(new DatabaseConfig(properties)).getMetadataSchema();

    // Act
    admin.createNamespace(NAMESPACE, true);
    admin.createTable(NAMESPACE, TABLE, textTableMetadata(), true);

    // Assert
    assertThat(
            testUtils.getColumnCollation(
                metadataSchema, NamespaceMetadataService.TABLE_NAME, "namespace_name"))
        .isEqualTo(expectedCollation());
    assertThat(
            testUtils.getColumnCollation(
                metadataSchema, TableMetadataService.TABLE_NAME, "full_table_name"))
        .isEqualTo(expectedCollation());
  }

  @Test
  @DisabledIf("isDb2OrSpanner")
  void put_KeysDifferingOnlyByTrailingSpace_ShouldBeDistinct() throws Exception {
    // Arrange
    String clause = rdbEngine.getBinaryTextCollationClause();
    assumeTrue(!JdbcTestUtils.isSqlServer(rdbEngine));
    assumeTrue(!clause.endsWith("utf8mb4_bin"));
    admin.createNamespace(NAMESPACE, true);
    admin.createTable(
        NAMESPACE,
        TABLE,
        TableMetadata.newBuilder()
            .addColumn(TEXT_PK, DataType.TEXT)
            .addColumn(TEXT_PLAIN, DataType.INT)
            .addPartitionKey(TEXT_PK)
            .build(),
        true);

    // Act
    try (DistributedStorage storage = storageFactory.getStorage()) {
      for (String key : Arrays.asList("a", "a ")) {
        storage.put(
            Put.newBuilder()
                .namespace(NAMESPACE)
                .table(TABLE)
                .partitionKey(Key.ofText(TEXT_PK, key))
                .intValue(TEXT_PLAIN, 1)
                .build());
      }

      // Assert
      try (Scanner scanner =
          storage.scan(Scan.newBuilder().namespace(NAMESPACE).table(TABLE).all().build())) {
        List<Result> results = scanner.all();
        assertThat(results).hasSize(2);
      }
    }
  }

  @Test
  @DisabledIf("isSqliteOrSpanner")
  void getTextColumnCollations_TableCreatedByAdmin_ShouldReportBinaryUtf8() throws Exception {
    // Arrange
    admin.createNamespace(NAMESPACE, true);
    admin.createTable(NAMESPACE, TABLE, textTableMetadata(), true);

    // Act
    Map<String, TextColumnCollation> collations =
        testUtils.withCatalogConnection(
            connection -> rdbEngine.getTextColumnCollations(connection, NAMESPACE, TABLE));

    // Assert
    assertThat(collations.keySet())
        .containsExactlyInAnyOrder(TEXT_PK, TEXT_CK, TEXT_INDEX, TEXT_PLAIN);
    for (Map.Entry<String, TextColumnCollation> entry : collations.entrySet()) {
      assertThat(rdbEngine.isBinaryCollation(entry.getValue())).as(entry.toString()).isTrue();
      assertThat(rdbEngine.isFullUnicodeCharset(entry.getValue())).as(entry.toString()).isTrue();
    }
  }

  @Test
  @DisabledIf("isSqliteOrSpanner")
  void getTextColumnCollations_MixedCaseNames_ShouldFindColumns() throws Exception {
    // Arrange
    String namespace = "MixedCase";
    String table = "MixedCase";
    try {
      admin.createNamespace(namespace, true);
      admin.createTable(
          namespace,
          table,
          TableMetadata.newBuilder().addColumn("Col", DataType.TEXT).addPartitionKey("Col").build(),
          true);

      // Act
      Map<String, TextColumnCollation> collations =
          testUtils.withCatalogConnection(
              connection -> rdbEngine.getTextColumnCollations(connection, namespace, table));

      // Assert
      assertThat(collations).containsKey("Col");
    } finally {
      if (JdbcTestUtils.isOracle(rdbEngine)) {
        if (testUtils.namespaceExists(namespace)) {
          testUtils.executeSql("DROP USER " + rdbEngine.enclose(namespace) + " CASCADE");
        }
      } else if (JdbcTestUtils.isMysql(rdbEngine)) {
        testUtils.executeSql("DROP DATABASE IF EXISTS `" + namespace + "`");
      } else {
        admin.dropTable(namespace, table, true);
        admin.dropNamespace(namespace, true);
      }
    }
  }

  @Test
  void getDatabaseEncodingIssue_CiDatabase_ShouldReturnNull() throws Exception {
    // Arrange Act
    DatabaseEncodingIssue encoding =
        testUtils.withCatalogConnection(rdbEngine::getDatabaseEncodingIssue);

    // Assert
    assertThat(encoding).isNull();
  }

  @Test
  void isBinaryCollation_PostgresqlColumnOnDefaultCollation_ShouldResolveDatabaseCollation()
      throws Exception {
    // Arrange
    assumeTrue(JdbcTestUtils.isPostgresql(rdbEngine) && !JdbcTestUtils.isSpanner(rdbEngine));
    admin.createNamespace(NAMESPACE, true);
    testUtils.executeSql(
        "CREATE TABLE "
            + rdbEngine.encloseFullTableName(NAMESPACE, TABLE)
            + "(\"pk\" text PRIMARY KEY)");
    try {
      // Act
      Map<String, TextColumnCollation> collations =
          testUtils.withCatalogConnection(
              connection -> rdbEngine.getTextColumnCollations(connection, NAMESPACE, TABLE));

      // Assert
      assertThat(collations.get("pk").getCollation()).isEqualTo("default");
      assertThat(rdbEngine.isBinaryCollation(collations.get("pk"))).isTrue();
    } finally {
      // The table has no ScalarDB metadata, so the admin cannot drop it.
      testUtils.dropTable(NAMESPACE, TABLE);
    }
  }

  private String nonBinaryCollation() {
    if (JdbcTestUtils.isTidb(rdbEngine)) {
      return "utf8mb4_general_ci";
    }
    if (JdbcTestUtils.isMysql(rdbEngine)) {
      return rdbEngine.getBinaryTextCollationClause().endsWith("utf8mb4_0900_bin")
          ? "utf8mb4_0900_ai_ci"
          : "utf8mb4_unicode_ci";
    }
    if (JdbcTestUtils.isPostgresql(rdbEngine)) {
      return "und-x-icu";
    }
    if (JdbcTestUtils.isOracle(rdbEngine)) {
      return "BINARY_CI";
    }
    if (JdbcTestUtils.isSqlServer(rdbEngine)) {
      return "Latin1_General_100_CI_AS_SC_UTF8";
    }
    throw new AssertionError();
  }

  private void createTableWithNonBinaryCollationOnPlainColumn() throws Exception {
    admin.createNamespace(NAMESPACE, true);
    admin.createTable(NAMESPACE, TABLE, textTableMetadata(), true);
    String collation = nonBinaryCollation();
    String clause;
    if (JdbcTestUtils.isMysql(rdbEngine)) {
      clause = "CHARACTER SET utf8mb4 COLLATE " + collation;
    } else if (JdbcTestUtils.isPostgresql(rdbEngine)) {
      clause = "COLLATE \"" + collation + "\"";
    } else {
      clause = "COLLATE " + collation;
    }
    String type = rdbEngine.getDataTypeForEngine(DataType.TEXT) + " " + clause;
    for (String sql : rdbEngine.alterColumnTypeSql(NAMESPACE, TABLE, TEXT_PLAIN, type)) {
      testUtils.executeSql(sql);
    }
  }

  @Test
  @DisabledIf("isNotMysqlOrPostgresqlFamily")
  void textColumnCollationSql_ColumnWithDefaultCollation_ShouldReturnNoRow() throws Exception {
    // Arrange
    createTableOnDefaultCollation();

    // Act
    String collation = testUtils.readTextColumnCollation(NAMESPACE, TABLE, TEXT_PLAIN);

    // Assert
    assertThat(collation).isNull();
  }

  @Test
  @DisabledIf("isNotMysqlOrPostgresqlFamily")
  void createIndex_ColumnWithDefaultCollation_ShouldKeepCollation() throws Exception {
    // Arrange
    createTableOnDefaultCollation();
    String before = testUtils.getColumnCollation(NAMESPACE, TABLE, TEXT_PLAIN);

    // Act
    admin.createIndex(NAMESPACE, TABLE, TEXT_PLAIN, Collections.emptyMap());

    // Assert
    assertThat(testUtils.getColumnCollation(NAMESPACE, TABLE, TEXT_PLAIN)).isEqualTo(before);
  }

  @Test
  @DisabledIf("isDb2SqliteOrSpanner")
  void createIndex_ColumnWithNonBinaryCollation_ShouldKeepCollation() throws Exception {
    // Arrange
    createTableWithNonBinaryCollationOnPlainColumn();

    // Act
    admin.createIndex(NAMESPACE, TABLE, TEXT_PLAIN, Collections.emptyMap());

    // Assert
    assertThat(testUtils.getColumnCollation(NAMESPACE, TABLE, TEXT_PLAIN))
        .isEqualTo(nonBinaryCollation());
  }

  @Test
  @DisabledIf("isDb2SqliteOrSpanner")
  void dropIndex_ColumnWithNonBinaryCollation_ShouldKeepCollation() throws Exception {
    // Arrange
    createTableWithNonBinaryCollationOnPlainColumn();
    admin.createIndex(NAMESPACE, TABLE, TEXT_PLAIN, Collections.emptyMap());

    // Act
    admin.dropIndex(NAMESPACE, TABLE, TEXT_PLAIN);

    // Assert
    assertThat(testUtils.getColumnCollation(NAMESPACE, TABLE, TEXT_PLAIN))
        .isEqualTo(nonBinaryCollation());
  }

  @Test
  @DisabledIf("isDb2SqliteOrSpanner")
  void renameColumn_ColumnWithNonBinaryCollation_ShouldKeepCollation() throws Exception {
    // Arrange
    createTableWithNonBinaryCollationOnPlainColumn();

    // Act
    admin.renameColumn(NAMESPACE, TABLE, TEXT_PLAIN, "renamed");

    // Assert
    assertThat(testUtils.getColumnCollation(NAMESPACE, TABLE, "renamed"))
        .isEqualTo(nonBinaryCollation());
  }

  @Test
  void renameColumn_TidbPrimaryKeyWithNonBinaryCollation_ShouldKeepCollation() throws Exception {
    // Arrange
    assumeTrue(JdbcTestUtils.isTidb(rdbEngine));
    admin.createNamespace(NAMESPACE, true);
    testUtils.executeSql(
        "CREATE TABLE "
            + rdbEngine.encloseFullTableName(NAMESPACE, TABLE)
            + "(`pk` VARCHAR(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci PRIMARY KEY,"
            + "`c` INT)");
    admin.importTable(NAMESPACE, TABLE, Collections.emptyMap());

    // Act
    Throwable thrown =
        catchThrowable(() -> admin.renameColumn(NAMESPACE, TABLE, TEXT_PK, "renamed"));

    // Assert
    assertThat(thrown).isNull();
    assertThat(testUtils.getColumnCollation(NAMESPACE, TABLE, "renamed"))
        .isEqualTo("utf8mb4_general_ci");
  }

  private boolean isOracleWithStandardMaxStringSize() throws Exception {
    if (!JdbcTestUtils.isOracle(rdbEngine)) {
      return false;
    }
    createOracleSchemaIfMissing(NAMESPACE);
    String probe = rdbEngine.encloseFullTableName(NAMESPACE, "probe");
    try {
      testUtils.executeSql(
          "CREATE TABLE "
              + probe
              + "(\"c\" VARCHAR2(10) "
              + rdbEngine.getBinaryTextCollationClause()
              + ")");
      testUtils.executeSql("DROP TABLE " + probe);
      return false;
    } catch (SQLException e) {
      return e.getErrorCode() == 43929;
    }
  }

  private void createOracleSchemaIfMissing(String schema) throws Exception {
    if (!testUtils.namespaceExists(schema)) {
      for (String sql : rdbEngine.createSchemaSqls(schema)) {
        testUtils.executeSql(sql);
      }
    }
  }

  // Creates the system tables and one user table with DDL that has no column collation.
  private void createOracleTablesWithoutCollation() throws Exception {
    String metadataSchema = new JdbcConfig(new DatabaseConfig(properties)).getMetadataSchema();
    createOracleSchemaIfMissing(metadataSchema);
    createOracleSchemaIfMissing(NAMESPACE);
    testUtils.executeSql(
        "CREATE TABLE \""
            + metadataSchema
            + "\".\"metadata\"(\"full_table_name\" VARCHAR2(128),\"column_name\" VARCHAR2(128),"
            + "\"data_type\" VARCHAR2(20) NOT NULL,\"key_type\" VARCHAR2(20),"
            + "\"clustering_order\" VARCHAR2(10),\"indexed\" NUMBER(1) NOT NULL,"
            + "\"ordinal_position\" INTEGER NOT NULL,"
            + "PRIMARY KEY (\"full_table_name\", \"column_name\"))");
    testUtils.executeSql(
        "CREATE TABLE \""
            + metadataSchema
            + "\".\"namespaces\"(\"namespace_name\" VARCHAR2(128), PRIMARY KEY (\"namespace_name\"))");
    for (String namespace : Arrays.asList(metadataSchema, NAMESPACE)) {
      testUtils.executeSql(
          "INSERT INTO \"" + metadataSchema + "\".\"namespaces\" VALUES ('" + namespace + "')");
    }
    testUtils.executeSql(
        "CREATE TABLE \""
            + NAMESPACE
            + "\".\""
            + TABLE
            + "\"(\"pk\" VARCHAR2(128),\"ck\" VARCHAR2(128),\"idx\" VARCHAR2(128),"
            + "\"plain\" VARCHAR2(4000), PRIMARY KEY (\"pk\",\"ck\")) ROWDEPENDENCIES");
  }

  @Test
  @DisabledIf("isDb2OrSpanner")
  void createTable_OracleMaxStringSizeStandard_ShouldFailWithMaxStringSizeMessage()
      throws Exception {
    // Arrange
    assumeTrue(isOracleWithStandardMaxStringSize());
    createOracleTablesWithoutCollation();
    testUtils.executeSql("DROP TABLE \"" + NAMESPACE + "\".\"" + TABLE + "\"");

    // Act
    Throwable thrown =
        catchThrowable(() -> admin.createTable(NAMESPACE, TABLE, textTableMetadata(), true));

    // Assert
    assertThat(thrown)
        .isInstanceOf(ExecutionException.class)
        .hasStackTraceContaining(
            CoreError.JDBC_ORACLE_EXTENDED_MAX_STRING_SIZE_REQUIRED.buildMessage());
  }

  @Test
  @DisabledIf("isDb2OrSpanner")
  void repairTable_MaxStringSizeStandardAndTablesCreatedBeforeUpgrade_ShouldSucceed()
      throws Exception {
    // Arrange
    assumeTrue(isOracleWithStandardMaxStringSize());
    createOracleTablesWithoutCollation();

    // Act
    Throwable thrown =
        catchThrowable(
            () -> admin.repairTable(NAMESPACE, TABLE, textTableMetadata(), Collections.emptyMap()));

    // Assert
    assertThat(thrown).isNull();
  }

  @Test
  @DisabledIf("isDb2OrSpanner")
  void createIndex_MaxStringSizeStandardAndTableCreatedBeforeUpgrade_ShouldSucceed()
      throws Exception {
    // Arrange
    assumeTrue(isOracleWithStandardMaxStringSize());
    createOracleTablesWithoutCollation();
    admin.importTable(NAMESPACE, TABLE, Collections.emptyMap());

    // Act
    Throwable thrown =
        catchThrowable(
            () -> admin.createIndex(NAMESPACE, TABLE, TEXT_PLAIN, Collections.emptyMap()));

    // Assert
    assertThat(thrown).isNull();
  }

  @Test
  @EnabledIf("isOracleOrSqlServer")
  void createVirtualTable_SourceTableCreatedWithColumnCollationDisabled_ShouldThrowConflict()
      throws Exception {
    // Arrange
    createVirtualTableSources();
    try {
      // Act
      Throwable thrown = catchThrowable(this::createVirtualTableOnSources);

      // Assert
      assertThat(thrown)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining(
              CoreError.JDBC_VIRTUAL_TABLE_SOURCE_KEY_COLLATIONS_CONFLICT.buildCode());
      assertThat(admin.tableExists(NAMESPACE, VIRTUAL_TABLE)).isFalse();
    } finally {
      dropVirtualTableAndSources();
    }
  }

  @Test
  @DisabledIf("isNotMysqlOrPostgresqlFamily")
  void createVirtualTable_SourceTableCreatedWithColumnCollationDisabled_ShouldReadThroughView()
      throws Exception {
    // Arrange
    createVirtualTableSources();
    try (DistributedStorage storage = storageFactory.getStorage()) {
      storage.put(
          Put.newBuilder()
              .namespace(NAMESPACE)
              .table(OLD_SOURCE_TABLE)
              .partitionKey(Key.ofText(TEXT_PK, "k"))
              .intValue("a", 1)
              .build());
      storage.put(
          Put.newBuilder()
              .namespace(NAMESPACE)
              .table(NEW_SOURCE_TABLE)
              .partitionKey(Key.ofText(TEXT_PK, "k"))
              .intValue("b", 2)
              .build());

      // Act
      createVirtualTableOnSources();
      Optional<Result> result =
          storage.get(
              Get.newBuilder()
                  .namespace(NAMESPACE)
                  .table(VIRTUAL_TABLE)
                  .partitionKey(Key.ofText(TEXT_PK, "k"))
                  .build());

      // Assert
      assertThat(result).isPresent();
      assertThat(result.get().getInt("b")).isEqualTo(2);
    } finally {
      dropVirtualTableAndSources();
    }
  }

  private void createVirtualTableSources() throws Exception {
    createTableWithColumnCollationDisabled(
        OLD_SOURCE_TABLE,
        TableMetadata.newBuilder()
            .addColumn(TEXT_PK, DataType.TEXT)
            .addColumn("a", DataType.INT)
            .addPartitionKey(TEXT_PK)
            .build());
    admin.createTable(
        NAMESPACE,
        NEW_SOURCE_TABLE,
        TableMetadata.newBuilder()
            .addColumn(TEXT_PK, DataType.TEXT)
            .addColumn("b", DataType.INT)
            .addPartitionKey(TEXT_PK)
            .build(),
        true);
  }

  private void createVirtualTableOnSources() throws ExecutionException {
    admin.createVirtualTable(
        NAMESPACE,
        VIRTUAL_TABLE,
        NAMESPACE,
        OLD_SOURCE_TABLE,
        NAMESPACE,
        NEW_SOURCE_TABLE,
        VirtualTableJoinType.INNER);
  }

  private void dropVirtualTableAndSources() throws Exception {
    if (admin.tableExists(NAMESPACE, VIRTUAL_TABLE)) {
      admin.dropTable(NAMESPACE, VIRTUAL_TABLE);
    }
    admin.dropTable(NAMESPACE, OLD_SOURCE_TABLE, true);
    admin.dropTable(NAMESPACE, NEW_SOURCE_TABLE, true);
    String metadataSchema = new JdbcConfig(new DatabaseConfig(properties)).getMetadataSchema();
    if (testUtils.tableExists(metadataSchema, "virtual_tables")) {
      testUtils.dropTable(metadataSchema, "virtual_tables");
    }
  }
}
