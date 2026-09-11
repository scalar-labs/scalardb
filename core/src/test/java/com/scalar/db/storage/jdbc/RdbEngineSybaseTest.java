package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.storage.jdbc.query.QueryBuilder;
import com.scalar.db.storage.jdbc.query.SelectQuery;
import com.scalar.db.storage.jdbc.query.UpsertQuery;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.junit.jupiter.api.Test;

class RdbEngineSybaseTest {

  private static final String NAMESPACE = "n1";
  private static final String TABLE = "t1";
  private static final TableMetadata TABLE_METADATA =
      TableMetadata.newBuilder()
          .addColumn("p1", DataType.TEXT)
          .addColumn("p2", DataType.INT)
          .addColumn("c1", DataType.TEXT)
          .addColumn("c2", DataType.TEXT)
          .addColumn("v1", DataType.TEXT)
          .addColumn("v2", DataType.TEXT)
          .addPartitionKey("p1")
          .addPartitionKey("p2")
          .addClusteringKey("c1", Scan.Ordering.Order.ASC)
          .addClusteringKey("c2", Scan.Ordering.Order.DESC)
          .addSecondaryIndex("v1")
          .build();

  private final RdbEngineSybase rdbEngine = new RdbEngineSybase();
  private final QueryBuilder queryBuilder = new QueryBuilder(rdbEngine);

  @Test
  void encloseFullTableName_ShouldQualifyTheTableWithTheOwner() {
    // A namespace is an ASE object owner, as a schema is in SQL Server
    assertThat(rdbEngine.encloseFullTableName(NAMESPACE, TABLE)).isEqualTo("\"n1\".\"t1\"");
  }

  @Test
  void createSchemaSqls_ShouldOnlyCheckThatTheOwnerExists() {
    // ScalarDB does not create an ASE user, since that needs a server-level login
    assertThat(rdbEngine.createSchemaSqls(NAMESPACE))
        .hasSize(1)
        .allSatisfy(
            sql -> {
              assertThat(sql)
                  .startsWith("IF NOT EXISTS (SELECT 1 FROM sysusers WHERE name = 'n1')")
                  .contains("RAISERROR 20000")
                  // The message is a string literal, not an identifier: quoted identifiers are on
                  .contains("'DB-CORE-10304:")
                  .doesNotContain("CREATE")
                  .doesNotContain("sp_adduser");
            });
    // Checking is idempotent, so the "if not exists" form is the same statement
    assertThat(rdbEngine.createSchemaIfNotExistsSqls(NAMESPACE))
        .isEqualTo(rdbEngine.createSchemaSqls(NAMESPACE));
  }

  @Test
  void createSchemaSqls_GivenNameWithQuote_ShouldEscapeIt() {
    assertThat(rdbEngine.createSchemaSqls("n'1")[0])
        .startsWith("IF NOT EXISTS (SELECT 1 FROM sysusers WHERE name = 'n''1')");
  }

  @Test
  void dropNamespaceSqls_ShouldDoNothing() {
    // Dropping an ASE user is a database administrator's decision, not ScalarDB's
    assertThat(rdbEngine.dropNamespaceSql(NAMESPACE)).isNull();
    assertThat(rdbEngine.deleteMetadataSchemaSql("scalardb")).isNull();
  }

  @Test
  void createTableInternalPrimaryKeyClause_ShouldUseDatarowsLocking() {
    assertThat(rdbEngine.createTableInternalPrimaryKeyClause(false, TABLE_METADATA))
        .isEqualTo("PRIMARY KEY (\"p1\",\"p2\",\"c1\",\"c2\")) LOCK DATAROWS");

    assertThat(rdbEngine.createTableInternalPrimaryKeyClause(true, TABLE_METADATA))
        .isEqualTo("PRIMARY KEY (\"p1\" ASC,\"p2\" ASC,\"c1\" ASC,\"c2\" DESC)) LOCK DATAROWS");
  }

  @Test
  void columnTypes_ShouldSpellNullabilityOutSinceAseDefaultsToNotNull() {
    // A key column has to be NOT NULL, or ASE rejects the primary key
    assertThat(rdbEngine.getDataTypeForKey(DataType.TEXT)).isEqualTo("UNIVARCHAR(128) NOT NULL");
    assertThat(rdbEngine.getDataTypeForKey(DataType.INT)).isEqualTo("INT NOT NULL");
    // and never null, so that a key column cannot fall back to the nullable regular column type
    for (DataType dataType : DataType.values()) {
      assertThat(rdbEngine.getDataTypeForKey(dataType)).endsWith(" NOT NULL");
    }

    // Altering a BLOB to TEXT is refused: ASE cannot convert image to varchar
    assertThatThrownBy(
            () -> rdbEngine.throwIfAlterColumnTypeNotSupported(DataType.BLOB, DataType.TEXT))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatCode(() -> rdbEngine.throwIfAlterColumnTypeNotSupported(DataType.INT, DataType.TEXT))
        .doesNotThrowAnyException();

    // An indexed column stays nullable, and the caller appends the clause
    assertThat(rdbEngine.getDataTypeForSecondaryIndex(DataType.TEXT)).isEqualTo("UNIVARCHAR(128)");
    assertThat(rdbEngine.getDataTypeForSecondaryIndex(DataType.INT)).isNull();
    // An imported bit column cannot be indexed, so BOOLEAN widens to the tinyint this engine uses
    assertThat(rdbEngine.getDataTypeForSecondaryIndex(DataType.BOOLEAN)).isEqualTo("TINYINT");
    // A BLOB is an image column, which ASE can neither index nor convert back from
    assertThatThrownBy(() -> rdbEngine.getDataTypeForSecondaryIndex(DataType.BLOB))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThat(rdbEngine.getNullableColumnClause()).isEqualTo(" NULL");

    // A metadata table key column carries its own NOT NULL, a regular one does not
    assertThat(rdbEngine.getTextType(128, true)).isEqualTo("UNIVARCHAR(128) NOT NULL");
    assertThat(rdbEngine.getTextType(20, false)).isEqualTo("UNIVARCHAR(20)");
  }

  @Test
  void getDataTypeForEngine_ShouldAvoidTypesThatCannotHoldScalarDbValues() {
    // Not "bit", which cannot be null
    assertThat(rdbEngine.getDataTypeForEngine(DataType.BOOLEAN)).isEqualTo("TINYINT");
    assertThat(rdbEngine.getDataTypeForEngine(DataType.INT)).isEqualTo("INT");
    assertThat(rdbEngine.getDataTypeForEngine(DataType.BIGINT)).isEqualTo("BIGINT");
    assertThat(rdbEngine.getDataTypeForEngine(DataType.FLOAT)).isEqualTo("REAL");
    assertThat(rdbEngine.getDataTypeForEngine(DataType.DOUBLE)).isEqualTo("DOUBLE PRECISION");
    // univarchar, not varchar: a varchar cannot hold non-Latin text on an iso_1 server
    assertThat(rdbEngine.getDataTypeForEngine(DataType.TEXT)).isEqualTo("UNIVARCHAR(900)");
    // Not a sized VARBINARY: ASE truncates silently, which corrupted the coordinator write set
    assertThat(rdbEngine.getDataTypeForEngine(DataType.BLOB)).isEqualTo("IMAGE");
    assertThat(rdbEngine.getDataTypeForEngine(DataType.DATE)).isEqualTo("DATE");
    // Not "time" and "datetime", whose resolutions are coarser than what ScalarDB stores
    assertThat(rdbEngine.getDataTypeForEngine(DataType.TIME)).isEqualTo("BIGTIME");
    assertThat(rdbEngine.getDataTypeForEngine(DataType.TIMESTAMP)).isEqualTo("BIGDATETIME");
    assertThat(rdbEngine.getDataTypeForEngine(DataType.TIMESTAMPTZ)).isEqualTo("BIGDATETIME");
  }

  @Test
  void getDataTypeForScalarDb_ShouldMapAseTypesForImport() {
    assertThat(importedType(JDBCType.BIT, "bit")).isEqualTo(DataType.BOOLEAN);
    assertThat(importedType(JDBCType.TINYINT, "tinyint")).isEqualTo(DataType.INT);
    assertThat(importedType(JDBCType.SMALLINT, "smallint")).isEqualTo(DataType.INT);
    assertThat(importedType(JDBCType.INTEGER, "int")).isEqualTo(DataType.INT);
    assertThat(importedType(JDBCType.INTEGER, "unsigned int")).isEqualTo(DataType.BIGINT);
    assertThat(importedType(JDBCType.BIGINT, "bigint")).isEqualTo(DataType.BIGINT);
    assertThat(importedType(JDBCType.REAL, "real")).isEqualTo(DataType.FLOAT);
    assertThat(importedType(JDBCType.DOUBLE, "double precision")).isEqualTo(DataType.DOUBLE);
    assertThat(importedType(JDBCType.VARCHAR, "varchar")).isEqualTo(DataType.TEXT);
    assertThat(importedType(JDBCType.LONGVARCHAR, "text")).isEqualTo(DataType.TEXT);
    assertThat(importedType(JDBCType.VARBINARY, "varbinary")).isEqualTo(DataType.BLOB);
    assertThat(importedType(JDBCType.LONGVARBINARY, "image")).isEqualTo(DataType.BLOB);
    assertThat(importedType(JDBCType.DATE, "date")).isEqualTo(DataType.DATE);
    assertThat(importedType(JDBCType.TIME, "bigtime")).isEqualTo(DataType.TIME);
    assertThat(importedType(JDBCType.TIMESTAMP, "datetime")).isEqualTo(DataType.TIMESTAMP);
    assertThat(importedType(JDBCType.TIMESTAMP, "smalldatetime")).isEqualTo(DataType.TIMESTAMP);
    // jConnect reports bigdatetime as 11 and bigtime as 10, which java.sql.Types does not define,
    // so JdbcUtils#getJdbcType maps them to OTHER
    assertThat(importedType(JDBCType.OTHER, "bigdatetime")).isEqualTo(DataType.TIMESTAMP);
    assertThat(importedType(JDBCType.OTHER, "bigtime")).isEqualTo(DataType.TIME);
    assertThat(importedType(JDBCType.CHAR, "unichar")).isEqualTo(DataType.TEXT);
    assertThat(importedType(JDBCType.VARCHAR, "univarchar")).isEqualTo(DataType.TEXT);
  }

  @Test
  void getDataTypeForScalarDb_ShouldRejectTypesWithNoScalarDbCounterpart() {
    // The ASE type named timestamp is a row version, not a point in time
    assertThatThrownBy(() -> importedType(JDBCType.BINARY, "timestamp"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("timestamp");
    // An unsigned bigint does not fit in the signed ScalarDB BIGINT
    assertThatThrownBy(() -> importedType(JDBCType.BIGINT, "unsigned bigint"))
        .isInstanceOf(IllegalArgumentException.class);
    // No ScalarDB type holds an exact decimal, so a scaled column needs the caller to ask for the
    // lossy DOUBLE rather than have it chosen for them. money and smallmoney report a scale of 4,
    // so they land here too.
    assertThatThrownBy(() -> importedType(JDBCType.DECIMAL, "decimal", 10, 3, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("DB-CORE-10307");
    assertThatThrownBy(() -> importedType(JDBCType.DECIMAL, "money", 19, 4, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("DB-CORE-10307");
    // More digits than a BIGINT holds, so even without a scale it would not be exact
    assertThatThrownBy(() -> importedType(JDBCType.DECIMAL, "decimal", 20, 0, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("DB-CORE-10308");
    assertThatThrownBy(() -> importedType(JDBCType.OTHER, "sysname"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void getDataTypeForScalarDb_GivenTimestampTZOverride_ShouldImportTheColumnAsUtc() {
    // ASE has no time zone aware type, so a column already holding UTC can be read as one
    assertThat(
            rdbEngine.getDataTypeForScalarDb(
                JDBCType.TIMESTAMP, "bigdatetime", 0, 0, "n1.t1 c", DataType.TIMESTAMPTZ))
        .isEqualTo(DataType.TIMESTAMPTZ);
  }

  @Test
  void upsertQuery_WithColumnsToUpdate_ShouldUpdateThenInsert() throws SQLException {
    // Arrange
    Map<String, Column<?>> columns = new LinkedHashMap<>();
    columns.put("v1", TextColumn.of("v1", "v1Value"));
    columns.put("v2", TextColumn.of("v2", "v2Value"));
    PreparedStatement preparedStatement = mock(PreparedStatement.class);

    // Act
    UpsertQuery query =
        queryBuilder
            .upsertInto(NAMESPACE, TABLE, TABLE_METADATA)
            .values(Key.ofText("p1", "p1Value"), Optional.of(Key.ofText("c1", "c1Value")), columns)
            .build();

    // Assert
    // Not MERGE: ASE compiles the WHEN NOT MATCHED THEN INSERT branch even for a row that is there
    // and rejects the statement if that insert would leave a NOT NULL column unset, which makes
    // MERGE unusable on a table ScalarDB did not create. See UpdateThenInsertQuery.
    assertThat(query.sql())
        .isEqualTo(
            "UPDATE \"n1\".\"t1\" SET \"v1\"=?,\"v2\"=? WHERE \"p1\"=? AND \"c1\"=?\n"
                + "IF @@rowcount = 0\n"
                + "INSERT INTO \"n1\".\"t1\" (\"p1\",\"c1\",\"v1\",\"v2\") VALUES (?,?,?,?)");

    query.bind(preparedStatement);
    // The UPDATE statement
    verify(preparedStatement).setString(1, "v1Value");
    verify(preparedStatement).setString(2, "v2Value");
    verify(preparedStatement).setString(3, "p1Value");
    verify(preparedStatement).setString(4, "c1Value");
    // The INSERT statement
    verify(preparedStatement).setString(5, "p1Value");
    verify(preparedStatement).setString(6, "c1Value");
    verify(preparedStatement).setString(7, "v1Value");
    verify(preparedStatement).setString(8, "v2Value");
  }

  @Test
  void upsertQuery_WithoutColumnsToUpdate_ShouldProbeThenInsert() throws SQLException {
    // Arrange
    PreparedStatement preparedStatement = mock(PreparedStatement.class);

    // Act
    UpsertQuery query =
        queryBuilder
            .upsertInto(NAMESPACE, TABLE, TABLE_METADATA)
            .values(
                Key.ofText("p1", "p1Value"),
                Optional.of(Key.ofText("c1", "c1Value")),
                Collections.emptyMap())
            .build();

    // Assert
    // With nothing to update, a no op UPDATE stands in so that @@rowcount still says whether the
    // row is there without producing a result set
    assertThat(query.sql())
        .isEqualTo(
            "UPDATE \"n1\".\"t1\" SET \"p1\"=\"p1\" WHERE \"p1\"=? AND \"c1\"=?\n"
                + "IF @@rowcount = 0\n"
                + "INSERT INTO \"n1\".\"t1\" (\"p1\",\"c1\") VALUES (?,?)");

    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "p1Value");
    verify(preparedStatement).setString(2, "c1Value");
    verify(preparedStatement).setString(3, "p1Value");
    verify(preparedStatement).setString(4, "c1Value");
  }

  @Test
  void selectQueryWithLimit_ShouldUseTop() {
    SelectQuery query =
        queryBuilder
            .select(Collections.emptyList())
            .from(NAMESPACE, TABLE, TABLE_METADATA)
            .where(Key.ofText("p1", "p1Value"), Optional.empty(), Collections.emptySet())
            .limit(10)
            .build();

    assertThat(query.sql()).isEqualTo("SELECT TOP 10 * FROM \"n1\".\"t1\" WHERE \"p1\"=?");
  }

  @Test
  void alterTableSqls_ShouldUseTheAseSpelling() {
    // ASE has no COLUMN keyword here
    assertThat(rdbEngine.dropColumnSql(NAMESPACE, TABLE, "v1"))
        .containsExactly("ALTER TABLE \"n1\".\"t1\" DROP \"v1\"");
    // ASE spells this MODIFY rather than ALTER COLUMN
    assertThat(rdbEngine.alterColumnTypeSql(NAMESPACE, TABLE, "v1", "UNIVARCHAR(128) NULL"))
        .containsExactly("ALTER TABLE \"n1\".\"t1\" MODIFY \"v1\" UNIVARCHAR(128) NULL");
    // The index name is not enclosed: ASE keeps the quotes in the name, and an index named
    // "idx" with the quotes included can then be neither dropped nor renamed
    assertThat(rdbEngine.createIndexSql(NAMESPACE, TABLE, "index_n1_t1_v1", "v1"))
        .isEqualTo("CREATE INDEX index_n1_t1_v1 ON \"n1\".\"t1\" (\"v1\")");
    // ASE names the table and the index together rather than with an ON clause
    // Owner-qualifying the table here gets "DROP INDEX does not allow specifying the database name
    // as a prefix", so the drop runs as the owner
    assertThat(rdbEngine.dropIndexSql(NAMESPACE, TABLE, "index_n1_t1_v1"))
        .isEqualTo("setuser 'n1'\nDROP INDEX \"t1\".index_n1_t1_v1\nsetuser");
    // sp_rename rejects both an owner-qualified name and quoted identifiers, and reports either as
    // a message rather than an error, so the rename runs as the owner with bare names
    assertThat(rdbEngine.renameTableSql(NAMESPACE, TABLE, "t2"))
        .isEqualTo("setuser 'n1'\nexec sp_rename 't1', 't2'\nsetuser");
    assertThat(rdbEngine.renameColumnSql(NAMESPACE, TABLE, "v1", "v9", "UNIVARCHAR(900)"))
        .isEqualTo("setuser 'n1'\nexec sp_rename 't1.v1', 'v9', 'column'\nsetuser");
    // The guard comes first because sp_rename reports an unresolvable name as a message with a
    // non-zero return status, not as an error, and the caller's fallback to the pre-shortening long
    // index name only runs when the rename throws
    assertThat(rdbEngine.renameIndexSqls(NAMESPACE, TABLE, "v1", "old_index", "new_index"))
        .containsExactly(
            "IF NOT EXISTS (SELECT 1 FROM sysindexes WHERE id = object_id('n1.t1')"
                + " AND name = 'old_index') RAISERROR 20001 'The index old_index does not exist'",
            "setuser 'n1'\nexec sp_rename 't1.old_index', 'new_index', 'index'\nsetuser");
  }

  @Test
  void catalogQueries_ShouldMatchOnTheOwner() throws SQLException {
    PreparedStatement preparedStatement = mock(PreparedStatement.class);

    assertThat(rdbEngine.internalTableExistsCheckSql())
        .isEqualTo(
            "SELECT 1 FROM sysobjects o JOIN sysusers u ON o.uid = u.uid"
                + " WHERE u.name = ? AND o.name = ? AND o.type = 'U'");
    // The default binding applies: the owner first, then the table
    rdbEngine.bindInternalTableExistsCheckParams(preparedStatement, NAMESPACE, TABLE);
    verify(preparedStatement).setString(1, "n1");
    verify(preparedStatement).setString(2, "t1");

    assertThat(rdbEngine.getTableNamesInNamespaceSql())
        .isEqualTo(
            "SELECT o.name FROM sysobjects o JOIN sysusers u ON o.uid = u.uid"
                + " WHERE u.name = ? AND o.type = 'U'");
  }

  @Test
  void errorClassification_ShouldMapAseErrorCodes() {
    assertThat(rdbEngine.isDuplicateTableError(sqlException(2714))).isTrue();
    assertThat(rdbEngine.isDuplicateKeyError(sqlException(2601))).isTrue();
    assertThat(rdbEngine.isDuplicateKeyError(sqlException(2615))).isTrue();
    assertThat(rdbEngine.isDuplicateKeyError(sqlException(2627))).isTrue();
    assertThat(rdbEngine.isDuplicateKeyError(sqlException(1205))).isFalse();
    // A deadlock must be reported as a conflict so that the caller retries it
    assertThat(rdbEngine.isConflict(sqlException(1205))).isTrue();
    // So must a lock wait timeout, which only happens because the engine sets SET LOCK WAIT
    assertThat(rdbEngine.isConflict(sqlException(12205))).isTrue();
    // A duplicate key is a conflict: the upsert only reaches its INSERT when the UPDATE matched
    // nothing, so a duplicate there means another transaction got the key in between
    assertThat(rdbEngine.isConflict(sqlException(2601))).isTrue();
    assertThat(rdbEngine.isConflict(sqlException(2615))).isTrue();
    assertThat(rdbEngine.isConflict(sqlException(2627))).isTrue();
    assertThat(rdbEngine.isConflict(sqlException(2714))).isFalse();
    assertThat(rdbEngine.isUndefinedIndexError(sqlException(2727))).isTrue();
    assertThat(rdbEngine.isDuplicateIndexError(sqlException(1913))).isTrue();
    // An owner is never created, so it can never be a duplicate
    assertThat(rdbEngine.isDuplicateSchemaError(sqlException(2714))).isFalse();
  }

  @Test
  void connectionSettings_ShouldEnableQuotedIdentifiersAndLanguageBatches() {
    // enclose() emits double quoted identifiers, which ASE only accepts with this setting
    assertThat(rdbEngine.getConnectionInitSql())
        .isEqualTo("SET QUOTED_IDENTIFIER ON\nSET STRING_RTRUNCATION ON\nSET LOCK WAIT 30");
    // The upsert holds two statements, which ASE can only run as a language batch
    assertThat(rdbEngine.getConnectionProperties(mock(JdbcConfig.class)))
        .containsEntry("DYNAMIC_PREPARE", "false");
  }

  @Test
  void getConnectionInitSql_ShouldBoundTheLockWait() {
    // ASE waits for a lock forever by default, which turns contention into a hang that Consensus
    // Commit cannot retry
    JdbcConfig config = mock(JdbcConfig.class);
    when(config.getSybaseLockWaitSeconds()).thenReturn(45);
    assertThat(new RdbEngineSybase(config).getConnectionInitSql())
        .isEqualTo("SET QUOTED_IDENTIFIER ON\nSET STRING_RTRUNCATION ON\nSET LOCK WAIT 45");

    // A negative value leaves the server's own behavior alone
    when(config.getSybaseLockWaitSeconds()).thenReturn(-1);
    assertThat(new RdbEngineSybase(config).getConnectionInitSql())
        .isEqualTo("SET QUOTED_IDENTIFIER ON\nSET STRING_RTRUNCATION ON");
  }

  @Test
  void getDataTypeForScalarDb_ShouldMapDecimalsWithoutAScaleExactly() {
    // No scale means the value is an integer, so it maps exactly as long as it fits
    assertThat(importedType(JDBCType.NUMERIC, "numeric", 5, 0, null)).isEqualTo(DataType.INT);
    assertThat(importedType(JDBCType.NUMERIC, "numeric", 18, 0, null)).isEqualTo(DataType.BIGINT);
    // With a scale, an explicit DOUBLE override accepts the precision loss
    assertThat(importedType(JDBCType.NUMERIC, "numeric", 12, 2, DataType.DOUBLE))
        .isEqualTo(DataType.DOUBLE);
    assertThat(importedType(JDBCType.DECIMAL, "money", 19, 4, DataType.DOUBLE))
        .isEqualTo(DataType.DOUBLE);
  }

  private DataType importedType(JDBCType type, String typeName) {
    return importedType(type, typeName, 0, 0, null);
  }

  private DataType importedType(
      JDBCType type, String typeName, int columnSize, int digits, @Nullable DataType override) {
    return rdbEngine.getDataTypeForScalarDb(
        type, typeName, columnSize, digits, "n1.t1 column", override);
  }

  private static SQLException sqlException(int errorCode) {
    return new SQLException("error", "S1000", errorCode);
  }
}
