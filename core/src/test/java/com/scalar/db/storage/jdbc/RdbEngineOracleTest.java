package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.ScanAll;
import com.scalar.db.api.Selection.Conjunction;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.CoreError;
import com.scalar.db.io.DataType;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class RdbEngineOracleTest {
  @Mock private ScanAll scanAll;
  @Mock private TableMetadata metadata;
  private RdbEngineOracle rdbEngineOracle;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    rdbEngineOracle = new RdbEngineOracle();
  }

  @Test
  void createTableInternalSqlsAfterCreateTable_GivenSameClusteringOrders_ShouldNotCreateIndex() {
    // Arrange
    RdbEngineStrategy rdbEngine = new RdbEngineOracle();
    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk")
            .addClusteringKey("ck1", Order.ASC)
            .addClusteringKey("ck2", Order.DESC)
            .addColumn("pk", DataType.INT)
            .addColumn("ck1", DataType.INT)
            .addColumn("ck2", DataType.INT)
            .build();

    // Act
    String[] sqls =
        rdbEngine.createTableInternalSqlsAfterCreateTable(
            false, "myschema", "mytable", metadata, false);

    // Assert
    assertThat(sqls).hasSize(1);
    assertThat(sqls[0]).startsWith("ALTER TABLE ");
  }

  @Test
  void createTableInternalSqlsAfterCreateTable_GivenDifferentClusteringOrders_ShouldCreateIndex() {
    // Arrange
    RdbEngineStrategy rdbEngine = new RdbEngineOracle();
    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk")
            .addClusteringKey("ck1", Order.ASC)
            .addClusteringKey("ck2", Order.DESC)
            .addColumn("pk", DataType.INT)
            .addColumn("ck1", DataType.INT)
            .addColumn("ck2", DataType.INT)
            .build();

    // Act
    String[] sqls =
        rdbEngine.createTableInternalSqlsAfterCreateTable(
            true, "myschema", "mytable", metadata, false);

    // Assert
    assertThat(sqls).hasSize(2);
    assertThat(sqls[0]).startsWith("ALTER TABLE ");
    assertThat(sqls[1]).startsWith("CREATE UNIQUE INDEX ");
  }

  @Test
  public void
      throwIfCrossPartitionScanOrderingOnBlobColumnNotSupported_WithBlobOrdering_ShouldThrowException() {
    // Arrange
    Scan.Ordering blobOrdering = Scan.Ordering.asc("blob_column");
    Scan.Ordering intOrdering = Scan.Ordering.desc("int_column");

    when(scanAll.getOrderings()).thenReturn(Arrays.asList(intOrdering, blobOrdering));
    when(metadata.getColumnDataType("blob_column")).thenReturn(DataType.BLOB);
    when(metadata.getColumnDataType("int_column")).thenReturn(DataType.INT);

    // Act & Assert
    assertThatThrownBy(
            () ->
                rdbEngineOracle.throwIfCrossPartitionScanOrderingOnBlobColumnNotSupported(
                    scanAll, metadata))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("blob_column");
  }

  @Test
  public void
      throwIfCrossPartitionScanOrderingOnBlobColumnNotSupported_WithoutBlobOrdering_ShouldNotThrowException() {
    // Arrange
    Scan.Ordering intOrdering = Scan.Ordering.asc("int_column");
    Scan.Ordering textOrdering = Scan.Ordering.desc("text_column");

    when(scanAll.getOrderings()).thenReturn(Arrays.asList(intOrdering, textOrdering));
    when(metadata.getColumnDataType("int_column")).thenReturn(DataType.INT);
    when(metadata.getColumnDataType("text_column")).thenReturn(DataType.TEXT);

    // Act & Assert
    assertThatCode(
            () ->
                rdbEngineOracle.throwIfCrossPartitionScanOrderingOnBlobColumnNotSupported(
                    scanAll, metadata))
        .doesNotThrowAnyException();
  }

  @Test
  public void
      throwIfCrossPartitionScanOrderingOnBlobColumnNotSupported_WithNoOrderings_ShouldNotThrowException() {
    // Arrange
    when(scanAll.getOrderings()).thenReturn(Arrays.asList());

    // Act & Assert
    assertThatCode(
            () ->
                rdbEngineOracle.throwIfCrossPartitionScanOrderingOnBlobColumnNotSupported(
                    scanAll, metadata))
        .doesNotThrowAnyException();
  }

  @Test
  public void
      throwIfCrossPartitionScanOrderingOnBlobColumnNotSupported_WithMultipleBlobOrderings_ShouldThrowForFirst() {
    // Arrange
    Scan.Ordering blobOrdering1 = Scan.Ordering.asc("blob_column1");
    Scan.Ordering blobOrdering2 = Scan.Ordering.desc("blob_column2");

    when(scanAll.getOrderings()).thenReturn(Arrays.asList(blobOrdering1, blobOrdering2));
    when(metadata.getColumnDataType("blob_column1")).thenReturn(DataType.BLOB);
    when(metadata.getColumnDataType("blob_column2")).thenReturn(DataType.BLOB);

    // Act & Assert
    assertThatThrownBy(
            () ->
                rdbEngineOracle.throwIfCrossPartitionScanOrderingOnBlobColumnNotSupported(
                    scanAll, metadata))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("blob_column1");
  }

  @Test
  public void throwIfConjunctionsColumnNotSupported_WithoutBlobCondition_ShouldNotThrowException() {
    // Arrange
    TableMetadata metadata = mock(TableMetadata.class);
    java.util.Set<Conjunction> conjunctions =
        java.util.Collections.singleton(
            Conjunction.of(
                ConditionBuilder.column("int_column").isEqualToInt(10),
                ConditionBuilder.column("text_column").isEqualToText("value")));

    when(metadata.getColumnDataType("int_column")).thenReturn(DataType.INT);
    when(metadata.getColumnDataType("text_column")).thenReturn(DataType.TEXT);

    // Act & Assert
    assertThatCode(
            () -> rdbEngineOracle.throwIfConjunctionsColumnNotSupported(conjunctions, metadata))
        .doesNotThrowAnyException();
  }

  @Test
  public void throwIfConjunctionsColumnNotSupported_WithNoConditions_ShouldNotThrowException() {
    // Arrange
    TableMetadata metadata = mock(TableMetadata.class);

    // Act & Assert
    assertThatCode(
            () ->
                rdbEngineOracle.throwIfConjunctionsColumnNotSupported(
                    java.util.Collections.emptySet(), metadata))
        .doesNotThrowAnyException();
  }

  @Test
  public void
      throwIfConjunctionsColumnNotSupported_WithMixedConditions_ShouldThrowWhenBlobPresent() {
    // Arrange
    TableMetadata metadata = mock(TableMetadata.class);
    java.util.Set<Conjunction> conjunctions =
        java.util.Collections.singleton(
            Conjunction.of(
                ConditionBuilder.column("int_column").isGreaterThanInt(100),
                ConditionBuilder.column("blob_column").isNotEqualToBlob(new byte[] {5, 6}),
                ConditionBuilder.column("text_column").isEqualToText("test")));

    when(metadata.getColumnDataType("int_column")).thenReturn(DataType.INT);
    when(metadata.getColumnDataType("blob_column")).thenReturn(DataType.BLOB);
    when(metadata.getColumnDataType("text_column")).thenReturn(DataType.TEXT);

    // Act & Assert
    assertThatThrownBy(
            () -> rdbEngineOracle.throwIfConjunctionsColumnNotSupported(conjunctions, metadata))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("blob_column");
  }

  @Test
  public void isConflict_WithSerializeAccessError_ShouldReturnTrue() {
    // Arrange
    // ORA-08177: can't serialize access for this transaction
    SQLException e = new SQLException("can't serialize access for this transaction", null, 8177);

    // Act
    boolean result = rdbEngineOracle.isConflict(e);

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void isConflict_WithDeadlockError_ShouldReturnTrue() {
    // Arrange
    // ORA-00060: deadlock detected while waiting for resource
    SQLException e = new SQLException("deadlock detected while waiting for resource", null, 60);

    // Act
    boolean result = rdbEngineOracle.isConflict(e);

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void isConflict_WithConsistentReadFailureError_ShouldReturnTrue() {
    // Arrange
    // ORA-08176: consistent read failure; rollback data not available
    SQLException e =
        new SQLException("consistent read failure; rollback data not available", null, 8176);

    // Act
    boolean result = rdbEngineOracle.isConflict(e);

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void isConflict_WithOtherError_ShouldReturnFalse() {
    // Arrange
    // ORA-00942: Table or view does not exist
    SQLException e = new SQLException("Table or view does not exist", null, 942);

    // Act
    boolean result = rdbEngineOracle.isConflict(e);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  void getBinaryTextCollationClause_ShouldReturnCollateBinary() {
    // Arrange
    RdbEngineStrategy strategy = rdbEngineOracle;

    // Act
    String clause = strategy.getBinaryTextCollationClause();

    // Assert
    assertThat(clause).isEqualTo("COLLATE BINARY");
  }

  @Test
  public void isCollationClauseRejectedError_Ora43929_ShouldReturnTrue() {
    // Arrange
    SQLException e = new SQLException("ORA-43929", "99999", 43929);

    // Act
    boolean actual = rdbEngineOracle.isCollationClauseRejectedError(e);

    // Assert
    assertThat(actual).isTrue();
  }

  @Test
  public void isCollationClauseRejectedError_OtherCode_ShouldReturnFalse() {
    // Arrange
    SQLException e = new SQLException("ORA-00942", "42000", 942);

    // Act
    boolean actual = rdbEngineOracle.isCollationClauseRejectedError(e);

    // Assert
    assertThat(actual).isFalse();
  }

  @Test
  public void classify_WesternEuropean_ShouldReturnPartialCharacterSet() {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation("BINARY", "WE8MSWIN1252");

    // Act
    TextColumnCollationKind actual = rdbEngineOracle.classify(collation);

    // Assert
    assertThat(actual).isEqualTo(TextColumnCollationKind.PARTIAL_CHARACTER_SET);
  }

  @Test
  public void classify_Binary_ShouldReturnCodePointOrder() {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation("BINARY", "AL32UTF8");

    // Act
    TextColumnCollationKind actual = rdbEngineOracle.classify(collation);

    // Assert
    assertThat(actual).isEqualTo(TextColumnCollationKind.CODE_POINT_ORDER);
  }

  @Test
  public void classify_BinaryCi_ShouldReturnOtherOrder() {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation("BINARY_CI", "AL32UTF8");

    // Act
    TextColumnCollationKind actual = rdbEngineOracle.classify(collation);

    // Assert
    assertThat(actual).isEqualTo(TextColumnCollationKind.OTHER_ORDER);
  }

  @Test
  public void classify_UsingNlsCompWithBinarySession_ShouldReturnCodePointOrder() {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation("USING_NLS_COMP", "AL32UTF8", "BINARY");

    // Act
    TextColumnCollationKind actual = rdbEngineOracle.classify(collation);

    // Assert
    assertThat(actual).isEqualTo(TextColumnCollationKind.CODE_POINT_ORDER);
  }

  @Test
  public void classify_UsingNlsCompWithLinguisticSession_ShouldReturnOtherOrder() {
    // Arrange
    TextColumnCollation collation =
        new TextColumnCollation("USING_NLS_COMP", "AL32UTF8", "LINGUISTIC");

    // Act
    TextColumnCollationKind actual = rdbEngineOracle.classify(collation);

    // Assert
    assertThat(actual).isEqualTo(TextColumnCollationKind.OTHER_ORDER);
  }

  @Test
  public void classify_BinaryOnCesu8Charset_ShouldReturnOtherOrder() {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation("BINARY", "UTF8");

    // Act
    TextColumnCollationKind actual = rdbEngineOracle.classify(collation);

    // Assert
    assertThat(actual).isEqualTo(TextColumnCollationKind.OTHER_ORDER);
  }

  @Test
  public void classify_BinaryOnAl16utf16Charset_ShouldReturnOtherOrder() {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation("BINARY", "AL16UTF16");

    // Act
    TextColumnCollationKind actual = rdbEngineOracle.classify(collation);

    // Assert
    assertThat(actual).isEqualTo(TextColumnCollationKind.OTHER_ORDER);
  }

  @Test
  public void classify_UsingNlsCompWithBinarySessionOnCesu8Charset_ShouldReturnOtherOrder() {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation("USING_NLS_COMP", "UTF8", "BINARY");

    // Act
    TextColumnCollationKind actual = rdbEngineOracle.classify(collation);

    // Assert
    assertThat(actual).isEqualTo(TextColumnCollationKind.OTHER_ORDER);
  }

  @Test
  public void getCollationClause_ShouldReturnCollate() {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation("BINARY_CI", "AL32UTF8");

    // Act
    String clause = rdbEngineOracle.getCollationClause(collation);

    // Assert
    assertThat(clause).isEqualTo("COLLATE BINARY_CI");
  }

  @Test
  public void throwIfCollationClausesRejected_MaxStringSizeStandard_ShouldThrowMaxStringSizeError()
      throws SQLException {
    // Arrange
    Connection connection = mockMaxStringSize("STANDARD");

    // Act
    Throwable thrown =
        catchThrowable(() -> rdbEngineOracle.throwIfCollationClausesRejected(connection));

    // Assert
    assertThat(thrown)
        .isInstanceOf(SQLException.class)
        .hasMessage(CoreError.JDBC_ORACLE_EXTENDED_MAX_STRING_SIZE_REQUIRED.buildMessage());
  }

  @Test
  public void throwIfCollationClausesRejected_MaxStringSizeExtended_ShouldNotThrow()
      throws SQLException {
    // Arrange
    Connection connection = mockMaxStringSize("EXTENDED");

    // Act Assert
    assertThatCode(() -> rdbEngineOracle.throwIfCollationClausesRejected(connection))
        .doesNotThrowAnyException();
  }

  private Connection mockMaxStringSize(String maxStringSize) throws SQLException {
    Connection connection = mock(Connection.class);
    PreparedStatement ps = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);
    when(connection.prepareStatement(
            "SELECT PROPERTY_VALUE FROM DATABASE_PROPERTIES WHERE PROPERTY_NAME = 'MAX_STRING_SIZE'"))
        .thenReturn(ps);
    when(ps.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(true);
    when(rs.getString(1)).thenReturn(maxStringSize);
    return connection;
  }

  @Test
  public void throwIfCollationClauseRejectedError_Ora43929_ShouldThrowMaxStringSizeError() {
    // Arrange
    SQLException e = new SQLException("ORA-43929", "99999", 43929);

    // Act
    Throwable thrown = catchThrowable(() -> rdbEngineOracle.throwIfCollationClauseRejectedError(e));

    // Assert
    assertThat(thrown)
        .isInstanceOf(SQLException.class)
        .hasMessage(CoreError.JDBC_ORACLE_EXTENDED_MAX_STRING_SIZE_REQUIRED.buildMessage())
        .hasCause(e);
    assertThat(((SQLException) thrown).getErrorCode()).isEqualTo(43929);
  }

  @Test
  public void throwIfCollationClauseRejectedError_OtherCode_ShouldNotThrow() {
    // Arrange
    SQLException e = new SQLException("ORA-00942", "42000", 942);

    // Act Assert
    assertThatCode(() -> rdbEngineOracle.throwIfCollationClauseRejectedError(e))
        .doesNotThrowAnyException();
  }

  @Test
  public void getSchemaDefaultCollation_ShouldQueryAllUsersWithNamespaceAsUsername()
      throws SQLException {
    // Arrange
    Connection connection = mock(Connection.class);
    PreparedStatement ps = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);
    when(connection.prepareStatement("SELECT DEFAULT_COLLATION FROM ALL_USERS WHERE USERNAME = ?"))
        .thenReturn(ps);
    when(ps.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(true);
    when(rs.getString(1)).thenReturn("BINARY_CI");

    // Act
    String actual = rdbEngineOracle.getSchemaDefaultCollation(connection, "MyNs");

    // Assert
    assertThat(actual).isEqualTo("BINARY_CI");
    verify(ps).setString(1, "MyNs");
  }

  @Test
  public void getSchemaDefaultCollation_WithNoRow_ShouldReturnNull() throws SQLException {
    // Arrange
    Connection connection = mock(Connection.class);
    PreparedStatement ps = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);
    when(connection.prepareStatement(org.mockito.ArgumentMatchers.anyString())).thenReturn(ps);
    when(ps.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(false);

    // Act
    String actual = rdbEngineOracle.getSchemaDefaultCollation(connection, "ns");

    // Assert
    assertThat(actual).isNull();
  }

  @Test
  public void getSchemaDefaultCollation_WithNullDefaultCollation_ShouldReturnUsingNlsComp()
      throws SQLException {
    // Arrange
    Connection connection = mock(Connection.class);
    PreparedStatement ps = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);
    when(connection.prepareStatement(org.mockito.ArgumentMatchers.anyString())).thenReturn(ps);
    when(ps.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(true);
    when(rs.getString(1)).thenReturn(null);

    // Act
    String actual = rdbEngineOracle.getSchemaDefaultCollation(connection, "ns");

    // Assert
    assertThat(actual).isEqualTo("USING_NLS_COMP");
  }

  private static Connection connectionWithCharacterSet(String charset) throws SQLException {
    Connection connection = mock(Connection.class);
    PreparedStatement ps = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);
    when(connection.prepareStatement(
            "SELECT VALUE FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER = ?"))
        .thenReturn(ps);
    when(ps.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(true);
    when(rs.getString(1)).thenReturn(charset);
    return connection;
  }

  @Test
  public void getDatabaseTextIssues_Al32utf8_ShouldReturnEmpty() throws SQLException {
    // Arrange
    Connection connection = connectionWithCharacterSet("AL32UTF8");

    // Act
    List<DatabaseTextIssue> actual = rdbEngineOracle.getDatabaseTextIssues(connection);

    // Assert
    assertThat(actual).isEmpty();
  }

  @Test
  public void getDatabaseTextIssues_Cesu8_ShouldReturnEncodingByteOrder() throws SQLException {
    // Arrange
    Connection connection = connectionWithCharacterSet("UTF8");

    // Act
    List<DatabaseTextIssue> actual = rdbEngineOracle.getDatabaseTextIssues(connection);

    // Assert
    assertThat(actual)
        .containsExactly(new DatabaseTextIssue("UTF8", DatabaseTextIssue.Kind.ENCODING_BYTE_ORDER));
  }

  @Test
  public void getDatabaseTextIssues_WesternEuropean_ShouldReturnPartialCharacterSet()
      throws SQLException {
    // Arrange
    Connection connection = connectionWithCharacterSet("WE8MSWIN1252");

    // Act
    List<DatabaseTextIssue> actual = rdbEngineOracle.getDatabaseTextIssues(connection);

    // Assert
    assertThat(actual)
        .containsExactly(
            new DatabaseTextIssue("WE8MSWIN1252", DatabaseTextIssue.Kind.PARTIAL_CHARACTER_SET));
  }
}
