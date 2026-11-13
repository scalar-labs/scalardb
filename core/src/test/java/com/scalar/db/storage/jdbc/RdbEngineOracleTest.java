package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.ScanAll;
import com.scalar.db.api.Selection.Conjunction;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
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
  public void isExtendedMaxStringSizeRequiredError_Ora43929_ShouldReturnTrue() {
    // Arrange
    SQLException e = new SQLException("ORA-43929", "99999", 43929);

    // Act
    boolean actual = rdbEngineOracle.isExtendedMaxStringSizeRequiredError(e);

    // Assert
    assertThat(actual).isTrue();
  }

  @Test
  public void isExtendedMaxStringSizeRequiredError_OtherCode_ShouldReturnFalse() {
    // Arrange
    SQLException e = new SQLException("ORA-00942", "42000", 942);

    // Act
    boolean actual = rdbEngineOracle.isExtendedMaxStringSizeRequiredError(e);

    // Assert
    assertThat(actual).isFalse();
  }

  @Test
  public void isFullUnicodeCharset_Al32utf8_ShouldReturnTrue() {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation("BINARY", "AL32UTF8");

    // Act
    boolean actual = rdbEngineOracle.isFullUnicodeCharset(collation);

    // Assert
    assertThat(actual).isTrue();
  }

  @Test
  public void isFullUnicodeCharset_Utf8Cesu_ShouldReturnTrue() {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation("BINARY", "UTF8");

    // Act
    boolean actual = rdbEngineOracle.isFullUnicodeCharset(collation);

    // Assert
    assertThat(actual).isTrue();
  }

  @Test
  public void isFullUnicodeCharset_Al16utf16_ShouldReturnTrue() {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation("BINARY", "AL16UTF16");

    // Act
    boolean actual = rdbEngineOracle.isFullUnicodeCharset(collation);

    // Assert
    assertThat(actual).isTrue();
  }

  @Test
  public void isFullUnicodeCharset_WesternEuropean_ShouldReturnFalse() {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation("BINARY", "WE8MSWIN1252");

    // Act
    boolean actual = rdbEngineOracle.isFullUnicodeCharset(collation);

    // Assert
    assertThat(actual).isFalse();
  }

  @Test
  public void isBinaryCollation_Binary_ShouldReturnTrue() {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation("BINARY", "AL32UTF8");

    // Act
    boolean actual = rdbEngineOracle.isBinaryCollation(collation);

    // Assert
    assertThat(actual).isTrue();
  }

  @Test
  public void isBinaryCollation_BinaryCi_ShouldReturnFalse() {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation("BINARY_CI", "AL32UTF8");

    // Act
    boolean actual = rdbEngineOracle.isBinaryCollation(collation);

    // Assert
    assertThat(actual).isFalse();
  }

  @Test
  public void isBinaryCollation_UsingNlsCompWithBinarySession_ShouldReturnTrue() {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation("USING_NLS_COMP", "AL32UTF8", "BINARY");

    // Act
    boolean actual = rdbEngineOracle.isBinaryCollation(collation);

    // Assert
    assertThat(actual).isTrue();
  }

  @Test
  public void isBinaryCollation_UsingNlsCompWithLinguisticSession_ShouldReturnFalse() {
    // Arrange
    TextColumnCollation collation =
        new TextColumnCollation("USING_NLS_COMP", "AL32UTF8", "LINGUISTIC");

    // Act
    boolean actual = rdbEngineOracle.isBinaryCollation(collation);

    // Assert
    assertThat(actual).isFalse();
  }

  @Test
  public void isBinaryCollation_BinaryOnCesu8Charset_ShouldReturnFalse() {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation("BINARY", "UTF8");

    // Act
    boolean actual = rdbEngineOracle.isBinaryCollation(collation);

    // Assert
    assertThat(actual).isFalse();
  }

  @Test
  public void isBinaryCollation_BinaryOnAl16utf16Charset_ShouldReturnFalse() {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation("BINARY", "AL16UTF16");

    // Act
    boolean actual = rdbEngineOracle.isBinaryCollation(collation);

    // Assert
    assertThat(actual).isFalse();
  }

  @Test
  public void isBinaryCollation_UsingNlsCompWithBinarySessionOnCesu8Charset_ShouldReturnFalse() {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation("USING_NLS_COMP", "UTF8", "BINARY");

    // Act
    boolean actual = rdbEngineOracle.isBinaryCollation(collation);

    // Assert
    assertThat(actual).isFalse();
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
  public void collationClauseRequiresExtendedStringSize_ShouldReturnTrue() {
    // Act
    boolean actual = rdbEngineOracle.collationClauseRequiresExtendedStringSize();

    // Assert
    assertThat(actual).isTrue();
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
  public void getDatabaseEncodingIssue_Al32utf8_ShouldReturnNull() throws SQLException {
    // Arrange
    Connection connection = connectionWithCharacterSet("AL32UTF8");

    // Act
    DatabaseEncodingIssue actual = rdbEngineOracle.getDatabaseEncodingIssue(connection);

    // Assert
    assertThat(actual).isNull();
  }

  @Test
  public void getDatabaseEncodingIssue_Cesu8_ShouldReturnUtf16CodeUnitOrder() throws SQLException {
    // Arrange
    Connection connection = connectionWithCharacterSet("UTF8");

    // Act
    DatabaseEncodingIssue actual = rdbEngineOracle.getDatabaseEncodingIssue(connection);

    // Assert
    assertThat(actual)
        .isEqualTo(
            new DatabaseEncodingIssue("UTF8", DatabaseEncodingIssue.Kind.UTF16_CODE_UNIT_ORDER));
  }

  @Test
  public void getDatabaseEncodingIssue_WesternEuropean_ShouldReturnPartialCharacterSet()
      throws SQLException {
    // Arrange
    Connection connection = connectionWithCharacterSet("WE8MSWIN1252");

    // Act
    DatabaseEncodingIssue actual = rdbEngineOracle.getDatabaseEncodingIssue(connection);

    // Assert
    assertThat(actual)
        .isEqualTo(
            new DatabaseEncodingIssue(
                "WE8MSWIN1252", DatabaseEncodingIssue.Kind.PARTIAL_CHARACTER_SET));
  }
}
