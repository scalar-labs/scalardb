package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.scalar.db.storage.jdbc.JdbcCollationTestUtils.CollationProbeResult;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the version-classification logic and the TiDB probe verdicts behind the collation
 * capability gate. Database-independent; runs on every backend the integrationTestJdbc task
 * targets.
 */
public class JdbcCollationTestUtilsTest {

  private static final String TIDB_CAPABLE_VERSION = "8.0.11-TiDB-v8.5.0";
  private static final String TIDB_PRE_74_VERSION = "5.7.25-TiDB-v6.5.0";

  @Test
  public void isCollationCapableMysqlVersion_Mysql8OrLaterVersions_ShouldReturnTrue() {
    assertThat(JdbcCollationTestUtils.isCollationCapableMysqlVersion("8.0.39")).isTrue();
    assertThat(JdbcCollationTestUtils.isCollationCapableMysqlVersion("8.4.2")).isTrue();
    assertThat(JdbcCollationTestUtils.isCollationCapableMysqlVersion("9.1.0")).isTrue();
  }

  @Test
  public void isCollationCapableMysqlVersion_Mysql5Versions_ShouldReturnFalse() {
    assertThat(JdbcCollationTestUtils.isCollationCapableMysqlVersion("5.7.44")).isFalse();
    assertThat(JdbcCollationTestUtils.isCollationCapableMysqlVersion("5.6.51")).isFalse();
  }

  @Test
  public void isCollationCapableMysqlVersion_TidbVersions_ShouldReturnFalse() {
    assertThat(JdbcCollationTestUtils.isCollationCapableMysqlVersion("8.0.11-TiDB-v8.5.0"))
        .isFalse();
    assertThat(JdbcCollationTestUtils.isCollationCapableMysqlVersion("5.7.25-TiDB-v6.5.0"))
        .isFalse();
  }

  @Test
  public void isCollationCapableMysqlVersion_MariaDbVersions_ShouldReturnFalse() {
    assertThat(JdbcCollationTestUtils.isCollationCapableMysqlVersion("11.4.2-MariaDB")).isFalse();
    assertThat(JdbcCollationTestUtils.isCollationCapableMysqlVersion("10.11.8-MariaDB-ubu2204"))
        .isFalse();
  }

  @Test
  public void isCollationCapableMysqlVersion_NullVersion_ShouldReturnFalse() {
    assertThat(JdbcCollationTestUtils.isCollationCapableMysqlVersion(null)).isFalse();
  }

  @Test
  public void isTidbVersion_TidbVersions_ShouldReturnTrue() {
    assertThat(JdbcCollationTestUtils.isTidbVersion("8.0.11-TiDB-v8.5.0")).isTrue();
    assertThat(JdbcCollationTestUtils.isTidbVersion("5.7.25-TiDB-v6.5.0")).isTrue();
  }

  @Test
  public void isTidbVersion_NonTidbVersions_ShouldReturnFalse() {
    assertThat(JdbcCollationTestUtils.isTidbVersion("8.4.2")).isFalse();
    assertThat(JdbcCollationTestUtils.isTidbVersion("11.4.2-MariaDB")).isFalse();
    assertThat(JdbcCollationTestUtils.isTidbVersion(null)).isFalse();
  }

  @Test
  public void isCollationIncapableTidbVersion_VersionsBelowTidb74_ShouldReturnTrue() {
    assertThat(JdbcCollationTestUtils.isCollationIncapableTidbVersion("5.7.25-TiDB-v6.5.0"))
        .isTrue();
    assertThat(JdbcCollationTestUtils.isCollationIncapableTidbVersion("8.0.11-TiDB-v7.3.0"))
        .isTrue();
    assertThat(JdbcCollationTestUtils.isCollationIncapableTidbVersion("5.7.25-TiDB-v5.4.0"))
        .isTrue();
  }

  @Test
  public void isCollationIncapableTidbVersion_VersionsAtOrAboveTidb74_ShouldReturnFalse() {
    assertThat(JdbcCollationTestUtils.isCollationIncapableTidbVersion("8.0.11-TiDB-v7.4.0"))
        .isFalse();
    assertThat(JdbcCollationTestUtils.isCollationIncapableTidbVersion("8.0.11-TiDB-v7.5.0"))
        .isFalse();
    assertThat(JdbcCollationTestUtils.isCollationIncapableTidbVersion("8.0.11-TiDB-v8.5.0"))
        .isFalse();
    assertThat(JdbcCollationTestUtils.isCollationIncapableTidbVersion("8.0.11-TiDB-v7.10.0"))
        .isFalse();
    assertThat(JdbcCollationTestUtils.isCollationIncapableTidbVersion("8.0.11-TiDB-v8.5.0-alpha"))
        .isFalse();
  }

  @Test
  public void isCollationIncapableTidbVersion_UnparseableTidbVersion_ShouldReturnFalse() {
    assertThat(JdbcCollationTestUtils.isCollationIncapableTidbVersion(null)).isFalse();
    assertThat(JdbcCollationTestUtils.isCollationIncapableTidbVersion("")).isFalse();
    assertThat(JdbcCollationTestUtils.isCollationIncapableTidbVersion("8.0.11-TiDB")).isFalse();
    assertThat(JdbcCollationTestUtils.isCollationIncapableTidbVersion("8.0.11-TiDB-vX.Y"))
        .isFalse();
    assertThat(JdbcCollationTestUtils.isCollationIncapableTidbVersion("8.0.39")).isFalse();
  }

  @Test
  public void isUnknownCollationError_UnknownCollationErrorCode_ShouldReturnTrue() {
    assertThat(
            JdbcCollationTestUtils.isUnknownCollationError(
                new SQLException("Unknown collation: 'utf8mb4_uca1400_ai_ci'", "HY000", 1273)))
        .isTrue();
  }

  @Test
  public void isUnknownCollationError_OtherErrorCodes_ShouldReturnFalse() {
    // Connection-class and permission-class errors must stay PROBE_FAILED, not UNSUPPORTED
    assertThat(
            JdbcCollationTestUtils.isUnknownCollationError(
                new SQLException("Connection reset", "08S01", 0)))
        .isFalse();
    assertThat(
            JdbcCollationTestUtils.isUnknownCollationError(
                new SQLException("Access denied", "28000", 1045)))
        .isFalse();
  }

  @Test
  public void probeTidbCollationSupport_CaseInsensitiveComparison_ShouldReturnSupported()
      throws SQLException {
    Statement statement = statementReturningComparison(true);

    assertThat(JdbcCollationTestUtils.probeTidbCollationSupport(statement, TIDB_CAPABLE_VERSION))
        .isEqualTo(CollationProbeResult.SUPPORTED);
  }

  @Test
  public void probeTidbCollationSupport_CaseSensitiveComparison_ShouldThrowIllegalStateException()
      throws SQLException {
    Statement statement = statementReturningComparison(false);

    assertThatThrownBy(
            () -> JdbcCollationTestUtils.probeTidbCollationSupport(statement, TIDB_CAPABLE_VERSION))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("new_collations_enabled_on_first_bootstrap");
  }

  @Test
  public void
      probeTidbCollationSupport_UnknownCollationErrorOnPre74Version_ShouldReturnUnsupported()
          throws SQLException {
    Statement statement = statementThrowing(new SQLException("Unknown collation", "HY000", 1273));

    assertThat(JdbcCollationTestUtils.probeTidbCollationSupport(statement, TIDB_PRE_74_VERSION))
        .isEqualTo(CollationProbeResult.UNSUPPORTED);
  }

  @Test
  public void
      probeTidbCollationSupport_UnknownCollationErrorOnCapableVersion_ShouldReturnProbeFailed()
          throws SQLException {
    Statement statement = statementThrowing(new SQLException("Unknown collation", "HY000", 1273));

    assertThat(JdbcCollationTestUtils.probeTidbCollationSupport(statement, TIDB_CAPABLE_VERSION))
        .isEqualTo(CollationProbeResult.PROBE_FAILED);
  }

  @Test
  public void probeTidbCollationSupport_OtherSqlErrorOnCapableVersion_ShouldReturnProbeFailed()
      throws SQLException {
    Statement statement = statementThrowing(new SQLException("Access denied", "28000", 1045));

    assertThat(JdbcCollationTestUtils.probeTidbCollationSupport(statement, TIDB_CAPABLE_VERSION))
        .isEqualTo(CollationProbeResult.PROBE_FAILED);
  }

  @Test
  public void probeTidbCollationSupport_NoRowsReturned_ShouldReturnProbeFailed()
      throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(false);
    Statement statement = mock(Statement.class);
    when(statement.executeQuery(anyString())).thenReturn(resultSet);

    assertThat(JdbcCollationTestUtils.probeTidbCollationSupport(statement, TIDB_CAPABLE_VERSION))
        .isEqualTo(CollationProbeResult.PROBE_FAILED);
  }

  private static Statement statementReturningComparison(boolean caseInsensitive)
      throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getBoolean(1)).thenReturn(caseInsensitive);
    Statement statement = mock(Statement.class);
    when(statement.executeQuery(anyString())).thenReturn(resultSet);
    return statement;
  }

  private static Statement statementThrowing(SQLException e) throws SQLException {
    Statement statement = mock(Statement.class);
    when(statement.executeQuery(anyString())).thenThrow(e);
    return statement;
  }
}
