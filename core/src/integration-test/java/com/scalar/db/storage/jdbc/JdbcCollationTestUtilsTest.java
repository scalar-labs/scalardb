package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the pure version-classification logic behind the collation capability gate.
 * Database-independent; runs on every backend the integrationTestJdbc task targets.
 */
public class JdbcCollationTestUtilsTest {

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
}
