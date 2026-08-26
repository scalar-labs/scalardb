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
}
