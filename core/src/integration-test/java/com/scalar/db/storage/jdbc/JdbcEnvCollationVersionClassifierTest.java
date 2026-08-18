package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit test for {@link JdbcEnv#isCollationCapableMysqlVersion(String)}. It is a pure function, so
 * this test needs no database and runs on every backend in milliseconds.
 */
public class JdbcEnvCollationVersionClassifierTest {

  @Test
  public void isCollationCapableMysqlVersion_Mysql8AndLaterVersions_ShouldReturnTrue() {
    assertThat(JdbcEnv.isCollationCapableMysqlVersion("8.0.39")).isTrue();
    assertThat(JdbcEnv.isCollationCapableMysqlVersion("8.4.2")).isTrue();
    assertThat(JdbcEnv.isCollationCapableMysqlVersion("9.1.0")).isTrue();
  }

  @Test
  public void isCollationCapableMysqlVersion_Mysql5Version_ShouldReturnFalse() {
    assertThat(JdbcEnv.isCollationCapableMysqlVersion("5.7.44")).isFalse();
  }

  @Test
  public void isCollationCapableMysqlVersion_TidbVersion_ShouldReturnFalse() {
    assertThat(JdbcEnv.isCollationCapableMysqlVersion("8.0.11-TiDB-v8.5.0")).isFalse();
  }

  @Test
  public void isCollationCapableMysqlVersion_MariadbVersion_ShouldReturnFalse() {
    assertThat(JdbcEnv.isCollationCapableMysqlVersion("11.4.2-MariaDB")).isFalse();
  }

  @Test
  public void isCollationCapableMysqlVersion_NullVersion_ShouldReturnFalse() {
    assertThat(JdbcEnv.isCollationCapableMysqlVersion(null)).isFalse();
  }
}
