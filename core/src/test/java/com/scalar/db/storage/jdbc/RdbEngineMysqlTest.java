package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RdbEngineMysqlTest {

  private RdbEngineMysql rdbEngineMysql;

  @BeforeEach
  void setUp() {
    rdbEngineMysql = new RdbEngineMysql();
  }

  @Test
  void adjustJdbcUrl_WithNoParams_ShouldAppendPermitMysqlScheme() {
    String result = rdbEngineMysql.adjustJdbcUrl("jdbc:mysql://localhost:3306/");
    assertThat(result).isEqualTo("jdbc:mysql://localhost:3306/?permitMysqlScheme=true");
  }

  @Test
  void adjustJdbcUrl_WithExistingParams_ShouldAppendPermitMysqlScheme() {
    String result = rdbEngineMysql.adjustJdbcUrl("jdbc:mysql://localhost:3306/?sslMode=REQUIRED");
    assertThat(result)
        .isEqualTo("jdbc:mysql://localhost:3306/?sslMode=REQUIRED&permitMysqlScheme=true");
  }

  @Test
  void adjustJdbcUrl_WithPermitMysqlSchemeAlreadyPresent_ShouldReturnAsIs() {
    String url = "jdbc:mysql://localhost:3306/?permitMysqlScheme=true";
    String result = rdbEngineMysql.adjustJdbcUrl(url);
    assertThat(result).isEqualTo(url);
  }

  /**
   * The AWS Advanced JDBC Wrapper reports failover with SQLState 08001, 08S02, and 08007. These
   * must never be treated as conflicts. {@link com.scalar.db.storage.jdbc.JdbcDatabase} converts a
   * conflict into a {@code RetriableExecutionException}, which tells the caller the operation
   * definitely did not apply and is safe to retry. That guarantee does not hold for a failover,
   * where the outcome may be unknown or the write may already have been applied. Adding a failover
   * error to this check would silently break the storage layer's safety.
   */
  @Test
  void isConflict_GivenFailoverSqlStates_ShouldReturnFalse() {
    // Act
    // Assert
    assertThat(rdbEngineMysql.isConflict(new SQLException("failover failed", "08001"))).isFalse();
    assertThat(rdbEngineMysql.isConflict(new SQLException("communication link changed", "08S02")))
        .isFalse();
    assertThat(
            rdbEngineMysql.isConflict(new SQLException("transaction resolution unknown", "08007")))
        .isFalse();
  }

  @Test
  void isConflict_GivenDeadlockOrLockWaitTimeout_ShouldReturnTrue() {
    // Act
    // Assert
    assertThat(rdbEngineMysql.isConflict(new SQLException("deadlock found", "40001", 1213)))
        .isTrue();
    assertThat(rdbEngineMysql.isConflict(new SQLException("lock wait timeout", "HY000", 1205)))
        .isTrue();
  }

  @Test
  void isConflict_GivenNullSqlState_ShouldReturnFalse() {
    // Act
    // Assert
    assertThat(rdbEngineMysql.isConflict(new SQLException("no sql state"))).isFalse();
  }

  /**
   * ScalarDB reaches MySQL through MariaDB Connector/J and does not bundle MySQL Connector/J, but
   * the AWS Advanced JDBC Wrapper defaults to the MySQL Connector/J dialect for a "jdbc:mysql://"
   * URL. Without the dialect parameter the wrapper asks for a driver that is not on the classpath.
   */
  @Test
  void adjustJdbcUrl_GivenAwsWrapperUrl_ShouldAppendSchemeAndDialectParameters() {
    String result = rdbEngineMysql.adjustJdbcUrl("jdbc:aws-wrapper:mysql://localhost:3306/");
    assertThat(result)
        .isEqualTo(
            "jdbc:aws-wrapper:mysql://localhost:3306/"
                + "?permitMysqlScheme=true&wrapperTargetDriverDialect=mariadb-connector-j-3");
  }

  @Test
  void adjustJdbcUrl_GivenAwsWrapperUrlWithExistingParams_ShouldAppendWithAmpersand() {
    String result =
        rdbEngineMysql.adjustJdbcUrl("jdbc:aws-wrapper:mysql://localhost:3306/?sslMode=REQUIRED");
    assertThat(result)
        .isEqualTo(
            "jdbc:aws-wrapper:mysql://localhost:3306/?sslMode=REQUIRED"
                + "&permitMysqlScheme=true&wrapperTargetDriverDialect=mariadb-connector-j-3");
  }

  @Test
  void adjustJdbcUrl_GivenAwsWrapperUrlWithDialectAlreadySet_ShouldNotOverrideIt() {
    String url =
        "jdbc:aws-wrapper:mysql://localhost:3306/?wrapperTargetDriverDialect=mysql-connector-j";
    String result = rdbEngineMysql.adjustJdbcUrl(url);
    // The user's explicit choice survives; only the scheme parameter is added.
    assertThat(result).isEqualTo(url + "&permitMysqlScheme=true");
  }

  @Test
  void adjustJdbcUrl_GivenAwsWrapperUrlWithSchemeParameterAlreadySet_ShouldAppendDialectOnly() {
    // What an existing MySQL user ends up with after prefixing their URL to adopt the wrapper.
    String url = "jdbc:aws-wrapper:mysql://localhost:3306/?permitMysqlScheme=true";
    String result = rdbEngineMysql.adjustJdbcUrl(url);
    assertThat(result).isEqualTo(url + "&wrapperTargetDriverDialect=mariadb-connector-j-3");
  }

  @Test
  void adjustJdbcUrl_GivenAwsWrapperUrlWithBothParametersSet_ShouldReturnAsIs() {
    String url =
        "jdbc:aws-wrapper:mysql://localhost:3306/"
            + "?permitMysqlScheme=true&wrapperTargetDriverDialect=mariadb-connector-j-3";
    String result = rdbEngineMysql.adjustJdbcUrl(url);
    assertThat(result).isEqualTo(url);
  }

  @Test
  void adjustJdbcUrl_GivenNonAwsWrapperUrl_ShouldNotAppendDialectParameter() {
    String result = rdbEngineMysql.adjustJdbcUrl("jdbc:mysql://localhost:3306/");
    assertThat(result).isEqualTo("jdbc:mysql://localhost:3306/?permitMysqlScheme=true");
    assertThat(result).doesNotContain("wrapperTargetDriverDialect");
  }
}
