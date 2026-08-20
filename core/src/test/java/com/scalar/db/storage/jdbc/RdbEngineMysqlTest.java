package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.sql.Connection;
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

  @Test
  void setConnectionToReadOnly_ShouldDoNothing() throws SQLException {
    // MariaDB Connector/J 3.5.10 and later issue SET SESSION TRANSACTION READ ONLY / READ WRITE
    // from Connection#setReadOnly(), which adds two round trips per read and which TiDB rejects.
    // This override was first added in #2801 and dropped in #3428; if this test starts failing,
    // check whether the driver still propagates the read-only state before relaxing it.
    Connection connection = mock(Connection.class);

    rdbEngineMysql.setConnectionToReadOnly(connection, true);

    verify(connection, never()).setReadOnly(anyBoolean());
  }
}
