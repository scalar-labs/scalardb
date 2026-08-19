package com.scalar.db.storage.jdbc;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.scalar.db.config.DatabaseConfig;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RdbEngineTidbTest {

  // TiDB is MySQL compatible and uses the same connection string, cf. RdbEngineFactory#create()
  private static final String ANY_JDBC_URL = "jdbc:mysql://localhost:4000/";

  private RdbEngineTidb rdbEngineTidb;

  @BeforeEach
  void setUp() {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_JDBC_URL);
    props.setProperty(DatabaseConfig.STORAGE, "jdbc");
    rdbEngineTidb = new RdbEngineTidb(new JdbcConfig(new DatabaseConfig(props)));
  }

  @Test
  void setConnectionToReadOnly_ShouldDoNothing() throws SQLException {
    // TiDB rejects "SET SESSION TRANSACTION READ ONLY" with error 1235 unless
    // tidb_enable_noop_functions is enabled, and MariaDB Connector/J 3.5.10 and later issue that
    // statement from Connection#setReadOnly(). Inherited from RdbEngineMysql.
    Connection connection = mock(Connection.class);

    rdbEngineTidb.setConnectionToReadOnly(connection, true);

    verify(connection, never()).setReadOnly(true);
  }
}
