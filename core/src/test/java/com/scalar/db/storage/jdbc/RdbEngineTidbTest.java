package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.config.DatabaseConfig;
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
  void adjustJdbcUrl_WithNoParams_ShouldAppendMysqlSchemeAndDisableReadOnlyPropagation() {
    String result = rdbEngineTidb.adjustJdbcUrl("jdbc:mysql://localhost:4000/");
    assertThat(result)
        .isEqualTo(
            "jdbc:mysql://localhost:4000/?permitMysqlScheme=true&readOnlyPropagatesToServer=false");
  }

  @Test
  void adjustJdbcUrl_WithExistingParams_ShouldAppendMysqlSchemeAndDisableReadOnlyPropagation() {
    String result = rdbEngineTidb.adjustJdbcUrl("jdbc:mysql://localhost:4000/?sslMode=REQUIRED");
    assertThat(result)
        .isEqualTo(
            "jdbc:mysql://localhost:4000/?sslMode=REQUIRED&permitMysqlScheme=true"
                + "&readOnlyPropagatesToServer=false");
  }

  @Test
  void adjustJdbcUrl_WithPermitMysqlSchemeAlreadyPresent_ShouldOnlyDisableReadOnlyPropagation() {
    String result =
        rdbEngineTidb.adjustJdbcUrl("jdbc:mysql://localhost:4000/?permitMysqlScheme=true");
    assertThat(result)
        .isEqualTo(
            "jdbc:mysql://localhost:4000/?permitMysqlScheme=true&readOnlyPropagatesToServer=false");
  }

  @Test
  void adjustJdbcUrl_WithReadOnlyPropagatesToServerAlreadyPresent_ShouldNotAppendItAgain() {
    // A user-specified value must win, so the driver option is never set twice.
    String url =
        "jdbc:mysql://localhost:4000/?permitMysqlScheme=true&readOnlyPropagatesToServer=true";
    assertThat(rdbEngineTidb.adjustJdbcUrl(url)).isEqualTo(url);
  }

  @Test
  void adjustJdbcUrl_ShouldDisableReadOnlyPropagationSoTidbAcceptsSetReadOnly() {
    // MariaDB Connector/J 3.5.10 (CONJ-1307) makes Connection.setReadOnly(true) issue
    // "SET SESSION TRANSACTION READ ONLY", which TiDB rejects with error 1235 unless
    // tidb_enable_noop_functions is enabled. The adjusted URL must keep the driver from
    // propagating the read-only state to the server.
    assertThat(rdbEngineTidb.adjustJdbcUrl(ANY_JDBC_URL))
        .contains("readOnlyPropagatesToServer=false");
  }
}
