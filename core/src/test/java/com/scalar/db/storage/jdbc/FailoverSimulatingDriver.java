package com.scalar.db.storage.jdbc;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

/**
 * A JDBC driver that simulates the exceptions the AWS Advanced JDBC Wrapper raises during an Aurora
 * failover, so the resulting behavior can be pinned without a real database or AWS credentials.
 *
 * <p>The connection it hands out succeeds at connect time and during the setup calls HikariCP makes
 * on a fresh connection. Only {@link Connection#commit()} fails, and only while a SQLState has been
 * armed via {@link #failOnCommitWith(String)}. This is what makes the interesting path reachable:
 * the connection must be established and pooled normally before the failure can be observed.
 *
 * <p>This class must be public with a public no-arg constructor. HikariCP resolves {@code
 * driverClassName} by loading the class and calling {@code getDeclaredConstructor().newInstance()}.
 */
public class FailoverSimulatingDriver implements Driver {

  public static final String URL_PREFIX = "jdbc:scalardb-failover-test:";

  private static final AtomicReference<String> commitSqlState = new AtomicReference<>();

  /** Arms the driver so the next {@code commit()} throws a {@link SQLException} with this state. */
  public static void failOnCommitWith(String sqlState) {
    commitSqlState.set(sqlState);
  }

  public static void reset() {
    commitSqlState.set(null);
  }

  /** Builds a transactional pool backed by this driver, configured the way ScalarDB configures. */
  public static HikariDataSource createDataSource() {
    HikariConfig config = new HikariConfig();
    config.setDriverClassName(FailoverSimulatingDriver.class.getName());
    config.setJdbcUrl(URL_PREFIX + "//localhost/test");
    config.setAutoCommit(false);
    config.setMinimumIdle(0);
    config.setMaximumPoolSize(1);
    config.setConnectionTimeout(1000);
    return new HikariDataSource(config);
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    if (!acceptsURL(url)) {
      return null;
    }

    Connection connection = mock(Connection.class);
    when(connection.isValid(anyInt())).thenReturn(true);

    DatabaseMetaData metaData = mock(DatabaseMetaData.class);
    when(metaData.getDriverName()).thenReturn(FailoverSimulatingDriver.class.getSimpleName());
    when(metaData.getDriverVersion()).thenReturn("1.0");
    when(connection.getMetaData()).thenReturn(metaData);

    doAnswer(
            invocation -> {
              String sqlState = commitSqlState.get();
              if (sqlState != null) {
                throw new SQLException("Simulated failover", sqlState);
              }
              return null;
            })
        .when(connection)
        .commit();

    return connection;
  }

  @Override
  public boolean acceptsURL(String url) {
    return url != null && url.startsWith(URL_PREFIX);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
    return new DriverPropertyInfo[0];
  }

  @Override
  public int getMajorVersion() {
    return 1;
  }

  @Override
  public int getMinorVersion() {
    return 0;
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  @Override
  public Logger getParentLogger() {
    return Logger.getLogger(FailoverSimulatingDriver.class.getName());
  }
}
