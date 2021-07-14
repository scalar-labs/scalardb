package com.scalar.db.storage.cassandra;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ConnectionException;
import java.util.Properties;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** */
public class ClusterManagerTest {
  private static final String VALID_ADDRESS = "127.0.0.1";
  private static final String INVALID_ADDRESS = "1024.0.0.1";
  private static final String ANY_USERNAME = "username";
  private static final String ANY_PASSWORD = "password";
  private static final String ANY_KEYSPACE_NAME = "keyspace";
  private static final String ANY_TABLE_NAME = "table";
  @Mock private Cluster cluster;
  @Mock private Session session;
  private ClusterManager manager;
  private Properties props;
  private DatabaseConfig config;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, VALID_ADDRESS);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    config = new DatabaseConfig(props);

    manager = Mockito.spy(new ClusterManager(config));
    doReturn(cluster).when(manager).getCluster(config);
    when(cluster.connect()).thenReturn(session);
  }

  @Test
  public void getSession_CalledOnce_ShouldConnectAndReturn() {
    // Act Assert
    assertThatCode(
            () -> {
              manager.getSession();
            })
        .doesNotThrowAnyException();

    // Assert
    verify(manager).build();
    verify(cluster).connect();
  }

  @Test
  public void getSession_ConfigWithInvalidEndpointGiven_ShouldThrowExceptionProperly() {
    // Arrange
    props.setProperty(DatabaseConfig.CONTACT_POINTS, INVALID_ADDRESS);
    config = new DatabaseConfig(props);
    manager = Mockito.spy(new ClusterManager(config));

    // Act Assert
    assertThatThrownBy(
            () -> {
              manager.getSession();
            })
        .isInstanceOf(ConnectionException.class)
        .hasCauseExactlyInstanceOf(IllegalArgumentException.class);

    // Assert
    verify(cluster, never()).connect();
  }

  @Test
  public void getSession_CalledOnceAndDriverExceptionThrown_ShouldThrowConnectionException() {
    // Arrange
    DriverException toThrow = mock(DriverException.class);
    when(cluster.connect()).thenThrow(toThrow);

    // Act Assert
    assertThatThrownBy(
            () -> {
              manager.getSession();
            })
        .isInstanceOf(ConnectionException.class)
        .hasCause(toThrow);

    // Assert
    verify(manager).build();
    verify(cluster).connect();
  }

  @Test
  public void getSession_CalledAfterBuilt_ShouldNotBuild() {
    // Arrange
    manager.build();

    // Act Assert
    assertThatCode(
            () -> {
              manager.getSession();
            })
        .doesNotThrowAnyException();

    // Assert
    verify(manager).build();
    verify(cluster).connect();
  }

  @Test
  public void getSession_CalledTwice_ShouldReuseSession() {
    // Arrange
    manager.getSession();

    // Act Assert
    assertThatCode(
            () -> {
              manager.getSession();
            })
        .doesNotThrowAnyException();

    // Assert
    verify(manager).build();
    verify(cluster).connect();
  }

  @Test
  public void constructor_NullGiven_ShouldThrowNullPointerException() {
    // Act Assert
    assertThatThrownBy(
            () -> {
              new ClusterManager(null);
            })
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void getMetadata_ExistingKeyspaceAndTableGiven_ShouldReturnMetadata() {
    // Arrange
    manager.getSession();
    Metadata metadata = mock(Metadata.class);
    KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(cluster.getMetadata()).thenReturn(metadata);
    when(metadata.getKeyspace(anyString())).thenReturn(keyspaceMetadata);
    when(keyspaceMetadata.getTable(anyString())).thenReturn(tableMetadata);

    // Act
    TableMetadata actual = manager.getMetadata(ANY_KEYSPACE_NAME, ANY_TABLE_NAME);

    // Assert
    assertThat(actual).isEqualTo(tableMetadata);
  }

  @Test
  public void getMetadata_NoHostAvailable_ShouldThrowConnectionException() {
    // Arrange
    manager.getSession();
    NoHostAvailableException toThrow = mock(NoHostAvailableException.class);
    when(cluster.getMetadata()).thenThrow(toThrow);

    // Act Assert
    assertThatThrownBy(
            () -> {
              manager.getMetadata(ANY_KEYSPACE_NAME, ANY_TABLE_NAME);
            })
        .isInstanceOf(ConnectionException.class)
        .hasCause(toThrow);
  }

  @Test
  public void getMetadata_KeyspaceNotExists_ShouldReturnNull() {
    // Arrange
    manager.getSession();
    Metadata metadata = mock(Metadata.class);
    when(cluster.getMetadata()).thenReturn(metadata);
    when(metadata.getKeyspace(anyString())).thenReturn(null);

    // Act
    TableMetadata actual = manager.getMetadata(ANY_KEYSPACE_NAME, ANY_TABLE_NAME);

    // Assert
    assertThat(actual).isNull();
  }

  @Test
  public void getMetadata_TableNotExists_ShouldReturnNull() {
    // Arrange
    manager.getSession();
    Metadata metadata = mock(Metadata.class);
    KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
    when(cluster.getMetadata()).thenReturn(metadata);
    when(metadata.getKeyspace(anyString())).thenReturn(keyspaceMetadata);
    when(keyspaceMetadata.getTable(anyString())).thenReturn(null);

    // Act
    TableMetadata actual = manager.getMetadata(ANY_KEYSPACE_NAME, ANY_TABLE_NAME);

    // Assert
    assertThat(actual).isNull();
  }
}
