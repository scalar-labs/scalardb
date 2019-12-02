package com.scalar.database.storage.cassandra4driver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.scalar.database.config.DatabaseConfig;
import com.scalar.database.exception.storage.ConnectionException;
import com.scalar.database.exception.storage.StorageRuntimeException;
import java.util.Optional;
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
  private static final String INVALID_PORT = Integer.toString(Integer.MAX_VALUE);
  private static final String ANY_USERNAME = "username";
  private static final String ANY_PASSWORD = "password";
  private static final String ANY_KEYSPACE_NAME = "keyspace";
  private static final String ANY_TABLE_NAME = "table";
  @Mock private CqlSession session;
  @Mock private CqlSessionBuilder builder;
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
    doReturn(builder).when(manager).getBuilder();
    when(builder.build()).thenReturn(session);
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
    verify(manager).getBuilder();
    verify(builder).build();
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
        .isInstanceOf(ConnectionException.class);

    // Assert
    verify(builder, never()).build();
  }

  @Test
  public void getSession_ConfigWithInvalidPortGiven_ShouldThrowExceptionProperly() {
    // Arrange
    props.setProperty(DatabaseConfig.CONTACT_PORT, INVALID_PORT);
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
    verify(builder, never()).build();
  }

  @Test
  public void getSession_CalledOnceAndDriverExceptionThrown_ShouldThrowConnectionException() {
    // Arrange
    DriverException toThrow = mock(DriverException.class);
    when(builder.build()).thenThrow(toThrow);

    // Act Assert
    assertThatThrownBy(
            () -> {
              manager.getSession();
            })
        .isInstanceOf(ConnectionException.class)
        .hasCause(toThrow);

    // Assert
    verify(manager).getBuilder();
    verify(builder).build();
  }

  @Test
  public void getSession_CalledAfterClosed_ShouldBuildAgain() {
    // Arrange
    manager.getSession();
    manager.close();

    // Act Assert
    assertThatCode(
            () -> {
              manager.getSession();
            })
        .doesNotThrowAnyException();

    // Assert
    verify(manager, times(2)).getBuilder();
    verify(builder, times(2)).build();
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
    verify(manager, times(1)).getBuilder();
    verify(builder, times(1)).build();
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
    when(session.getMetadata()).thenReturn(metadata);
    when(metadata.getKeyspace(anyString())).thenReturn(Optional.of(keyspaceMetadata));
    when(keyspaceMetadata.getTable(anyString())).thenReturn(Optional.of(tableMetadata));

    // Act
    TableMetadata actual = manager.getMetadata(ANY_KEYSPACE_NAME, ANY_TABLE_NAME);

    // Assert
    assertThat(actual).isEqualTo(tableMetadata);
  }

  @Test
  public void getMetadata_KeyspaceNotExists_ShouldThrowStorageRuntimeException() {
    // Arrange
    manager.getSession();
    Metadata metadata = mock(Metadata.class);
    when(session.getMetadata()).thenReturn(metadata);
    when(metadata.getKeyspace(anyString())).thenReturn(Optional.empty());

    // Act Assert
    assertThatThrownBy(
            () -> {
              manager.getMetadata(ANY_KEYSPACE_NAME, ANY_TABLE_NAME);
            })
        .isInstanceOf(StorageRuntimeException.class);
  }

  @Test
  public void getMetadata_TableNotExists_ShouldThrowStorageRuntimeException() {
    // Arrange
    manager.getSession();
    Metadata metadata = mock(Metadata.class);
    KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
    when(session.getMetadata()).thenReturn(metadata);
    when(metadata.getKeyspace(anyString())).thenReturn(Optional.of(keyspaceMetadata));
    when(keyspaceMetadata.getTable(anyString())).thenReturn(Optional.empty());

    // Act
    assertThatThrownBy(
            () -> {
              manager.getMetadata(ANY_KEYSPACE_NAME, ANY_TABLE_NAME);
            })
        .isInstanceOf(StorageRuntimeException.class);
  }
}
