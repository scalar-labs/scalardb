package com.scalar.db.storage.cassandra;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.scalar.db.exception.storage.ConnectionException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** */
public class ClusterManagerTest {
  private static final String ANY_KEYSPACE_NAME = "keyspace";
  private static final String ANY_TABLE_NAME = "table";
  @Mock private Cluster cluster;
  @Mock private Session session;
  private ClusterManager manager;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    // Arrange
    manager = Mockito.spy(new ClusterManager(cluster, session));
  }

  @Test
  public void constructor_NullGiven_ShouldThrowNullPointerException() {
    // Act Assert
    assertThatThrownBy(() -> new ClusterManager(null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void getMetadata_ExistingKeyspaceAndTableGiven_ShouldReturnMetadata() {
    // Arrange
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
