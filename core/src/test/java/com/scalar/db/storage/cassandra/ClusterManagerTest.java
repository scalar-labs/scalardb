package com.scalar.db.storage.cassandra;

import static com.datastax.driver.core.Metadata.quote;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class ClusterManagerTest {
  private static final String ANY_KEYSPACE_NAME = "any_keyspace";
  private static final String ANY_TABLE_NAME = "any_table";
  @Mock private Cluster cluster;
  @Mock private Session session;
  private ClusterManager manager;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

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
  public void getMetadata_WithReservedKeywordsExistingKeyspaceAndTableGiven_ShouldReturnMetadata() {
    // Arrange
    String keyspace = "keyspace";
    String table = "table";

    Metadata metadata = mock(Metadata.class);
    KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(cluster.getMetadata()).thenReturn(metadata);
    when(metadata.getKeyspace(quote(keyspace))).thenReturn(keyspaceMetadata);
    when(keyspaceMetadata.getTable(quote(table))).thenReturn(tableMetadata);

    // Act
    TableMetadata actual = manager.getMetadata(keyspace, table);

    // Assert
    assertThat(actual).isEqualTo(tableMetadata);
  }

  @Test
  public void getMetadata_NoHostAvailable_ShouldThrowNoHostAvailableException() {
    // Arrange
    when(cluster.getMetadata()).thenThrow(NoHostAvailableException.class);

    // Act Assert
    assertThatThrownBy(() -> manager.getMetadata(ANY_KEYSPACE_NAME, ANY_TABLE_NAME))
        .isInstanceOf(NoHostAvailableException.class);
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
