package com.scalar.db.storage.cassandra;

import static com.datastax.driver.core.Metadata.quote;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.ServerSideTimestampGenerator;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.scalar.db.config.DatabaseConfig;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class ClusterManagerTest {
  private static final String ANY_KEYSPACE_NAME = "any_keyspace";
  private static final String ANY_TABLE_NAME = "any_table";
  private static final String ANY_CONTACT_POINT = "localhost";
  private static final int ANY_CONTACT_PORT = 9999;
  private static final String ANY_USERNAME = "any_username";
  private static final String ANY_PASSWORD = "any_password";
  @Mock private Cluster cluster;
  @Mock private Session session;
  private ClusterManager manager;

  @BeforeEach
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

  @Test
  public void getCluster_ShouldReturnClusterWithProperConfiguration() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_CONTACT_POINT);

    // Act
    Cluster actual = manager.getCluster(new DatabaseConfig(props));

    // Assert
    assertThat(actual.getClusterName()).isEqualTo("Scalar Cluster");
    assertThat(actual.getConfiguration().getProtocolOptions().getPort())
        .isEqualTo(ClusterManager.DEFAULT_CASSANDRA_PORT);
    assertThat(actual.getConfiguration().getProtocolOptions().getAuthProvider())
        .isSameAs(AuthProvider.NONE);
    assertThat(actual.getConfiguration().getMetricsOptions().isJMXReportingEnabled()).isFalse();
    // Non-conditional writes must take their write timestamps from the same source as lightweight
    // transactions, whose timestamps always come from the server
    assertThat(actual.getConfiguration().getPolicies().getTimestampGenerator())
        .isSameAs(ServerSideTimestampGenerator.INSTANCE);
    assertThat(actual.getConfiguration().getPolicies().getRetryPolicy())
        .isSameAs(DefaultRetryPolicy.INSTANCE);
    assertThat(actual.getConfiguration().getPolicies().getLoadBalancingPolicy())
        .isInstanceOf(TokenAwarePolicy.class);
    assertThat(
            ((TokenAwarePolicy) actual.getConfiguration().getPolicies().getLoadBalancingPolicy())
                .getChildPolicy())
        .isInstanceOf(DCAwareRoundRobinPolicy.class);
    // Requests for a partition must always reach the same coordinator, because server-side write
    // timestamps are monotonic only per coordinator. TokenAwarePolicy exposes no getter for it
    assertThat(actual.getConfiguration().getPolicies().getLoadBalancingPolicy())
        .hasFieldOrPropertyWithValue(
            "replicaOrdering", TokenAwarePolicy.ReplicaOrdering.TOPOLOGICAL);
  }

  @Test
  public void getCluster_ContactPortGiven_ShouldReturnClusterWithTheContactPort() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_CONTACT_POINT);
    props.setProperty(DatabaseConfig.CONTACT_PORT, String.valueOf(ANY_CONTACT_PORT));

    // Act
    Cluster actual = manager.getCluster(new DatabaseConfig(props));

    // Assert
    assertThat(actual.getConfiguration().getProtocolOptions().getPort())
        .isEqualTo(ANY_CONTACT_PORT);
  }

  @Test
  public void getCluster_UsernameAndPasswordGiven_ShouldReturnClusterWithCredentials() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_CONTACT_POINT);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);

    // Act
    Cluster actual = manager.getCluster(new DatabaseConfig(props));

    // Assert
    assertThat(actual.getConfiguration().getProtocolOptions().getAuthProvider())
        .isInstanceOf(PlainTextAuthProvider.class);
  }
}
