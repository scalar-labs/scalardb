package com.scalar.db.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.storage.cassandra.Cassandra;
import com.scalar.db.storage.cassandra.CassandraAdmin;
import com.scalar.db.storage.cosmos.Cosmos;
import com.scalar.db.storage.cosmos.CosmosAdmin;
import com.scalar.db.storage.dynamo.Dynamo;
import com.scalar.db.storage.dynamo.DynamoAdmin;
import com.scalar.db.storage.jdbc.JdbcAdmin;
import com.scalar.db.storage.jdbc.JdbcDatabase;
import com.scalar.db.storage.multistorage.MultiStorage;
import com.scalar.db.storage.multistorage.MultiStorageAdmin;
import com.scalar.db.storage.rpc.GrpcAdmin;
import com.scalar.db.storage.rpc.GrpcStorage;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitManager;
import com.scalar.db.transaction.jdbc.JdbcTransactionManager;
import com.scalar.db.transaction.rpc.GrpcTransactionManager;
import java.util.Collections;
import java.util.Properties;
import org.junit.Test;

public class DatabaseConfigTest {
  private static final String ANY_HOST = "localhost";
  private static final int ANY_PORT = 9999;
  private static final String ANY_USERNAME = "username";
  private static final String ANY_PASSWORD = "password";

  @Test
  public void constructor_PropertiesWithoutPortGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);

    // Act
    DatabaseConfig config = new DatabaseConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_HOST));
    assertThat(config.getContactPort()).isEqualTo(0);
    assertThat(config.getUsername().isPresent()).isTrue();
    assertThat(config.getUsername().get()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword().isPresent()).isTrue();
    assertThat(config.getPassword().get()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getStorageClass()).isEqualTo(Cassandra.class);
    assertThat(config.getAdminClass()).isEqualTo(CassandraAdmin.class);
    assertThat(config.getTransactionManagerClass()).isEqualTo(ConsensusCommitManager.class);
    assertThat(config.getTableMetadataCacheExpirationTimeSecs()).isEqualTo(-1);
  }

  @Test
  public void constructor_PropertiesWithoutUsernameGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.CONTACT_PORT, Integer.toString(ANY_PORT));
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);

    // Act
    DatabaseConfig config = new DatabaseConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_HOST));
    assertThat(config.getContactPort()).isEqualTo(ANY_PORT);
    assertThat(config.getUsername().isPresent()).isFalse();
    assertThat(config.getPassword().isPresent()).isTrue();
    assertThat(config.getPassword().get()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getStorageClass()).isEqualTo(Cassandra.class);
    assertThat(config.getAdminClass()).isEqualTo(CassandraAdmin.class);
    assertThat(config.getTransactionManagerClass()).isEqualTo(ConsensusCommitManager.class);
    assertThat(config.getTableMetadataCacheExpirationTimeSecs()).isEqualTo(-1);
  }

  @Test
  public void constructor_PropertiesWithoutPasswordGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.CONTACT_PORT, Integer.toString(ANY_PORT));
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);

    // Act
    DatabaseConfig config = new DatabaseConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_HOST));
    assertThat(config.getContactPort()).isEqualTo(ANY_PORT);
    assertThat(config.getUsername().isPresent()).isTrue();
    assertThat(config.getUsername().get()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword().isPresent()).isFalse();
    assertThat(config.getStorageClass()).isEqualTo(Cassandra.class);
    assertThat(config.getAdminClass()).isEqualTo(CassandraAdmin.class);
    assertThat(config.getTransactionManagerClass()).isEqualTo(ConsensusCommitManager.class);
    assertThat(config.getTableMetadataCacheExpirationTimeSecs()).isEqualTo(-1);
  }

  @Test
  public void constructor_PropertiesWithPortGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.CONTACT_PORT, Integer.toString(ANY_PORT));
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);

    // Act
    DatabaseConfig config = new DatabaseConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_HOST));
    assertThat(config.getContactPort()).isEqualTo(ANY_PORT);
    assertThat(config.getUsername().isPresent()).isTrue();
    assertThat(config.getUsername().get()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword().isPresent()).isTrue();
    assertThat(config.getPassword().get()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getStorageClass()).isEqualTo(Cassandra.class);
    assertThat(config.getAdminClass()).isEqualTo(CassandraAdmin.class);
    assertThat(config.getTransactionManagerClass()).isEqualTo(ConsensusCommitManager.class);
    assertThat(config.getTableMetadataCacheExpirationTimeSecs()).isEqualTo(-1);
  }

  @Test
  public void constructor_NonQualifiedPropertiesGiven_ShouldThrowNullPointerException() {
    // Arrange
    Properties props = new Properties();

    // Act Assert
    assertThatThrownBy(() -> new DatabaseConfig(props)).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void constructor_PropertiesWithNegativePortGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.CONTACT_PORT, Integer.toString(-1));
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);

    // Act Assert
    assertThatThrownBy(() -> new DatabaseConfig(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void constructor_PropertiesWithCassandraGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, "cassandra");

    // Act
    DatabaseConfig config = new DatabaseConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_HOST));
    assertThat(config.getContactPort()).isEqualTo(0);
    assertThat(config.getUsername().isPresent()).isTrue();
    assertThat(config.getUsername().get()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword().isPresent()).isTrue();
    assertThat(config.getPassword().get()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getStorageClass()).isEqualTo(Cassandra.class);
    assertThat(config.getAdminClass()).isEqualTo(CassandraAdmin.class);
  }

  @Test
  public void constructor_PropertiesWithCosmosGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, "cosmos");

    // Act
    DatabaseConfig config = new DatabaseConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_HOST));
    assertThat(config.getContactPort()).isEqualTo(0);
    assertThat(config.getUsername().isPresent()).isTrue();
    assertThat(config.getUsername().get()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword().isPresent()).isTrue();
    assertThat(config.getPassword().get()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getStorageClass()).isEqualTo(Cosmos.class);
    assertThat(config.getAdminClass()).isEqualTo(CosmosAdmin.class);
  }

  @Test
  public void constructor_PropertiesWithDynamoGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, "dynamo");

    // Act
    DatabaseConfig config = new DatabaseConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_HOST));
    assertThat(config.getContactPort()).isEqualTo(0);
    assertThat(config.getUsername().isPresent()).isTrue();
    assertThat(config.getUsername().get()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword().isPresent()).isTrue();
    assertThat(config.getPassword().get()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getStorageClass()).isEqualTo(Dynamo.class);
    assertThat(config.getAdminClass()).isEqualTo(DynamoAdmin.class);
  }

  @Test
  public void constructor_PropertiesWithJdbcGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, "jdbc");

    // Act
    DatabaseConfig config = new DatabaseConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_HOST));
    assertThat(config.getContactPort()).isEqualTo(0);
    assertThat(config.getUsername().isPresent()).isTrue();
    assertThat(config.getUsername().get()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword().isPresent()).isTrue();
    assertThat(config.getPassword().get()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getStorageClass()).isEqualTo(JdbcDatabase.class);
    assertThat(config.getAdminClass()).isEqualTo(JdbcAdmin.class);
  }

  @Test
  public void constructor_PropertiesWithMultiStorageGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.STORAGE, "multi-storage");

    // Act
    DatabaseConfig config = new DatabaseConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isNull();
    assertThat(config.getContactPort()).isEqualTo(0);
    assertThat(config.getUsername().isPresent()).isFalse();
    assertThat(config.getPassword().isPresent()).isFalse();
    assertThat(config.getStorageClass()).isEqualTo(MultiStorage.class);
    assertThat(config.getAdminClass()).isEqualTo(MultiStorageAdmin.class);
  }

  @Test
  public void constructor_PropertiesWithGrpcGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, "grpc");

    // Act
    DatabaseConfig config = new DatabaseConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_HOST));
    assertThat(config.getContactPort()).isEqualTo(0);
    assertThat(config.getUsername().isPresent()).isTrue();
    assertThat(config.getUsername().get()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword().isPresent()).isTrue();
    assertThat(config.getPassword().get()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getStorageClass()).isEqualTo(GrpcStorage.class);
    assertThat(config.getAdminClass()).isEqualTo(GrpcAdmin.class);
  }

  @Test
  public void constructor_WrongStorageClassGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, "WrongStorage");

    // Act Assert
    assertThatThrownBy(() -> new DatabaseConfig(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      constructor_PropertiesWithConsensusCommitTransactionManagerGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.TRANSACTION_MANAGER, "consensus-commit");

    // Act
    DatabaseConfig config = new DatabaseConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_HOST));
    assertThat(config.getContactPort()).isEqualTo(0);
    assertThat(config.getUsername().isPresent()).isTrue();
    assertThat(config.getUsername().get()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword().isPresent()).isTrue();
    assertThat(config.getPassword().get()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getStorageClass()).isEqualTo(Cassandra.class);
    assertThat(config.getAdminClass()).isEqualTo(CassandraAdmin.class);
    assertThat(config.getTransactionManagerClass()).isEqualTo(ConsensusCommitManager.class);
  }

  @Test
  public void
      constructor_PropertiesWithJdbcTransactionManagerAndJdbcStorageGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, "jdbc");
    props.setProperty(DatabaseConfig.TRANSACTION_MANAGER, "jdbc");

    // Act
    DatabaseConfig config = new DatabaseConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_HOST));
    assertThat(config.getContactPort()).isEqualTo(0);
    assertThat(config.getUsername().isPresent()).isTrue();
    assertThat(config.getUsername().get()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword().isPresent()).isTrue();
    assertThat(config.getPassword().get()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getStorageClass()).isEqualTo(JdbcDatabase.class);
    assertThat(config.getAdminClass()).isEqualTo(JdbcAdmin.class);
    assertThat(config.getTransactionManagerClass()).isEqualTo(JdbcTransactionManager.class);
  }

  @Test
  public void
      constructor_PropertiesWithJdbcTransactionManagerAndCassandraStorageGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, "cassandra");
    props.setProperty(DatabaseConfig.TRANSACTION_MANAGER, "jdbc");

    // Act Assert
    assertThatThrownBy(() -> new DatabaseConfig(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      constructor_PropertiesWithGrpcTransactionManagerAndGrpcStorageGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, "grpc");
    props.setProperty(DatabaseConfig.TRANSACTION_MANAGER, "grpc");

    // Act
    DatabaseConfig config = new DatabaseConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_HOST));
    assertThat(config.getContactPort()).isEqualTo(0);
    assertThat(config.getUsername().isPresent()).isTrue();
    assertThat(config.getUsername().get()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword().isPresent()).isTrue();
    assertThat(config.getPassword().get()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getStorageClass()).isEqualTo(GrpcStorage.class);
    assertThat(config.getAdminClass()).isEqualTo(GrpcAdmin.class);
    assertThat(config.getTransactionManagerClass()).isEqualTo(GrpcTransactionManager.class);
  }

  @Test
  public void
      constructor_PropertiesWithGrpcTransactionManagerAndCassandraStorageGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.STORAGE, "cassandra");
    props.setProperty(DatabaseConfig.TRANSACTION_MANAGER, "grpc");

    // Act Assert
    assertThatThrownBy(() -> new DatabaseConfig(props))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void constructor_PropertiesWithTableMetadataExpirationTimeSecsGiven_ShouldLoadProperly() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_HOST);
    props.setProperty(DatabaseConfig.USERNAME, ANY_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, ANY_PASSWORD);
    props.setProperty(DatabaseConfig.TABLE_METADATA_CACHE_EXPIRATION_TIME_SECS, "3600");

    // Act
    DatabaseConfig config = new DatabaseConfig(props);

    // Assert
    assertThat(config.getContactPoints()).isEqualTo(Collections.singletonList(ANY_HOST));
    assertThat(config.getContactPort()).isEqualTo(0);
    assertThat(config.getUsername().isPresent()).isTrue();
    assertThat(config.getUsername().get()).isEqualTo(ANY_USERNAME);
    assertThat(config.getPassword().isPresent()).isTrue();
    assertThat(config.getPassword().get()).isEqualTo(ANY_PASSWORD);
    assertThat(config.getTableMetadataCacheExpirationTimeSecs()).isEqualTo(3600);
  }
}
