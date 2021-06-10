package com.scalar.db.storage.dynamo;

import static org.mockito.Mockito.verify;

import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class DynamoAdminTest {

  @Mock DynamoTableMetadataManager metadataManager;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void
      getTableMetadata_ConstructedWithoutNamespacePrefix_ShouldBeCalledWithoutNamespacePrefix() {
    // Arrange
    Optional<String> namespacePrefix = Optional.empty();
    String namespace = "ns";
    String table = "table";

    DynamoAdmin admin = new DynamoAdmin(metadataManager, namespacePrefix);

    // Act
    admin.getTableMetadata(namespace, table);

    // Assert
    verify(metadataManager).getTableMetadata(namespace, table);
  }

  @Test
  public void getTableMetadata_ConstructedWithNamespacePrefix_ShouldBeCalledWithNamespacePrefix() {
    // Arrange
    Optional<String> namespacePrefix = Optional.of("prefix_");
    String namespace = "ns";
    String table = "table";

    DynamoAdmin admin = new DynamoAdmin(metadataManager, namespacePrefix);

    // Act
    admin.getTableMetadata(namespace, table);

    // Assert
    verify(metadataManager).getTableMetadata(namespacePrefix.get() + "_" + namespace, table);
  }
}
