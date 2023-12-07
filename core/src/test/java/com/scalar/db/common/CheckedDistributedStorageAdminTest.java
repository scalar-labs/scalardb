package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.Collections;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CheckedDistributedStorageAdminTest {

  private static final String SYSTEM_NAMESPACE = "scalardb";

  @Mock private DistributedStorageAdmin admin;
  @Mock private DatabaseConfig databaseConfig;

  private CheckedDistributedStorageAdmin checkedAdmin;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    when(databaseConfig.getSystemNamespaceName()).thenReturn(SYSTEM_NAMESPACE);
    checkedAdmin = new CheckedDistributedStorageAdmin(admin, databaseConfig);
  }

  @Test
  public void createNamespace_SystemNamespaceNameGiven_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> checkedAdmin.createNamespace(SYSTEM_NAMESPACE))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void dropNamespace_SystemNamespaceNameGiven_ShouldThrowIllegalArgumentException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> checkedAdmin.dropNamespace(SYSTEM_NAMESPACE))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void namespaceExists_SystemNamespaceNameGiven_ShouldReturnTrue()
      throws ExecutionException {
    // Arrange

    // Act
    boolean actual = checkedAdmin.namespaceExists(SYSTEM_NAMESPACE);

    // Assert
    assertThat(actual).isTrue();
  }

  @Test
  public void getNamespaceNames_ShouldReturnListWithSystemNamespaceName()
      throws ExecutionException {
    // Arrange
    when(admin.getNamespaceNames()).thenReturn(Collections.emptySet());

    // Act
    Set<String> actual = checkedAdmin.getNamespaceNames();

    // Assert
    assertThat(actual).containsExactly(SYSTEM_NAMESPACE);
  }
}
