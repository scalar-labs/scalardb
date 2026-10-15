package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AdminTest {

  private static final String NAMESPACE = "ns";
  private static final String TABLE = "tbl";

  private Admin admin;

  @BeforeEach
  void setUp() {
    // Use the real default methods so that the default tableExists() is tested
    admin = mock(Admin.class, CALLS_REAL_METHODS);
  }

  @Test
  void tableExists_WhenTableMetadataExists_ShouldReturnTrue() throws ExecutionException {
    // Arrange
    TableMetadata metadata =
        TableMetadata.newBuilder().addColumn("c1", DataType.INT).addPartitionKey("c1").build();
    doReturn(metadata).when(admin).getTableMetadata(NAMESPACE, TABLE);

    // Act
    boolean actual = admin.tableExists(NAMESPACE, TABLE);

    // Assert
    assertThat(actual).isTrue();
    verify(admin, never()).getNamespaceTableNames(anyString());
  }

  @Test
  void tableExists_WhenTableMetadataDoesNotExist_ShouldReturnFalse() throws ExecutionException {
    // Arrange
    doReturn(null).when(admin).getTableMetadata(NAMESPACE, TABLE);

    // Act
    boolean actual = admin.tableExists(NAMESPACE, TABLE);

    // Assert
    assertThat(actual).isFalse();
    verify(admin, never()).getNamespaceTableNames(anyString());
  }

  @Test
  void tableExists_WhenGettingTableMetadataFails_ShouldThrowExecutionException()
      throws ExecutionException {
    // Arrange
    ExecutionException exception = new ExecutionException("error");
    doThrow(exception).when(admin).getTableMetadata(NAMESPACE, TABLE);

    // Act Assert
    assertThatThrownBy(() -> admin.tableExists(NAMESPACE, TABLE)).isSameAs(exception);
  }
}
