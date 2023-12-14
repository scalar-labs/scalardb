package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Get;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.util.ThrowableFunction;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TableMetadataManagerTest {

  @Mock private DistributedStorageAdmin admin;

  @Mock
  private ThrowableFunction<TableMetadataManager.TableKey, Optional<TableMetadata>, Exception>
      getTableMetadataFunc;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  @Test
  public void getTableMetadata_CalledOnce_ShouldCallDistributedStorageAdminOnce()
      throws ExecutionException {
    // Arrange
    TableMetadataManager tableMetadataManager = new TableMetadataManager(admin, -1);

    TableMetadata expectedTableMetadata =
        TableMetadata.newBuilder()
            .addColumn("c1", DataType.INT)
            .addColumn("c2", DataType.INT)
            .addPartitionKey("c1")
            .build();

    when(admin.getTableMetadata("ns", "tbl")).thenReturn(expectedTableMetadata);

    Get get =
        Get.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofText("c1", "aaa")).build();

    // Act
    TableMetadata actualTableMetadata = tableMetadataManager.getTableMetadata(get);

    // Assert
    verify(admin).getTableMetadata("ns", "tbl");
    assertThat(actualTableMetadata).isEqualTo(expectedTableMetadata);
  }

  @Test
  public void getTableMetadata_CalledTwice_ShouldCallDistributedStorageAdminOnlyOnce()
      throws ExecutionException {
    // Arrange
    TableMetadataManager tableMetadataManager = new TableMetadataManager(admin, -1);

    TableMetadata expectedTableMetadata =
        TableMetadata.newBuilder()
            .addColumn("c1", DataType.INT)
            .addColumn("c2", DataType.INT)
            .addPartitionKey("c1")
            .build();

    when(admin.getTableMetadata("ns", "tbl")).thenReturn(expectedTableMetadata);

    Get get =
        Get.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofText("c1", "aaa")).build();

    // Act
    tableMetadataManager.getTableMetadata(get);
    TableMetadata actualTableMetadata = tableMetadataManager.getTableMetadata(get);

    // Assert
    verify(admin).getTableMetadata("ns", "tbl");
    assertThat(actualTableMetadata).isEqualTo(expectedTableMetadata);
  }

  @Test
  public void getTableMetadata_CalledAfterCacheExpiration_ShouldCallDistributedStorageAdminAgain()
      throws ExecutionException {
    // Arrange
    TableMetadataManager tableMetadataManager = new TableMetadataManager(admin, 1L); // one second

    TableMetadata expectedTableMetadata =
        TableMetadata.newBuilder()
            .addColumn("c1", DataType.INT)
            .addColumn("c2", DataType.INT)
            .addPartitionKey("c1")
            .build();

    when(admin.getTableMetadata("ns", "tbl")).thenReturn(expectedTableMetadata);

    Get get =
        Get.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofText("c1", "aaa")).build();

    // Act
    tableMetadataManager.getTableMetadata(get);
    // Wait for cache to be expired
    Uninterruptibles.sleepUninterruptibly(1200, TimeUnit.MILLISECONDS);
    TableMetadata actualTableMetadata = tableMetadataManager.getTableMetadata(get);

    // Assert
    verify(admin, times(2)).getTableMetadata("ns", "tbl");
    assertThat(actualTableMetadata).isEqualTo(expectedTableMetadata);
  }

  @Test
  public void getTableMetadata_ExecutionExceptionThrownByAdmin_ShouldThrowExecutionException()
      throws ExecutionException {
    // Arrange
    TableMetadataManager tableMetadataManager = new TableMetadataManager(admin, 1L); // one second

    when(admin.getTableMetadata("ns", "tbl")).thenThrow(ExecutionException.class);

    Get get =
        Get.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofText("c1", "aaa")).build();

    // Act
    assertThatThrownBy(() -> tableMetadataManager.getTableMetadata(get))
        .isInstanceOf(ExecutionException.class);

    // Assert
    verify(admin).getTableMetadata("ns", "tbl");
  }

  @Test
  public void
      getTableMetadata_WithGetTableMetadataFunc_CalledOnce_ShouldCallDistributedStorageAdminOnce()
          throws Exception {
    // Arrange
    TableMetadataManager tableMetadataManager = new TableMetadataManager(getTableMetadataFunc, -1);

    TableMetadata expectedTableMetadata =
        TableMetadata.newBuilder()
            .addColumn("c1", DataType.INT)
            .addColumn("c2", DataType.INT)
            .addPartitionKey("c1")
            .build();

    when(getTableMetadataFunc.apply(new TableMetadataManager.TableKey("ns", "tbl")))
        .thenReturn(Optional.of(expectedTableMetadata));

    Get get =
        Get.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofText("c1", "aaa")).build();

    // Act
    TableMetadata actualTableMetadata = tableMetadataManager.getTableMetadata(get);

    // Assert
    verify(getTableMetadataFunc).apply(new TableMetadataManager.TableKey("ns", "tbl"));
    assertThat(actualTableMetadata).isEqualTo(expectedTableMetadata);
  }

  @Test
  public void
      getTableMetadata_WithGetTableMetadataFunc_CalledTwice_ShouldCallDistributedStorageAdminOnlyOnce()
          throws Exception {
    // Arrange
    TableMetadataManager tableMetadataManager = new TableMetadataManager(getTableMetadataFunc, -1);

    TableMetadata expectedTableMetadata =
        TableMetadata.newBuilder()
            .addColumn("c1", DataType.INT)
            .addColumn("c2", DataType.INT)
            .addPartitionKey("c1")
            .build();

    when(getTableMetadataFunc.apply(new TableMetadataManager.TableKey("ns", "tbl")))
        .thenReturn(Optional.of(expectedTableMetadata));

    Get get =
        Get.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofText("c1", "aaa")).build();

    // Act
    tableMetadataManager.getTableMetadata(get);
    TableMetadata actualTableMetadata = tableMetadataManager.getTableMetadata(get);

    // Assert
    verify(getTableMetadataFunc).apply(new TableMetadataManager.TableKey("ns", "tbl"));
    assertThat(actualTableMetadata).isEqualTo(expectedTableMetadata);
  }

  @Test
  public void
      getTableMetadata_WithGetTableMetadataFunc_CalledAfterCacheExpiration_ShouldCallDistributedStorageAdminAgain()
          throws Exception {
    // Arrange
    TableMetadataManager tableMetadataManager =
        new TableMetadataManager(getTableMetadataFunc, 1L); // one second

    TableMetadata expectedTableMetadata =
        TableMetadata.newBuilder()
            .addColumn("c1", DataType.INT)
            .addColumn("c2", DataType.INT)
            .addPartitionKey("c1")
            .build();

    when(getTableMetadataFunc.apply(new TableMetadataManager.TableKey("ns", "tbl")))
        .thenReturn(Optional.of(expectedTableMetadata));

    Get get =
        Get.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofText("c1", "aaa")).build();

    // Act
    tableMetadataManager.getTableMetadata(get);
    // Wait for cache to be expired
    Uninterruptibles.sleepUninterruptibly(1200, TimeUnit.MILLISECONDS);
    TableMetadata actualTableMetadata = tableMetadataManager.getTableMetadata(get);

    // Assert
    verify(getTableMetadataFunc, times(2)).apply(new TableMetadataManager.TableKey("ns", "tbl"));
    assertThat(actualTableMetadata).isEqualTo(expectedTableMetadata);
  }

  @Test
  public void getTableMetadata_ExceptionThrownByGetTableMetadataFunc_ShouldThrowExecutionException()
      throws Exception {
    // Arrange
    TableMetadataManager tableMetadataManager =
        new TableMetadataManager(getTableMetadataFunc, 1L); // one second

    when(getTableMetadataFunc.apply(new TableMetadataManager.TableKey("ns", "tbl")))
        .thenThrow(Exception.class);

    Get get =
        Get.newBuilder().namespace("ns").table("tbl").partitionKey(Key.ofText("c1", "aaa")).build();

    // Act
    assertThatThrownBy(() -> tableMetadataManager.getTableMetadata(get))
        .isInstanceOf(ExecutionException.class);

    // Assert
    verify(getTableMetadataFunc).apply(new TableMetadataManager.TableKey("ns", "tbl"));
  }
}
