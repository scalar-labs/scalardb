package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Get;
import com.scalar.db.api.Operation;
import com.scalar.db.api.VirtualTableInfo;
import com.scalar.db.api.VirtualTableJoinType;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class VirtualTableInfoManagerTest {

  @Mock private DistributedStorageAdmin admin;

  private VirtualTableInfoManager manager;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  @Test
  public void getVirtualTableInfo_WithOperation_VirtualTableExists_ShouldReturnVirtualTableInfo()
      throws Exception {
    // Arrange
    manager = new VirtualTableInfoManager(admin, -1);
    VirtualTableInfo virtualTableInfo = createVirtualTableInfo();
    when(admin.getVirtualTableInfo("ns", "table")).thenReturn(Optional.of(virtualTableInfo));

    Operation operation =
        Get.newBuilder().namespace("ns").table("table").partitionKey(Key.ofInt("pk", 1)).build();

    // Act
    VirtualTableInfo result = manager.getVirtualTableInfo(operation);

    // Assert
    assertThat(result).isNotNull();
    assertThat(result.getNamespaceName()).isEqualTo("ns");
    assertThat(result.getTableName()).isEqualTo("table");
    verify(admin).getVirtualTableInfo("ns", "table");
  }

  @Test
  public void getVirtualTableInfo_WithOperation_OperationDoesNotHaveNamespace_ShouldThrowException()
      throws Exception {
    // Arrange
    manager = new VirtualTableInfoManager(admin, -1);
    Operation operation = mock(Operation.class);
    when(operation.forNamespace()).thenReturn(Optional.empty());
    when(operation.forTable()).thenReturn(Optional.of("table"));

    // Act Assert
    assertThatThrownBy(() -> manager.getVirtualTableInfo(operation))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not have");
  }

  @Test
  public void getVirtualTableInfo_WithOperation_OperationDoesNotHaveTable_ShouldThrowException()
      throws Exception {
    // Arrange
    manager = new VirtualTableInfoManager(admin, -1);
    Operation operation = mock(Operation.class);
    when(operation.forNamespace()).thenReturn(Optional.of("ns"));
    when(operation.forTable()).thenReturn(Optional.empty());

    // Act Assert
    assertThatThrownBy(() -> manager.getVirtualTableInfo(operation))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not have");
  }

  @Test
  public void getVirtualTableInfo_VirtualTableExists_ShouldReturnVirtualTableInfo()
      throws Exception {
    // Arrange
    manager = new VirtualTableInfoManager(admin, -1);
    VirtualTableInfo virtualTableInfo = createVirtualTableInfo();
    when(admin.getVirtualTableInfo("ns", "table")).thenReturn(Optional.of(virtualTableInfo));

    // Act
    VirtualTableInfo result = manager.getVirtualTableInfo("ns", "table");

    // Assert
    assertThat(result).isNotNull();
    assertThat(result.getNamespaceName()).isEqualTo("ns");
    assertThat(result.getTableName()).isEqualTo("table");
    assertThat(result.getLeftSourceNamespaceName()).isEqualTo("left_ns");
    assertThat(result.getLeftSourceTableName()).isEqualTo("left_table");
    assertThat(result.getRightSourceNamespaceName()).isEqualTo("right_ns");
    assertThat(result.getRightSourceTableName()).isEqualTo("right_table");
    assertThat(result.getJoinType()).isEqualTo(VirtualTableJoinType.INNER);
    verify(admin).getVirtualTableInfo("ns", "table");
  }

  @Test
  public void getVirtualTableInfo_VirtualTableDoesNotExist_ShouldReturnNull() throws Exception {
    // Arrange
    manager = new VirtualTableInfoManager(admin, -1);
    when(admin.getVirtualTableInfo("ns", "table")).thenReturn(Optional.empty());

    // Act
    VirtualTableInfo result = manager.getVirtualTableInfo("ns", "table");

    // Assert
    assertThat(result).isNull();
    verify(admin).getVirtualTableInfo("ns", "table");
  }

  @Test
  public void getVirtualTableInfo_AdminThrowsRuntimeException_ShouldThrowRuntimeException()
      throws Exception {
    // Arrange
    manager = new VirtualTableInfoManager(admin, -1);
    when(admin.getVirtualTableInfo("ns", "table"))
        .thenThrow(new IllegalArgumentException("Table does not exist"));

    // Act Assert
    assertThatThrownBy(() -> manager.getVirtualTableInfo("ns", "table"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Table does not exist");
  }

  @Test
  public void getVirtualTableInfo_AdminThrowsExecutionException_ShouldWrapInExecutionException()
      throws Exception {
    // Arrange
    manager = new VirtualTableInfoManager(admin, -1);
    when(admin.getVirtualTableInfo("ns", "table"))
        .thenThrow(new ExecutionException("Storage execution error"));

    // Act Assert
    assertThatThrownBy(() -> manager.getVirtualTableInfo("ns", "table"))
        .isInstanceOf(ExecutionException.class)
        .hasMessageContaining("Getting the virtual table information failed")
        .hasMessageContaining("ns.table");
  }

  @Test
  public void getVirtualTableInfo_CalledMultipleTimes_ShouldUseCache() throws Exception {
    // Arrange
    manager = new VirtualTableInfoManager(admin, -1);
    VirtualTableInfo virtualTableInfo = createVirtualTableInfo();
    when(admin.getVirtualTableInfo("ns", "table")).thenReturn(Optional.of(virtualTableInfo));

    // Act
    VirtualTableInfo result1 = manager.getVirtualTableInfo("ns", "table");
    VirtualTableInfo result2 = manager.getVirtualTableInfo("ns", "table");
    VirtualTableInfo result3 = manager.getVirtualTableInfo("ns", "table");

    // Assert
    assertThat(result1).isNotNull();
    assertThat(result2).isNotNull();
    assertThat(result3).isNotNull();
    verify(admin, times(1)).getVirtualTableInfo("ns", "table");
  }

  @Test
  public void getVirtualTableInfo_WithCacheExpiration_ShouldExpireCache() throws Exception {
    // Arrange
    manager = new VirtualTableInfoManager(admin, 1); // 1 second expiration
    VirtualTableInfo virtualTableInfo = createVirtualTableInfo();
    when(admin.getVirtualTableInfo(anyString(), anyString()))
        .thenReturn(Optional.of(virtualTableInfo));

    // Act
    manager.getVirtualTableInfo("ns", "table");
    Thread.sleep(1100); // Wait for cache to expire
    manager.getVirtualTableInfo("ns", "table");

    // Assert
    verify(admin, times(2)).getVirtualTableInfo("ns", "table");
  }

  private VirtualTableInfo createVirtualTableInfo() {
    return new VirtualTableInfo() {
      @Override
      public String getNamespaceName() {
        return "ns";
      }

      @Override
      public String getTableName() {
        return "table";
      }

      @Override
      public String getLeftSourceNamespaceName() {
        return "left_ns";
      }

      @Override
      public String getLeftSourceTableName() {
        return "left_table";
      }

      @Override
      public String getRightSourceNamespaceName() {
        return "right_ns";
      }

      @Override
      public String getRightSourceTableName() {
        return "right_table";
      }

      @Override
      public VirtualTableJoinType getJoinType() {
        return VirtualTableJoinType.INNER;
      }
    };
  }
}
