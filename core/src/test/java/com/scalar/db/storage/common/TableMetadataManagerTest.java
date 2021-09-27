package com.scalar.db.storage.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Get;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
public class TableMetadataManagerTest {

  @Mock private DistributedStorageAdmin admin;
  @Mock DatabaseConfig config;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void getTableMetadata_CalledOnce_ShouldCallDistributedStorageAdminOnce()
      throws ExecutionException {
    // Arrange
    TableMetadataManager tableMetadataManager = new TableMetadataManager(admin, config);

    TableMetadata expectedTableMetadata =
        TableMetadata.newBuilder()
            .addColumn("c1", DataType.INT)
            .addColumn("c2", DataType.INT)
            .addPartitionKey("c1")
            .build();

    when(admin.getTableMetadata(anyString(), anyString())).thenReturn(expectedTableMetadata);

    // Act
    TableMetadata actualTableMetadata =
        tableMetadataManager.getTableMetadata(
            new Get(new Key("c1", "aaa")).forNamespace("ns").forTable("tbl"));

    // Assert
    verify(admin).getTableMetadata(anyString(), anyString());
    assertThat(actualTableMetadata).isEqualTo(expectedTableMetadata);
  }

  @Test
  public void getTableMetadata_CalledTwice_ShouldCallDistributedStorageAdminOnlyOnce()
      throws ExecutionException {
    // Arrange
    TableMetadataManager tableMetadataManager = new TableMetadataManager(admin, config);

    TableMetadata expectedTableMetadata =
        TableMetadata.newBuilder()
            .addColumn("c1", DataType.INT)
            .addColumn("c2", DataType.INT)
            .addPartitionKey("c1")
            .build();

    when(admin.getTableMetadata(anyString(), anyString())).thenReturn(expectedTableMetadata);

    Get get = new Get(new Key("c1", "aaa")).forNamespace("ns").forTable("tbl");

    // Act
    tableMetadataManager.getTableMetadata(get);
    TableMetadata actualTableMetadata = tableMetadataManager.getTableMetadata(get);

    // Assert
    verify(admin).getTableMetadata(anyString(), anyString());
    assertThat(actualTableMetadata).isEqualTo(expectedTableMetadata);
  }
}
