package com.scalar.db.storage.multistorage;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class MultiStorageAdminTest {

  protected static final String NAMESPACE1 = "test_ns1";
  protected static final String NAMESPACE2 = "test_ns2";
  protected static final String TABLE1 = "test_table1";
  protected static final String TABLE2 = "test_table2";
  protected static final String TABLE3 = "test_table3";

  @Mock private DistributedStorageAdmin admin1;
  @Mock private DistributedStorageAdmin admin2;
  @Mock private DistributedStorageAdmin admin3;

  private MultiStorageAdmin multiStorageAdmin;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    // Arrange
    Map<String, DistributedStorageAdmin> tableAdminMap = new HashMap<>();
    tableAdminMap.put(NAMESPACE1 + "." + TABLE1, admin1);
    tableAdminMap.put(NAMESPACE1 + "." + TABLE2, admin2);
    Map<String, DistributedStorageAdmin> namespaceAdminMap = new HashMap<>();
    namespaceAdminMap.put(NAMESPACE2, admin2);
    DistributedStorageAdmin defaultAdmin = admin3;
    multiStorageAdmin = new MultiStorageAdmin(tableAdminMap, namespaceAdminMap, defaultAdmin);
  }

  @Test
  public void getTableMetadata_ForTable1_ShouldReturnMetadataFromAdmin1()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE1;

    // Act
    multiStorageAdmin.getTableMetadata(namespace, table);

    // Assert
    verify(admin1).getTableMetadata(any(), any());
  }

  @Test
  public void getTableMetadata_ForTable2_ShouldReturnMetadataFromAdmin2()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE2;

    // Act
    multiStorageAdmin.getTableMetadata(namespace, table);

    // Assert
    verify(admin2).getTableMetadata(any(), any());
  }

  @Test
  public void getTableMetadata_ForTable3_ShouldReturnMetadataFromDefaultAdmin()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE3;

    // Act
    multiStorageAdmin.getTableMetadata(namespace, table);

    // Assert
    verify(admin3).getTableMetadata(any(), any());
  }

  @Test
  public void getTableMetadata_ForTable1InNamespace2_ShouldReturnMetadataFromAdmin2()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE2;
    String table = TABLE1;

    // Act
    multiStorageAdmin.getTableMetadata(namespace, table);

    // Assert
    verify(admin2).getTableMetadata(any(), any());
  }
}
