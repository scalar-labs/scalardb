package com.scalar.db.storage.objectstorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Get;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Key;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ObjectStorageOperationTest {
  private static final String ANY_NAMESPACE_NAME = "namespace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final int ANY_INT_1 = 1;

  @Mock private TableMetadata metadata;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  @Test
  public void checkArgument_WrongOperationGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Operation operation = mock(Put.class);
    ObjectStorageOperation objectStorageOperation = new ObjectStorageOperation(operation, metadata);

    // Act Assert
    assertThatThrownBy(() -> objectStorageOperation.checkArgument(Get.class))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void getConcatenatedPartitionKey_MultipleKeysGiven_ShouldReturnConcatenatedPartitionKey() {
    // Arrange
    when(metadata.getPartitionKeyNames())
        .thenReturn(new LinkedHashSet<>(Arrays.asList(ANY_NAME_1, ANY_NAME_2, ANY_NAME_3)));

    Key partitionKey =
        Key.of(ANY_NAME_1, ANY_TEXT_1, ANY_NAME_2, ANY_TEXT_2, ANY_NAME_3, ANY_INT_1);
    Get get =
        Get.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(partitionKey)
            .build();
    ObjectStorageOperation objectStorageOperation = new ObjectStorageOperation(get, metadata);

    // Act
    String actual = objectStorageOperation.getConcatenatedPartitionKey();

    // Assert
    assertThat(actual)
        .isEqualTo(
            String.join(
                String.valueOf(ObjectStorageUtils.CONCATENATED_KEY_DELIMITER),
                ANY_TEXT_1,
                ANY_TEXT_2,
                String.valueOf(ANY_INT_1)));
  }

  @Test
  public void getId_MultipleKeysGiven_ShouldReturnConcatenatedPartitionKey() {
    // Arrange
    when(metadata.getPartitionKeyNames())
        .thenReturn(new LinkedHashSet<>(Arrays.asList(ANY_NAME_1, ANY_NAME_3)));
    when(metadata.getClusteringKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList(ANY_NAME_2)));

    Key partitionKey = Key.of(ANY_NAME_1, ANY_TEXT_1, ANY_NAME_3, ANY_INT_1);
    Key clusteringKey = Key.ofText(ANY_NAME_2, ANY_TEXT_2);
    Get get =
        Get.newBuilder()
            .namespace(ANY_NAMESPACE_NAME)
            .table(ANY_TABLE_NAME)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .build();
    ObjectStorageOperation objectStorageOperation = new ObjectStorageOperation(get, metadata);

    // Act
    String actual = objectStorageOperation.getRecordId();

    // Assert
    assertThat(actual)
        .isEqualTo(
            String.join(
                String.valueOf(ObjectStorageUtils.CONCATENATED_KEY_DELIMITER),
                ANY_TEXT_1,
                String.valueOf(ANY_INT_1),
                ANY_TEXT_2));
  }
}
