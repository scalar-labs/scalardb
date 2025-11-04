package com.scalar.db.storage.objectstorage;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Put;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Key;
import java.util.Collections;
import java.util.LinkedHashSet;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ObjectStorageMutationTest {
  private static final String ANY_NAMESPACE_NAME = "namespace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_NAME_4 = "name4";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final int ANY_INT_1 = 1;
  private static final int ANY_INT_2 = 2;

  @Mock private TableMetadata metadata;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    when(metadata.getPartitionKeyNames())
        .thenReturn(new LinkedHashSet<>(Collections.singletonList(ANY_NAME_1)));
  }

  private Put preparePut() {
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = Key.ofText(ANY_NAME_2, ANY_TEXT_2);
    return Put.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .intValue(ANY_NAME_3, ANY_INT_1)
        .intValue(ANY_NAME_4, ANY_INT_2)
        .build();
  }

  private Delete prepareDelete() {
    Key partitionKey = Key.ofText(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = Key.ofText(ANY_NAME_2, ANY_TEXT_2);
    return Delete.newBuilder()
        .namespace(ANY_NAMESPACE_NAME)
        .table(ANY_TABLE_NAME)
        .partitionKey(partitionKey)
        .clusteringKey(clusteringKey)
        .build();
  }

  @Test
  public void makeRecord_PutGiven_ShouldReturnWithValues() {
    // Arrange
    Put put = preparePut();
    ObjectStorageMutation objectStorageMutation = new ObjectStorageMutation(put, metadata);
    String concatenatedKey = objectStorageMutation.getRecordId();

    // Act
    ObjectStorageRecord actual = objectStorageMutation.makeRecord();

    // Assert
    assertThat(actual.getId()).isEqualTo(concatenatedKey);
    Assertions.assertThat(actual.getPartitionKey().get(ANY_NAME_1)).isEqualTo(ANY_TEXT_1);
    Assertions.assertThat(actual.getClusteringKey().get(ANY_NAME_2)).isEqualTo(ANY_TEXT_2);
    Assertions.assertThat(actual.getValues().get(ANY_NAME_3)).isEqualTo(ANY_INT_1);
    Assertions.assertThat(actual.getValues().get(ANY_NAME_4)).isEqualTo(ANY_INT_2);
  }

  @Test
  public void makeRecord_PutWithNullValueGiven_ShouldReturnWithValues() {
    // Arrange
    Put put = preparePut();
    put = Put.newBuilder(put).intValue(ANY_NAME_3, null).build();
    ObjectStorageMutation objectStorageMutation = new ObjectStorageMutation(put, metadata);
    String concatenatedKey = objectStorageMutation.getRecordId();

    // Act
    ObjectStorageRecord actual = objectStorageMutation.makeRecord();

    // Assert
    assertThat(actual.getId()).isEqualTo(concatenatedKey);
    Assertions.assertThat(actual.getPartitionKey().get(ANY_NAME_1)).isEqualTo(ANY_TEXT_1);
    Assertions.assertThat(actual.getClusteringKey().get(ANY_NAME_2)).isEqualTo(ANY_TEXT_2);
    Assertions.assertThat(actual.getValues().containsKey(ANY_NAME_3)).isTrue();
    Assertions.assertThat(actual.getValues().get(ANY_NAME_3)).isNull();
    Assertions.assertThat(actual.getValues().get(ANY_NAME_4)).isEqualTo(ANY_INT_2);
  }

  @Test
  public void makeRecord_DeleteGiven_ShouldReturnEmpty() {
    // Arrange
    Delete delete = prepareDelete();
    ObjectStorageMutation objectStorageMutation = new ObjectStorageMutation(delete, metadata);

    // Act
    ObjectStorageRecord actual = objectStorageMutation.makeRecord();

    // Assert
    assertThat(actual.getId()).isEqualTo("");
  }
}
