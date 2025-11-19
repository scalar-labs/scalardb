package com.scalar.db.storage.objectstorage.cloudstorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.paging.Page;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageBatch;
import com.google.cloud.storage.StorageBatchResult;
import com.google.cloud.storage.StorageException;
import com.scalar.db.storage.objectstorage.ObjectStorageWrapperException;
import com.scalar.db.storage.objectstorage.ObjectStorageWrapperResponse;
import com.scalar.db.storage.objectstorage.PreconditionFailedException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CloudStorageWrapperTest {
  private static final String METADATA_NAMESPACE = "scalardb";
  private static final String PROJECT_ID = "project-id";
  private static final String BUCKET = "bucket";
  private static final String ANY_OBJECT_KEY = "any-object-key";
  private static final String ANY_PREFIX = "any-prefix/";
  private static final String ANY_DATA = "any-data";
  private static final long ANY_GENERATION = 12345L;

  @Mock private CloudStorageConfig config;
  @Mock private Storage storage;
  private CloudStorageWrapper wrapper;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    when(config.getMetadataNamespace()).thenReturn(METADATA_NAMESPACE);
    when(config.getProjectId()).thenReturn(PROJECT_ID);
    when(config.getBucket()).thenReturn(BUCKET);
    when(config.getParallelUploadBlockSizeInBytes()).thenReturn(Optional.empty());
    wrapper = new CloudStorageWrapper(config, storage);
  }

  @Test
  public void get_ExistingObjectKeyGiven_ShouldReturnObjectData() throws Exception {
    // Arrange
    BlobId blobId = BlobId.of(BUCKET, ANY_OBJECT_KEY);
    Blob blob = mock(Blob.class);
    when(storage.get(blobId)).thenReturn(blob);
    when(blob.getContent()).thenReturn(ANY_DATA.getBytes(StandardCharsets.UTF_8));
    when(blob.getGeneration()).thenReturn(ANY_GENERATION);

    // Act
    Optional<ObjectStorageWrapperResponse> result = wrapper.get(ANY_OBJECT_KEY);

    // Assert
    verify(storage).get(blobId);
    assertThat(result).isPresent();
    assertThat(result.get().getPayload()).isEqualTo(ANY_DATA);
    assertThat(result.get().getVersion()).isEqualTo(String.valueOf(ANY_GENERATION));
  }

  @Test
  public void get_NonExistingObjectKeyGiven_ShouldReturnEmptyOptional() throws Exception {
    // Arrange
    BlobId blobId = BlobId.of(BUCKET, ANY_OBJECT_KEY);
    when(storage.get(blobId)).thenReturn(null);

    // Act
    Optional<ObjectStorageWrapperResponse> result = wrapper.get(ANY_OBJECT_KEY);

    // Assert
    assertThat(result).isNotPresent();
  }

  @Test
  public void get_StorageExceptionThrown_ShouldThrowObjectStorageWrapperException() {
    // Arrange
    BlobId blobId = BlobId.of(BUCKET, ANY_OBJECT_KEY);
    when(storage.get(blobId)).thenThrow(new StorageException(500, "Any Error"));

    // Act Assert
    assertThatCode(() -> wrapper.get(ANY_OBJECT_KEY))
        .isInstanceOf(ObjectStorageWrapperException.class);
  }

  @Test
  public void getKeys_PrefixGiven_ShouldReturnObjectKeys() throws Exception {
    // Arrange
    String objectKey1 = ANY_PREFIX + "test1/object1";
    String objectKey2 = ANY_PREFIX + "test1/object2";
    String objectKey3 = ANY_PREFIX + "test2/object3";

    Blob blob1 = mock(Blob.class);
    Blob blob2 = mock(Blob.class);
    Blob blob3 = mock(Blob.class);
    when(blob1.getName()).thenReturn(objectKey1);
    when(blob2.getName()).thenReturn(objectKey2);
    when(blob3.getName()).thenReturn(objectKey3);

    @SuppressWarnings("unchecked")
    Page<Blob> page = mock(Page.class);
    when(storage.list(eq(BUCKET), any(Storage.BlobListOption.class))).thenReturn(page);
    when(page.iterateAll()).thenReturn(Arrays.asList(blob1, blob2, blob3));

    // Act
    Set<String> result = wrapper.getKeys(ANY_PREFIX);

    // Assert
    assertThat(result).containsExactlyInAnyOrder(objectKey1, objectKey2, objectKey3);
  }

  @Test
  public void getKeys_NoObjectsWithPrefix_ShouldReturnEmptySet() throws Exception {
    // Arrange
    @SuppressWarnings("unchecked")
    Page<Blob> page = mock(Page.class);
    when(storage.list(eq(BUCKET), any(Storage.BlobListOption.class))).thenReturn(page);
    when(page.iterateAll()).thenReturn(Collections.emptyList());

    // Act
    Set<String> result = wrapper.getKeys(ANY_PREFIX);

    // Assert
    assertThat(result).isEmpty();
  }

  @Test
  public void getKeys_StorageExceptionThrown_ShouldThrowObjectStorageWrapperException() {
    // Arrange
    when(storage.list(eq(BUCKET), any(Storage.BlobListOption.class)))
        .thenThrow(new StorageException(500, "Any Error"));

    // Act Assert
    assertThatCode(() -> wrapper.getKeys(ANY_PREFIX))
        .isInstanceOf(ObjectStorageWrapperException.class);
  }

  @Test
  public void insert_NonExistingObjectKeyGiven_ShouldInsertObject() throws Exception {
    // Arrange
    WriteChannel writeChannel = mock(WriteChannel.class);
    doAnswer(
            invocation -> {
              ByteBuffer buffer = invocation.getArgument(0);
              int remaining = buffer.remaining();
              buffer.position(buffer.limit());
              return remaining;
            })
        .when(writeChannel)
        .write(any(ByteBuffer.class));
    when(storage.writer(any(BlobInfo.class), any(Storage.BlobWriteOption.class)))
        .thenReturn(writeChannel);

    // Act
    wrapper.insert(ANY_OBJECT_KEY, ANY_DATA);

    // Assert
    verify(storage).writer(any(BlobInfo.class), any(Storage.BlobWriteOption.class));
  }

  @Test
  public void insert_PreconditionFailed_ShouldThrowPreconditionFailedException() {
    // Arrange
    when(storage.writer(any(BlobInfo.class), any(Storage.BlobWriteOption.class)))
        .thenThrow(new StorageException(412, "Any Error"));

    // Act Assert
    assertThatCode(() -> wrapper.insert(ANY_OBJECT_KEY, ANY_DATA))
        .isInstanceOf(PreconditionFailedException.class);
  }

  @Test
  public void insert_OtherStorageExceptionThrown_ShouldThrowObjectStorageWrapperException() {
    // Arrange
    when(storage.writer(any(BlobInfo.class), any(Storage.BlobWriteOption.class)))
        .thenThrow(new StorageException(500, "Any Error"));

    // Act Assert
    assertThatCode(() -> wrapper.insert(ANY_OBJECT_KEY, ANY_DATA))
        .isInstanceOf(ObjectStorageWrapperException.class);
  }

  @Test
  public void update_ExistingObjectKeyGiven_ShouldUpdateObject() throws Exception {
    // Arrange
    WriteChannel writeChannel = mock(WriteChannel.class);
    doAnswer(
            invocation -> {
              ByteBuffer buffer = invocation.getArgument(0);
              int remaining = buffer.remaining();
              buffer.position(buffer.limit());
              return remaining;
            })
        .when(writeChannel)
        .write(any(ByteBuffer.class));
    when(storage.writer(any(BlobInfo.class), any(Storage.BlobWriteOption.class)))
        .thenReturn(writeChannel);

    // Act
    wrapper.update(ANY_OBJECT_KEY, ANY_DATA, String.valueOf(ANY_GENERATION));

    // Assert
    verify(storage).writer(any(BlobInfo.class), any(Storage.BlobWriteOption.class));
  }

  @Test
  public void update_PreconditionFailed_ShouldThrowPreconditionFailedException() {
    // Arrange
    when(storage.writer(any(BlobInfo.class), any(Storage.BlobWriteOption.class)))
        .thenThrow(new StorageException(412, "Any Error"));

    // Act Assert
    assertThatCode(() -> wrapper.update(ANY_OBJECT_KEY, ANY_DATA, String.valueOf(ANY_GENERATION)))
        .isInstanceOf(PreconditionFailedException.class);
  }

  @Test
  public void update_OtherStorageExceptionThrown_ShouldThrowObjectStorageWrapperException() {
    // Arrange
    when(storage.writer(any(BlobInfo.class), any(Storage.BlobWriteOption.class)))
        .thenThrow(new StorageException(500, "Any Error"));

    // Act Assert
    assertThatCode(() -> wrapper.update(ANY_OBJECT_KEY, ANY_DATA, String.valueOf(ANY_GENERATION)))
        .isInstanceOf(ObjectStorageWrapperException.class);
  }

  @Test
  public void delete_ExistingObjectKeyGiven_ShouldDeleteObject() throws Exception {
    // Arrange
    BlobId blobId = BlobId.of(BUCKET, ANY_OBJECT_KEY);
    when(storage.delete(blobId)).thenReturn(true);

    // Act
    wrapper.delete(ANY_OBJECT_KEY);

    // Assert
    verify(storage).delete(blobId);
  }

  @Test
  public void delete_NonExistingObjectKeyGiven_ShouldThrowPreconditionFailedException() {
    // Arrange
    BlobId blobId = BlobId.of(BUCKET, ANY_OBJECT_KEY);
    when(storage.delete(blobId)).thenReturn(false);

    // Act Assert
    assertThatCode(() -> wrapper.delete(ANY_OBJECT_KEY))
        .isInstanceOf(PreconditionFailedException.class);
  }

  @Test
  public void delete_OtherStorageExceptionThrown_ShouldThrowObjectStorageWrapperException() {
    // Arrange
    BlobId blobId = BlobId.of(BUCKET, ANY_OBJECT_KEY);
    when(storage.delete(blobId)).thenThrow(new StorageException(500, "Any Error"));

    // Act Assert
    assertThatCode(() -> wrapper.delete(ANY_OBJECT_KEY))
        .isInstanceOf(ObjectStorageWrapperException.class);
  }

  @Test
  public void delete_WithVersion_ExistingObjectKeyGiven_ShouldDeleteObject() throws Exception {
    // Arrange
    BlobId blobId = BlobId.of(BUCKET, ANY_OBJECT_KEY);
    when(storage.delete(eq(blobId), any(Storage.BlobSourceOption.class))).thenReturn(true);

    // Act
    wrapper.delete(ANY_OBJECT_KEY, String.valueOf(ANY_GENERATION));

    // Assert
    verify(storage).delete(eq(blobId), any(Storage.BlobSourceOption.class));
  }

  @Test
  public void
      delete_WithVersion_NonExistingObjectKeyGiven_ShouldThrowPreconditionFailedException() {
    // Arrange
    BlobId blobId = BlobId.of(BUCKET, ANY_OBJECT_KEY);
    when(storage.delete(eq(blobId), any(Storage.BlobSourceOption.class))).thenReturn(false);

    // Act Assert
    assertThatCode(() -> wrapper.delete(ANY_OBJECT_KEY, String.valueOf(ANY_GENERATION)))
        .isInstanceOf(PreconditionFailedException.class);
  }

  @Test
  public void delete_WithVersion_PreconditionFailed_ShouldThrowPreconditionFailedException() {
    // Arrange
    BlobId blobId = BlobId.of(BUCKET, ANY_OBJECT_KEY);
    when(storage.delete(eq(blobId), any(Storage.BlobSourceOption.class)))
        .thenThrow(new StorageException(412, "Any Error"));

    // Act Assert
    assertThatCode(() -> wrapper.delete(ANY_OBJECT_KEY, String.valueOf(ANY_GENERATION)))
        .isInstanceOf(PreconditionFailedException.class);
  }

  @Test
  public void
      delete_WithVersion_OtherStorageExceptionThrown_ShouldThrowObjectStorageWrapperException() {
    // Arrange
    BlobId blobId = BlobId.of(BUCKET, ANY_OBJECT_KEY);
    when(storage.delete(eq(blobId), any(Storage.BlobSourceOption.class)))
        .thenThrow(new StorageException(500, "Any Error"));

    // Act Assert
    assertThatCode(() -> wrapper.delete(ANY_OBJECT_KEY, String.valueOf(ANY_GENERATION)))
        .isInstanceOf(ObjectStorageWrapperException.class);
  }

  @Test
  public void deleteByPrefix_PrefixGiven_ShouldDeleteAllObjectsWithThePrefix() throws Exception {
    // Arrange
    String objectKey1 = ANY_PREFIX + "test1/object1";
    String objectKey2 = ANY_PREFIX + "test1/object2";
    String objectKey3 = ANY_PREFIX + "test2/object3";

    Blob blob1 = mock(Blob.class);
    Blob blob2 = mock(Blob.class);
    Blob blob3 = mock(Blob.class);
    when(blob1.getName()).thenReturn(objectKey1);
    when(blob2.getName()).thenReturn(objectKey2);
    when(blob3.getName()).thenReturn(objectKey3);

    @SuppressWarnings("unchecked")
    Page<Blob> page = mock(Page.class);
    when(storage.list(eq(BUCKET), any(Storage.BlobListOption.class))).thenReturn(page);
    when(page.iterateAll()).thenReturn(Arrays.asList(blob1, blob2, blob3));

    StorageBatch batch = mock(StorageBatch.class);
    when(storage.batch()).thenReturn(batch);
    @SuppressWarnings("unchecked")
    StorageBatchResult<Boolean> batchResult = mock(StorageBatchResult.class);
    doReturn(batchResult).when(batch).delete(any(BlobId.class));

    // Act
    wrapper.deleteByPrefix(ANY_PREFIX);

    // Assert
    verify(storage).batch();
    verify(batch).submit();
  }

  @Test
  public void deleteByPrefix_MultiplePagesGiven_ShouldDeleteAllObjectsAcrossPages()
      throws Exception {
    // Arrange
    String objectKey1 = ANY_PREFIX + "test1/object1";
    String objectKey2 = ANY_PREFIX + "test1/object2";
    String objectKey3 = ANY_PREFIX + "test2/object3";

    Blob blob1 = mock(Blob.class);
    Blob blob2 = mock(Blob.class);
    Blob blob3 = mock(Blob.class);
    when(blob1.getName()).thenReturn(objectKey1);
    when(blob2.getName()).thenReturn(objectKey2);
    when(blob3.getName()).thenReturn(objectKey3);

    // Mock with iterateAll() that returns all blobs across pages
    @SuppressWarnings("unchecked")
    Page<Blob> page = mock(Page.class);
    when(storage.list(eq(BUCKET), any(Storage.BlobListOption.class))).thenReturn(page);
    when(page.iterateAll()).thenReturn(Arrays.asList(blob1, blob2, blob3));

    StorageBatch batch = mock(StorageBatch.class);
    when(storage.batch()).thenReturn(batch);
    @SuppressWarnings("unchecked")
    StorageBatchResult<Boolean> batchResult = mock(StorageBatchResult.class);
    doReturn(batchResult).when(batch).delete(any(BlobId.class));

    // Act
    wrapper.deleteByPrefix(ANY_PREFIX);

    // Assert
    verify(storage).batch();
    verify(batch).submit();
  }

  @Test
  public void deleteByPrefix_NoObjectsWithPrefix_ShouldDoNothing() throws Exception {
    // Arrange
    @SuppressWarnings("unchecked")
    Page<Blob> page = mock(Page.class);
    when(storage.list(eq(BUCKET), any(Storage.BlobListOption.class))).thenReturn(page);
    when(page.iterateAll()).thenReturn(Collections.emptyList());

    // Act
    wrapper.deleteByPrefix(ANY_PREFIX);

    // Assert
    verify(storage, never()).batch();
  }

  @Test
  public void deleteByPrefix_StorageExceptionThrown_ShouldThrowObjectStorageWrapperException() {
    // Arrange
    when(storage.list(eq(BUCKET), any(Storage.BlobListOption.class)))
        .thenThrow(new StorageException(500, "Any Error"));

    // Act Assert
    assertThatCode(() -> wrapper.deleteByPrefix(ANY_PREFIX))
        .isInstanceOf(ObjectStorageWrapperException.class);
  }

  @Test
  public void close_ShouldCloseTheStorage() throws Exception {
    // Arrange

    // Act
    wrapper.close();

    // Assert
    verify(storage).close();
  }
}
