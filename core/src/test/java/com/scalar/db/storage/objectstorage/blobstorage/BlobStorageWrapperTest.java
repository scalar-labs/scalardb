package com.scalar.db.storage.objectstorage.blobstorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.rest.PagedIterable;
import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobDownloadContentResponse;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.scalar.db.storage.objectstorage.ObjectStorageWrapperException;
import com.scalar.db.storage.objectstorage.ObjectStorageWrapperResponse;
import com.scalar.db.storage.objectstorage.PreconditionFailedException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class BlobStorageWrapperTest {
  private static final String ANY_OBJECT_KEY = "any_object_key";
  private static final String ANY_PREFIX = "any_prefix/";
  private static final String ANY_DATA = "any_data";
  private static final String ANY_ETAG = "any_etag";

  @Mock private BlobStorageConfig config;
  @Mock private BlobContainerClient client;
  private BlobStorageWrapper wrapper;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    when(config.getRequestTimeoutSecs()).thenReturn(Optional.empty());
    when(config.getParallelUploadBlockSizeBytes()).thenReturn(Optional.empty());
    when(config.getParallelUploadMaxConcurrency()).thenReturn(Optional.empty());
    when(config.getParallelUploadThresholdSizeBytes()).thenReturn(Optional.empty());
    wrapper = new BlobStorageWrapper(config, client);
  }

  private BlobStorageException createBlobStorageException(BlobErrorCode errorCode) {
    HttpResponse httpResponse = mock(HttpResponse.class);
    HttpHeaders headers = new HttpHeaders().set("x-ms-error-code", errorCode.toString());
    when(httpResponse.getHeaders()).thenReturn(headers);
    return new BlobStorageException("test error", httpResponse, null);
  }

  // get tests

  @Test
  public void get_ExistingObjectKeyGiven_ShouldReturnObjectData() throws Exception {
    // Arrange
    BlobClient blobClient = mock(BlobClient.class);
    when(client.getBlobClient(ANY_OBJECT_KEY)).thenReturn(blobClient);

    BlobDownloadContentResponse response = mock(BlobDownloadContentResponse.class);
    when(blobClient.downloadContentWithResponse(any(), any(), any(), any())).thenReturn(response);
    when(response.getValue()).thenReturn(BinaryData.fromString(ANY_DATA));
    HttpHeaders headers = new HttpHeaders().set("ETag", ANY_ETAG);
    when(response.getHeaders()).thenReturn(headers);

    // Act
    Optional<ObjectStorageWrapperResponse> result = wrapper.get(ANY_OBJECT_KEY);

    // Assert
    verify(blobClient).downloadContentWithResponse(any(), any(), any(), any());
    assertThat(result).isPresent();
    assertThat(result.get().getPayload()).isEqualTo(ANY_DATA);
    assertThat(result.get().getVersion()).isEqualTo(ANY_ETAG);
  }

  @Test
  public void get_NonExistingObjectKeyGiven_ShouldReturnEmptyOptional() throws Exception {
    // Arrange
    BlobClient blobClient = mock(BlobClient.class);
    when(client.getBlobClient(ANY_OBJECT_KEY)).thenReturn(blobClient);

    BlobStorageException exception = createBlobStorageException(BlobErrorCode.BLOB_NOT_FOUND);
    when(blobClient.downloadContentWithResponse(any(), any(), any(), any())).thenThrow(exception);

    // Act
    Optional<ObjectStorageWrapperResponse> result = wrapper.get(ANY_OBJECT_KEY);

    // Assert
    assertThat(result).isNotPresent();
  }

  @Test
  public void get_OtherBlobStorageExceptionThrown_ShouldThrowObjectStorageWrapperException() {
    // Arrange
    BlobClient blobClient = mock(BlobClient.class);
    when(client.getBlobClient(ANY_OBJECT_KEY)).thenReturn(blobClient);

    BlobStorageException exception = createBlobStorageException(BlobErrorCode.INTERNAL_ERROR);
    when(blobClient.downloadContentWithResponse(any(), any(), any(), any())).thenThrow(exception);

    // Act & Assert
    assertThatCode(() -> wrapper.get(ANY_OBJECT_KEY))
        .isInstanceOf(ObjectStorageWrapperException.class);
  }

  // getKeys tests

  @Test
  @SuppressWarnings("unchecked")
  public void getKeys_PrefixGiven_ShouldReturnObjectKeys() throws Exception {
    // Arrange
    String objectKey1 = ANY_PREFIX + "test1/object1";
    String objectKey2 = ANY_PREFIX + "test1/object2";
    String objectKey3 = ANY_PREFIX + "test2/object3";

    BlobItem item1 = new BlobItem().setName(objectKey1);
    BlobItem item2 = new BlobItem().setName(objectKey2);
    BlobItem item3 = new BlobItem().setName(objectKey3);

    PagedIterable<BlobItem> pagedIterable = mock(PagedIterable.class);
    when(client.listBlobs(any(ListBlobsOptions.class), any())).thenReturn(pagedIterable);
    when(pagedIterable.stream()).thenReturn(Stream.of(item1, item2, item3));

    // Act
    Set<String> result = wrapper.getKeys(ANY_PREFIX);

    // Assert
    assertThat(result).containsExactlyInAnyOrder(objectKey1, objectKey2, objectKey3);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void getKeys_NoObjectsWithPrefix_ShouldReturnEmptySet() throws Exception {
    // Arrange
    PagedIterable<BlobItem> pagedIterable = mock(PagedIterable.class);
    when(client.listBlobs(any(ListBlobsOptions.class), any())).thenReturn(pagedIterable);
    when(pagedIterable.stream()).thenReturn(Stream.empty());

    // Act
    Set<String> result = wrapper.getKeys(ANY_PREFIX);

    // Assert
    assertThat(result).isEmpty();
  }

  @Test
  public void getKeys_ExceptionThrown_ShouldThrowObjectStorageWrapperException() {
    // Arrange
    when(client.listBlobs(any(ListBlobsOptions.class), any()))
        .thenThrow(new RuntimeException("test error"));

    // Act & Assert
    assertThatCode(() -> wrapper.getKeys(ANY_PREFIX))
        .isInstanceOf(ObjectStorageWrapperException.class);
  }

  // insert tests

  @Test
  public void insert_NonExistingObjectKeyGiven_ShouldCallClientUploadWithResponse()
      throws Exception {
    // Arrange
    BlobClient blobClient = mock(BlobClient.class);
    when(client.getBlobClient(ANY_OBJECT_KEY)).thenReturn(blobClient);

    // Act
    wrapper.insert(ANY_OBJECT_KEY, ANY_DATA);

    // Assert
    verify(blobClient).uploadWithResponse(any(BlobParallelUploadOptions.class), any(), any());
  }

  @Test
  public void insert_BlobAlreadyExists_ShouldThrowPreconditionFailedException() {
    // Arrange
    BlobClient blobClient = mock(BlobClient.class);
    when(client.getBlobClient(ANY_OBJECT_KEY)).thenReturn(blobClient);

    BlobStorageException exception = createBlobStorageException(BlobErrorCode.BLOB_ALREADY_EXISTS);
    when(blobClient.uploadWithResponse(any(BlobParallelUploadOptions.class), any(), any()))
        .thenThrow(exception);

    // Act & Assert
    assertThatCode(() -> wrapper.insert(ANY_OBJECT_KEY, ANY_DATA))
        .isInstanceOf(PreconditionFailedException.class);
  }

  @Test
  public void insert_OtherBlobStorageExceptionThrown_ShouldThrowObjectStorageWrapperException() {
    // Arrange
    BlobClient blobClient = mock(BlobClient.class);
    when(client.getBlobClient(ANY_OBJECT_KEY)).thenReturn(blobClient);

    BlobStorageException exception = createBlobStorageException(BlobErrorCode.INTERNAL_ERROR);
    when(blobClient.uploadWithResponse(any(BlobParallelUploadOptions.class), any(), any()))
        .thenThrow(exception);

    // Act & Assert
    assertThatCode(() -> wrapper.insert(ANY_OBJECT_KEY, ANY_DATA))
        .isInstanceOf(ObjectStorageWrapperException.class);
  }

  // update tests

  @Test
  public void update_ExistingObjectKeyGiven_ShouldCallClientUploadWithResponseWithETag()
      throws Exception {
    // Arrange
    BlobClient blobClient = mock(BlobClient.class);
    when(client.getBlobClient(ANY_OBJECT_KEY)).thenReturn(blobClient);

    // Act
    wrapper.update(ANY_OBJECT_KEY, ANY_DATA, ANY_ETAG);

    // Assert
    verify(blobClient).uploadWithResponse(any(BlobParallelUploadOptions.class), any(), any());
  }

  @Test
  public void update_ConditionNotMet_ShouldThrowPreconditionFailedException() {
    // Arrange
    BlobClient blobClient = mock(BlobClient.class);
    when(client.getBlobClient(ANY_OBJECT_KEY)).thenReturn(blobClient);

    BlobStorageException exception = createBlobStorageException(BlobErrorCode.CONDITION_NOT_MET);
    when(blobClient.uploadWithResponse(any(BlobParallelUploadOptions.class), any(), any()))
        .thenThrow(exception);

    // Act & Assert
    assertThatCode(() -> wrapper.update(ANY_OBJECT_KEY, ANY_DATA, ANY_ETAG))
        .isInstanceOf(PreconditionFailedException.class);
  }

  @Test
  public void update_BlobNotFound_ShouldThrowPreconditionFailedException() {
    // Arrange
    BlobClient blobClient = mock(BlobClient.class);
    when(client.getBlobClient(ANY_OBJECT_KEY)).thenReturn(blobClient);

    BlobStorageException exception = createBlobStorageException(BlobErrorCode.BLOB_NOT_FOUND);
    when(blobClient.uploadWithResponse(any(BlobParallelUploadOptions.class), any(), any()))
        .thenThrow(exception);

    // Act & Assert
    assertThatCode(() -> wrapper.update(ANY_OBJECT_KEY, ANY_DATA, ANY_ETAG))
        .isInstanceOf(PreconditionFailedException.class);
  }

  @Test
  public void update_OtherBlobStorageExceptionThrown_ShouldThrowObjectStorageWrapperException() {
    // Arrange
    BlobClient blobClient = mock(BlobClient.class);
    when(client.getBlobClient(ANY_OBJECT_KEY)).thenReturn(blobClient);

    BlobStorageException exception = createBlobStorageException(BlobErrorCode.INTERNAL_ERROR);
    when(blobClient.uploadWithResponse(any(BlobParallelUploadOptions.class), any(), any()))
        .thenThrow(exception);

    // Act & Assert
    assertThatCode(() -> wrapper.update(ANY_OBJECT_KEY, ANY_DATA, ANY_ETAG))
        .isInstanceOf(ObjectStorageWrapperException.class);
  }

  // delete (without version) tests

  @Test
  public void delete_ExistingObjectKeyGiven_ShouldCallClientDelete() throws Exception {
    // Arrange
    BlobClient blobClient = mock(BlobClient.class);
    when(client.getBlobClient(ANY_OBJECT_KEY)).thenReturn(blobClient);

    // Act
    wrapper.delete(ANY_OBJECT_KEY);

    // Assert
    verify(blobClient).delete();
  }

  @Test
  public void delete_BlobNotFound_ShouldThrowPreconditionFailedException() {
    // Arrange
    BlobClient blobClient = mock(BlobClient.class);
    when(client.getBlobClient(ANY_OBJECT_KEY)).thenReturn(blobClient);

    BlobStorageException exception = createBlobStorageException(BlobErrorCode.BLOB_NOT_FOUND);
    doThrow(exception).when(blobClient).delete();

    // Act & Assert
    assertThatCode(() -> wrapper.delete(ANY_OBJECT_KEY))
        .isInstanceOf(PreconditionFailedException.class);
  }

  @Test
  public void delete_OtherBlobStorageExceptionThrown_ShouldThrowObjectStorageWrapperException() {
    // Arrange
    BlobClient blobClient = mock(BlobClient.class);
    when(client.getBlobClient(ANY_OBJECT_KEY)).thenReturn(blobClient);

    BlobStorageException exception = createBlobStorageException(BlobErrorCode.INTERNAL_ERROR);
    doThrow(exception).when(blobClient).delete();

    // Act & Assert
    assertThatCode(() -> wrapper.delete(ANY_OBJECT_KEY))
        .isInstanceOf(ObjectStorageWrapperException.class);
  }

  // delete (with version) tests

  @Test
  public void delete_WithVersion_ExistingObjectKeyGiven_ShouldCallClientDeleteWithResponse()
      throws Exception {
    // Arrange
    BlobClient blobClient = mock(BlobClient.class);
    when(client.getBlobClient(ANY_OBJECT_KEY)).thenReturn(blobClient);

    // Act
    wrapper.delete(ANY_OBJECT_KEY, ANY_ETAG);

    // Assert
    verify(blobClient).deleteWithResponse(any(), any(BlobRequestConditions.class), any(), any());
  }

  @Test
  public void delete_WithVersion_ConditionNotMet_ShouldThrowPreconditionFailedException() {
    // Arrange
    BlobClient blobClient = mock(BlobClient.class);
    when(client.getBlobClient(ANY_OBJECT_KEY)).thenReturn(blobClient);

    BlobStorageException exception = createBlobStorageException(BlobErrorCode.CONDITION_NOT_MET);
    when(blobClient.deleteWithResponse(any(), any(BlobRequestConditions.class), any(), any()))
        .thenThrow(exception);

    // Act & Assert
    assertThatCode(() -> wrapper.delete(ANY_OBJECT_KEY, ANY_ETAG))
        .isInstanceOf(PreconditionFailedException.class);
  }

  @Test
  public void delete_WithVersion_BlobNotFound_ShouldThrowPreconditionFailedException() {
    // Arrange
    BlobClient blobClient = mock(BlobClient.class);
    when(client.getBlobClient(ANY_OBJECT_KEY)).thenReturn(blobClient);

    BlobStorageException exception = createBlobStorageException(BlobErrorCode.BLOB_NOT_FOUND);
    when(blobClient.deleteWithResponse(any(), any(BlobRequestConditions.class), any(), any()))
        .thenThrow(exception);

    // Act & Assert
    assertThatCode(() -> wrapper.delete(ANY_OBJECT_KEY, ANY_ETAG))
        .isInstanceOf(PreconditionFailedException.class);
  }

  @Test
  public void
      delete_WithVersion_OtherBlobStorageExceptionThrown_ShouldThrowObjectStorageWrapperException() {
    // Arrange
    BlobClient blobClient = mock(BlobClient.class);
    when(client.getBlobClient(ANY_OBJECT_KEY)).thenReturn(blobClient);

    BlobStorageException exception = createBlobStorageException(BlobErrorCode.INTERNAL_ERROR);
    when(blobClient.deleteWithResponse(any(), any(BlobRequestConditions.class), any(), any()))
        .thenThrow(exception);

    // Act & Assert
    assertThatCode(() -> wrapper.delete(ANY_OBJECT_KEY, ANY_ETAG))
        .isInstanceOf(ObjectStorageWrapperException.class);
  }

  // deleteByPrefix tests

  @Test
  @SuppressWarnings("unchecked")
  public void deleteByPrefix_PrefixGiven_ShouldDeleteAllObjectsWithThePrefix() throws Exception {
    // Arrange
    String objectKey1 = ANY_PREFIX + "test1/object1";
    String objectKey2 = ANY_PREFIX + "test1/object2";

    BlobItem item1 = new BlobItem().setName(objectKey1);
    BlobItem item2 = new BlobItem().setName(objectKey2);

    BlobClient blobClient1 = mock(BlobClient.class);
    BlobClient blobClient2 = mock(BlobClient.class);
    when(client.getBlobClient(objectKey1)).thenReturn(blobClient1);
    when(client.getBlobClient(objectKey2)).thenReturn(blobClient2);

    PagedIterable<BlobItem> pagedIterable = mock(PagedIterable.class);
    when(client.listBlobs(any(ListBlobsOptions.class), any())).thenReturn(pagedIterable);
    doAnswer(
            invocation -> {
              java.util.function.Consumer<BlobItem> consumer = invocation.getArgument(0);
              consumer.accept(item1);
              consumer.accept(item2);
              return null;
            })
        .when(pagedIterable)
        .forEach(any());

    // Act
    wrapper.deleteByPrefix(ANY_PREFIX);

    // Assert
    verify(blobClient1).delete();
    verify(blobClient2).delete();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void deleteByPrefix_NoObjectsWithPrefix_ShouldDoNothing() throws Exception {
    // Arrange
    PagedIterable<BlobItem> pagedIterable = mock(PagedIterable.class);
    when(client.listBlobs(any(ListBlobsOptions.class), any())).thenReturn(pagedIterable);
    doAnswer(
            invocation -> {
              // No items to iterate
              return null;
            })
        .when(pagedIterable)
        .forEach(any());

    // Act
    wrapper.deleteByPrefix(ANY_PREFIX);

    // Assert
    verify(client, never()).getBlobClient(any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void deleteByPrefix_BlobNotFoundDuringDelete_ShouldIgnore() throws Exception {
    // Arrange
    String objectKey1 = ANY_PREFIX + "test1/object1";
    BlobItem item1 = new BlobItem().setName(objectKey1);

    BlobClient blobClient1 = mock(BlobClient.class);
    when(client.getBlobClient(objectKey1)).thenReturn(blobClient1);

    BlobStorageException exception = createBlobStorageException(BlobErrorCode.BLOB_NOT_FOUND);
    doThrow(exception).when(blobClient1).delete();

    PagedIterable<BlobItem> pagedIterable = mock(PagedIterable.class);
    when(client.listBlobs(any(ListBlobsOptions.class), any())).thenReturn(pagedIterable);
    doAnswer(
            invocation -> {
              java.util.function.Consumer<BlobItem> consumer = invocation.getArgument(0);
              consumer.accept(item1);
              return null;
            })
        .when(pagedIterable)
        .forEach(any());

    // Act & Assert (should not throw)
    assertThatCode(() -> wrapper.deleteByPrefix(ANY_PREFIX)).doesNotThrowAnyException();
  }

  @Test
  public void deleteByPrefix_ExceptionThrown_ShouldThrowObjectStorageWrapperException() {
    // Arrange
    when(client.listBlobs(any(ListBlobsOptions.class), any()))
        .thenThrow(new RuntimeException("test error"));

    // Act & Assert
    assertThatCode(() -> wrapper.deleteByPrefix(ANY_PREFIX))
        .isInstanceOf(ObjectStorageWrapperException.class);
  }

  // close test

  @Test
  public void close_ShouldDoNothing() {
    // Act & Assert
    assertThatCode(() -> wrapper.close()).doesNotThrowAnyException();
  }
}
