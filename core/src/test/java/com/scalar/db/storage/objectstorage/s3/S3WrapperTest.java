package com.scalar.db.storage.objectstorage.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.storage.objectstorage.ObjectStorageWrapperException;
import com.scalar.db.storage.objectstorage.ObjectStorageWrapperResponse;
import com.scalar.db.storage.objectstorage.PreconditionFailedException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

public class S3WrapperTest {
  private static final String METADATA_NAMESPACE = "scalardb";
  private static final String REGION = "us-west-2";
  private static final String BUCKET = "bucket";

  @Mock private S3Config config;
  @Mock private S3Client client;
  private S3Wrapper wrapper;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    when(config.getMetadataNamespace()).thenReturn(METADATA_NAMESPACE);
    when(config.getRegion()).thenReturn(REGION);
    when(config.getBucket()).thenReturn(BUCKET);
    wrapper = new S3Wrapper(config, client);
  }

  @Test
  public void get_ExistingObjectKeyGiven_ShouldCallClientGetObjectAsBytes() throws Exception {
    // Arrange
    String objectKey = "test-object-key";
    String data = "test-data";
    String eTag = "test-etag";
    GetObjectRequest request = GetObjectRequest.builder().bucket(BUCKET).key(objectKey).build();

    @SuppressWarnings("unchecked")
    ResponseBytes<GetObjectResponse> responseBytes = mock(ResponseBytes.class);
    GetObjectResponse getObjectResponse = mock(GetObjectResponse.class);

    when(client.getObjectAsBytes(any(GetObjectRequest.class))).thenReturn(responseBytes);
    when(responseBytes.asUtf8String()).thenReturn(data);
    when(responseBytes.response()).thenReturn(getObjectResponse);
    when(getObjectResponse.eTag()).thenReturn(eTag);

    // Act
    Optional<ObjectStorageWrapperResponse> result = wrapper.get(objectKey);

    // Assert
    verify(client).getObjectAsBytes(eq(request));
    assertThat(result).isPresent();
    assertThat(result.get().getPayload()).isEqualTo(data);
    assertThat(result.get().getVersion()).isEqualTo(eTag);
  }

  @Test
  public void get_NonExistingObjectKeyGiven_ShouldReturnEmptyOptional() throws Exception {
    // Arrange
    String objectKey = "non-existing-object-key";

    when(client.getObjectAsBytes(any(GetObjectRequest.class)))
        .thenThrow(S3Exception.builder().statusCode(404).build());

    // Act
    Optional<ObjectStorageWrapperResponse> result = wrapper.get(objectKey);

    // Assert
    assertThat(result).isNotPresent();
  }

  @Test
  public void get_OtherS3ExceptionThrown_ShouldThrowObjectStorageWrapperException() {
    // Arrange
    String objectKey = "test-object-key";

    when(client.getObjectAsBytes(any(GetObjectRequest.class)))
        .thenThrow(S3Exception.builder().statusCode(500).build());

    // Act & Assert
    assertThatCode(() -> wrapper.get(objectKey)).isInstanceOf(ObjectStorageWrapperException.class);
  }

  @Test
  public void getKeys_PrefixGiven_ShouldCallClientListObjectsV2Paginator() throws Exception {
    // Arrange
    String prefix = "test-prefix/";
    String objectKey1 = "test-prefix/test1/object1";
    String objectKey2 = "test-prefix/test1/object2";
    String objectKey3 = "test-prefix/test2/object3";
    ListObjectsV2Request request =
        ListObjectsV2Request.builder().bucket(BUCKET).prefix(prefix).build();

    S3Object s3Object1 = mock(S3Object.class);
    S3Object s3Object2 = mock(S3Object.class);
    S3Object s3Object3 = mock(S3Object.class);
    when(s3Object1.key()).thenReturn(objectKey1);
    when(s3Object2.key()).thenReturn(objectKey2);
    when(s3Object3.key()).thenReturn(objectKey3);

    // Mock paginated responses with 2 pages
    ListObjectsV2Response response1 = mock(ListObjectsV2Response.class);
    ListObjectsV2Response response2 = mock(ListObjectsV2Response.class);
    when(response1.contents()).thenReturn(Arrays.asList(s3Object1, s3Object2));
    when(response2.contents()).thenReturn(Collections.singletonList(s3Object3));

    // Mock the paginator
    ListObjectsV2Iterable paginator = mock(ListObjectsV2Iterable.class);
    when(client.listObjectsV2Paginator(any(ListObjectsV2Request.class))).thenReturn(paginator);
    when(paginator.stream()).thenReturn(Stream.of(response1, response2));

    // Act
    Set<String> result = wrapper.getKeys(prefix);

    // Assert
    verify(client).listObjectsV2Paginator(eq(request));
    assertThat(result).containsExactlyInAnyOrder(objectKey1, objectKey2, objectKey3);
  }

  @Test
  public void getKeys_NoObjectsWithPrefix_ShouldReturnEmptySet() throws Exception {
    // Arrange
    String prefix = "empty-prefix/";

    // Mock paginated response with no objects
    ListObjectsV2Response response = mock(ListObjectsV2Response.class);
    when(response.contents()).thenReturn(Collections.emptyList());

    // Mock the paginator
    ListObjectsV2Iterable paginator = mock(ListObjectsV2Iterable.class);
    when(client.listObjectsV2Paginator(any(ListObjectsV2Request.class))).thenReturn(paginator);
    when(paginator.stream()).thenReturn(Stream.of(response));

    // Act
    Set<String> result = wrapper.getKeys(prefix);

    // Assert
    assertThat(result).isEmpty();
  }

  @Test
  public void getKeys_S3ExceptionThrown_ShouldThrowObjectStorageWrapperException() {
    // Arrange
    String prefix = "test-prefix/";

    when(client.listObjectsV2Paginator(any(ListObjectsV2Request.class)))
        .thenThrow(S3Exception.builder().statusCode(500).build());

    // Act & Assert
    assertThatCode(() -> wrapper.getKeys(prefix)).isInstanceOf(ObjectStorageWrapperException.class);
  }

  @Test
  public void insert_NonExistingObjectKeyGiven_ShouldCallClientPutObject() throws Exception {
    // Arrange
    String objectKey = "test-object-key";
    String objectData = "test-object-data";
    PutObjectRequest request =
        PutObjectRequest.builder().bucket(BUCKET).key(objectKey).ifNoneMatch("*").build();

    PutObjectResponse putObjectResponse = mock(PutObjectResponse.class);
    when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
        .thenReturn(putObjectResponse);

    // Act
    wrapper.insert(objectKey, objectData);

    // Assert
    ArgumentCaptor<RequestBody> requestBodyCaptor = ArgumentCaptor.forClass(RequestBody.class);
    verify(client).putObject(eq(request), requestBodyCaptor.capture());
    assertThat(requestBodyCaptor.getValue()).isNotNull();
  }

  @Test
  public void insert_S3ExceptionWith412Thrown_ShouldThrowPreconditionFailedException() {
    // Arrange
    String objectKey = "existing-object-key";
    String objectData = "test-object-data";

    when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
        .thenThrow(S3Exception.builder().statusCode(412).build());

    // Act & Assert
    assertThatCode(() -> wrapper.insert(objectKey, objectData))
        .isInstanceOf(PreconditionFailedException.class);
  }

  @Test
  public void insert_S3ExceptionWith409Thrown_ShouldThrowConflictOccurredException() {
    // Arrange
    String objectKey = "existing-object-key";
    String objectData = "test-object-data";

    when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
        .thenThrow(S3Exception.builder().statusCode(409).build());

    // Act & Assert
    assertThatCode(() -> wrapper.insert(objectKey, objectData))
        .isInstanceOf(com.scalar.db.storage.objectstorage.ConflictOccurredException.class);
  }

  @Test
  public void insert_OtherS3ExceptionThrown_ShouldThrowObjectStorageWrapperException() {
    // Arrange
    String objectKey = "test-object-key";
    String objectData = "test-object-data";

    when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
        .thenThrow(S3Exception.builder().statusCode(500).build());

    // Act & Assert
    assertThatCode(() -> wrapper.insert(objectKey, objectData))
        .isInstanceOf(ObjectStorageWrapperException.class);
  }

  @Test
  public void update_ExistingObjectKeyGiven_ShouldCallClientPutObjectWithETag() throws Exception {
    // Arrange
    String objectKey = "test-object-key";
    String objectData = "updated-object-data";
    String eTag = "test-etag";
    PutObjectRequest request =
        PutObjectRequest.builder().bucket(BUCKET).key(objectKey).ifMatch(eTag).build();

    PutObjectResponse putObjectResponse = mock(PutObjectResponse.class);
    when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
        .thenReturn(putObjectResponse);

    // Act
    wrapper.update(objectKey, objectData, eTag);

    // Assert
    ArgumentCaptor<RequestBody> requestBodyCaptor = ArgumentCaptor.forClass(RequestBody.class);
    verify(client).putObject(eq(request), requestBodyCaptor.capture());
    assertThat(requestBodyCaptor.getValue()).isNotNull();
  }

  @Test
  public void update_S3ExceptionWith412Thrown_ShouldThrowPreconditionFailedException() {
    // Arrange
    String objectKey = "test-object-key";
    String objectData = "updated-object-data";
    String eTag = "test-etag";

    when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
        .thenThrow(S3Exception.builder().statusCode(412).build());

    // Act & Assert
    assertThatCode(() -> wrapper.update(objectKey, objectData, eTag))
        .isInstanceOf(PreconditionFailedException.class);
  }

  @Test
  public void update_S3ExceptionWith409Thrown_ShouldThrowPreconditionFailedException() {
    // Arrange
    String objectKey = "test-object-key";
    String objectData = "updated-object-data";
    String eTag = "test-etag";

    when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
        .thenThrow(S3Exception.builder().statusCode(409).build());

    // Act & Assert
    assertThatCode(() -> wrapper.update(objectKey, objectData, eTag))
        .isInstanceOf(PreconditionFailedException.class);
  }

  @Test
  public void update_OtherS3ExceptionThrown_ShouldThrowObjectStorageWrapperException() {
    // Arrange
    String objectKey = "test-object-key";
    String objectData = "updated-object-data";
    String eTag = "test-etag";

    when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
        .thenThrow(S3Exception.builder().statusCode(500).build());

    // Act & Assert
    assertThatCode(() -> wrapper.update(objectKey, objectData, eTag))
        .isInstanceOf(ObjectStorageWrapperException.class);
  }

  @Test
  public void delete_ExistingObjectKeyGiven_ShouldCallClientDeleteObject() throws Exception {
    // Arrange
    String objectKey = "test-object-key";
    DeleteObjectRequest request =
        DeleteObjectRequest.builder().bucket(BUCKET).key(objectKey).build();

    DeleteObjectResponse deleteObjectResponse = mock(DeleteObjectResponse.class);
    when(client.deleteObject(any(DeleteObjectRequest.class))).thenReturn(deleteObjectResponse);

    // Act
    wrapper.delete(objectKey);

    // Assert
    verify(client).deleteObject(eq(request));
  }

  @Test
  public void delete_NonExistingObjectKeyGiven_ShouldThrowPreconditionFailedException() {
    // Arrange
    String objectKey = "non-existing-object-key";

    when(client.deleteObject(any(DeleteObjectRequest.class)))
        .thenThrow(S3Exception.builder().statusCode(404).build());

    // Act & Assert
    assertThatCode(() -> wrapper.delete(objectKey)).isInstanceOf(PreconditionFailedException.class);
  }

  @Test
  public void delete_OtherS3ExceptionThrown_ShouldThrowObjectStorageWrapperException() {
    // Arrange
    String objectKey = "test-object-key";

    when(client.deleteObject(any(DeleteObjectRequest.class)))
        .thenThrow(S3Exception.builder().statusCode(500).build());

    // Act & Assert
    assertThatCode(() -> wrapper.delete(objectKey))
        .isInstanceOf(ObjectStorageWrapperException.class);
  }

  @Test
  public void delete_WithVersion_ExistingObjectKeyGiven_ShouldCallClientDeleteObjectWithETag()
      throws Exception {
    // Arrange
    String objectKey = "test-object-key";
    String eTag = "test-etag";
    DeleteObjectRequest request =
        DeleteObjectRequest.builder().bucket(BUCKET).key(objectKey).ifMatch(eTag).build();

    DeleteObjectResponse deleteObjectResponse = mock(DeleteObjectResponse.class);
    when(client.deleteObject(any(DeleteObjectRequest.class))).thenReturn(deleteObjectResponse);

    // Act
    wrapper.delete(objectKey, eTag);

    // Assert
    verify(client).deleteObject(eq(request));
  }

  @Test
  public void
      delete_WithVersion_NonExistingObjectKeyGiven_ShouldThrowPreconditionFailedException() {
    // Arrange
    String objectKey = "non-existing-object-key";
    String eTag = "test-etag";

    when(client.deleteObject(any(DeleteObjectRequest.class)))
        .thenThrow(S3Exception.builder().statusCode(404).build());

    // Act & Assert
    assertThatCode(() -> wrapper.delete(objectKey, eTag))
        .isInstanceOf(PreconditionFailedException.class);
  }

  @Test
  public void delete_WithVersion_S3ExceptionWith412Thrown_ShouldThrowPreconditionFailedException() {
    // Arrange
    String objectKey = "test-object-key";
    String eTag = "test-etag";

    when(client.deleteObject(any(DeleteObjectRequest.class)))
        .thenThrow(S3Exception.builder().statusCode(412).build());

    // Act & Assert
    assertThatCode(() -> wrapper.delete(objectKey, eTag))
        .isInstanceOf(PreconditionFailedException.class);
  }

  @Test
  public void delete_WithVersion_S3ExceptionWith409Thrown_ShouldThrowPreconditionFailedException() {
    // Arrange
    String objectKey = "test-object-key";
    String eTag = "test-etag";

    when(client.deleteObject(any(DeleteObjectRequest.class)))
        .thenThrow(S3Exception.builder().statusCode(409).build());

    // Act & Assert
    assertThatCode(() -> wrapper.delete(objectKey, eTag))
        .isInstanceOf(PreconditionFailedException.class);
  }

  @Test
  public void delete_WithVersion_OtherS3ExceptionThrown_ShouldThrowObjectStorageWrapperException() {
    // Arrange
    String objectKey = "test-object-key";
    String eTag = "test-etag";

    when(client.deleteObject(any(DeleteObjectRequest.class)))
        .thenThrow(S3Exception.builder().statusCode(500).build());

    // Act & Assert
    assertThatCode(() -> wrapper.delete(objectKey, eTag))
        .isInstanceOf(ObjectStorageWrapperException.class);
  }

  @Test
  public void deleteByPrefix_PrefixGiven_ShouldDeleteAllObjectsWithThePrefix() throws Exception {
    // Arrange
    String prefix = "test-prefix/";
    String objectKey1 = "test-prefix/test1/object1";
    String objectKey2 = "test-prefix/test1/object2";
    String objectKey3 = "test-prefix/test2/object3";

    S3Object s3Object1 = mock(S3Object.class);
    S3Object s3Object2 = mock(S3Object.class);
    S3Object s3Object3 = mock(S3Object.class);
    when(s3Object1.key()).thenReturn(objectKey1);
    when(s3Object2.key()).thenReturn(objectKey2);
    when(s3Object3.key()).thenReturn(objectKey3);

    // Mock paginated responses with 2 pages
    ListObjectsV2Response response1 = mock(ListObjectsV2Response.class);
    ListObjectsV2Response response2 = mock(ListObjectsV2Response.class);
    when(response1.contents()).thenReturn(Arrays.asList(s3Object1, s3Object2));
    when(response2.contents()).thenReturn(Collections.singletonList(s3Object3));

    // Mock the paginator
    ListObjectsV2Iterable paginator = mock(ListObjectsV2Iterable.class);
    when(client.listObjectsV2Paginator(any(ListObjectsV2Request.class))).thenReturn(paginator);
    when(paginator.stream()).thenReturn(Stream.of(response1, response2));

    DeleteObjectsResponse deleteObjectsResponse = mock(DeleteObjectsResponse.class);
    when(client.deleteObjects(any(DeleteObjectsRequest.class))).thenReturn(deleteObjectsResponse);

    // Act
    wrapper.deleteByPrefix(prefix);

    // Assert
    ArgumentCaptor<DeleteObjectsRequest> requestCaptor =
        ArgumentCaptor.forClass(DeleteObjectsRequest.class);
    verify(client).deleteObjects(requestCaptor.capture());

    DeleteObjectsRequest capturedRequest = requestCaptor.getValue();
    assertThat(capturedRequest.bucket()).isEqualTo(BUCKET);
    assertThat(capturedRequest.delete().objects()).hasSize(3);
    assertThat(capturedRequest.delete().objects())
        .extracting(ObjectIdentifier::key)
        .containsExactlyInAnyOrder(objectKey1, objectKey2, objectKey3);
  }

  @Test
  public void deleteByPrefix_NoObjectsWithPrefix_ShouldDoNothing() throws Exception {
    // Arrange
    String prefix = "empty-prefix/";

    // Mock paginated response with no objects
    ListObjectsV2Response response = mock(ListObjectsV2Response.class);
    when(response.contents()).thenReturn(Collections.emptyList());

    // Mock the paginator
    ListObjectsV2Iterable paginator = mock(ListObjectsV2Iterable.class);
    when(client.listObjectsV2Paginator(any(ListObjectsV2Request.class))).thenReturn(paginator);
    when(paginator.stream()).thenReturn(Stream.of(response));

    // Act
    wrapper.deleteByPrefix(prefix);

    // Assert
    verify(client, org.mockito.Mockito.never()).deleteObjects(any(DeleteObjectsRequest.class));
  }

  @Test
  public void deleteByPrefix_S3ExceptionThrown_ShouldThrowObjectStorageWrapperException() {
    // Arrange
    String prefix = "test-prefix/";

    when(client.listObjectsV2Paginator(any(ListObjectsV2Request.class)))
        .thenThrow(S3Exception.builder().statusCode(500).build());

    // Act & Assert
    assertThatCode(() -> wrapper.deleteByPrefix(prefix))
        .isInstanceOf(ObjectStorageWrapperException.class);
  }

  @Test
  public void close_ShouldCloseTheClient() throws Exception {
    // Arrange

    // Act
    wrapper.close();

    // Assert
    verify(client).close();
  }
}
