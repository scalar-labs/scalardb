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
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
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
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Publisher;

public class S3WrapperTest {
  private static final String METADATA_NAMESPACE = "scalardb";
  private static final String REGION = "us-west-2";
  private static final String BUCKET = "bucket";
  private static final String ANY_OBJECT_KEY = "any-object-key";
  private static final String ANY_PREFIX = "any-prefix/";
  private static final String ANY_DATA = "any-data";
  private static final String ANY_ETAG = "any-etag";

  @Mock private S3Config config;
  @Mock private S3AsyncClient client;
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
  @SuppressWarnings("unchecked")
  public void get_ExistingObjectKeyGiven_ShouldCallClientGetObjectAsBytes() throws Exception {
    // Arrange
    GetObjectRequest request =
        GetObjectRequest.builder().bucket(BUCKET).key(ANY_OBJECT_KEY).build();

    ResponseBytes<GetObjectResponse> responseBytes = mock(ResponseBytes.class);
    GetObjectResponse getObjectResponse = mock(GetObjectResponse.class);

    when(client.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
        .thenReturn(CompletableFuture.completedFuture(responseBytes));
    when(responseBytes.asUtf8String()).thenReturn(ANY_DATA);
    when(responseBytes.response()).thenReturn(getObjectResponse);
    when(getObjectResponse.eTag()).thenReturn(ANY_ETAG);

    // Act
    Optional<ObjectStorageWrapperResponse> result = wrapper.get(ANY_OBJECT_KEY);

    // Assert
    verify(client).getObject(eq(request), any(AsyncResponseTransformer.class));
    assertThat(result).isPresent();
    assertThat(result.get().getPayload()).isEqualTo(ANY_DATA);
    assertThat(result.get().getVersion()).isEqualTo(ANY_ETAG);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void get_NonExistingObjectKeyGiven_ShouldReturnEmptyOptional() throws Exception {
    // Arrange
    CompletableFuture<ResponseBytes<GetObjectResponse>> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(S3Exception.builder().statusCode(404).build());
    when(client.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
        .thenReturn(failedFuture);

    // Act
    Optional<ObjectStorageWrapperResponse> result = wrapper.get(ANY_OBJECT_KEY);

    // Assert
    assertThat(result).isNotPresent();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void get_OtherS3ExceptionThrown_ShouldThrowObjectStorageWrapperException() {
    // Arrange
    CompletableFuture<ResponseBytes<GetObjectResponse>> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(S3Exception.builder().statusCode(500).build());
    when(client.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
        .thenReturn(failedFuture);

    // Act & Assert
    assertThatCode(() -> wrapper.get(ANY_OBJECT_KEY))
        .isInstanceOf(ObjectStorageWrapperException.class);
  }

  @Test
  public void getKeys_PrefixGiven_ShouldCallClientListObjectsV2Paginator() throws Exception {
    // Arrange
    String objectKey1 = ANY_PREFIX + "test1/object1";
    String objectKey2 = ANY_PREFIX + "test1/object2";
    String objectKey3 = ANY_PREFIX + "test2/object3";
    ListObjectsV2Request request =
        ListObjectsV2Request.builder().bucket(BUCKET).prefix(ANY_PREFIX).build();

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
    ListObjectsV2Publisher paginator = mock(ListObjectsV2Publisher.class);
    when(client.listObjectsV2Paginator(any(ListObjectsV2Request.class))).thenReturn(paginator);
    @SuppressWarnings("unchecked")
    Consumer<ListObjectsV2Response> consumer = any(Consumer.class);
    when(paginator.subscribe(consumer))
        .thenAnswer(
            invocation -> {
              Consumer<ListObjectsV2Response> actualConsumer = invocation.getArgument(0);
              actualConsumer.accept(response1);
              actualConsumer.accept(response2);
              return CompletableFuture.completedFuture(null);
            });

    // Act
    Set<String> result = wrapper.getKeys(ANY_PREFIX);

    // Assert
    verify(client).listObjectsV2Paginator(eq(request));
    assertThat(result).containsExactlyInAnyOrder(objectKey1, objectKey2, objectKey3);
  }

  @Test
  public void getKeys_NoObjectsWithPrefix_ShouldReturnEmptySet() throws Exception {
    // Arrange
    // Mock paginated response with no objects
    ListObjectsV2Response response = mock(ListObjectsV2Response.class);
    when(response.contents()).thenReturn(Collections.emptyList());

    // Mock the paginator
    ListObjectsV2Publisher paginator = mock(ListObjectsV2Publisher.class);
    when(client.listObjectsV2Paginator(any(ListObjectsV2Request.class))).thenReturn(paginator);

    @SuppressWarnings("unchecked")
    Consumer<ListObjectsV2Response> consumer = any(Consumer.class);
    when(paginator.subscribe(consumer))
        .thenAnswer(
            invocation -> {
              Consumer<ListObjectsV2Response> actualConsumer = invocation.getArgument(0);
              actualConsumer.accept(response);
              return CompletableFuture.completedFuture(null);
            });

    // Act
    Set<String> result = wrapper.getKeys(ANY_PREFIX);

    // Assert
    assertThat(result).isEmpty();
  }

  @Test
  public void getKeys_S3ExceptionThrown_ShouldThrowObjectStorageWrapperException() {
    // Arrange
    String prefix = "test-prefix/";

    ListObjectsV2Publisher paginator = mock(ListObjectsV2Publisher.class);
    when(client.listObjectsV2Paginator(any(ListObjectsV2Request.class))).thenReturn(paginator);

    CompletableFuture<Void> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(S3Exception.builder().statusCode(500).build());
    @SuppressWarnings("unchecked")
    Consumer<ListObjectsV2Response> consumer = any(Consumer.class);
    when(paginator.subscribe(consumer)).thenReturn(failedFuture);

    // Act & Assert
    assertThatCode(() -> wrapper.getKeys(prefix)).isInstanceOf(ObjectStorageWrapperException.class);
  }

  @Test
  public void insert_NonExistingObjectKeyGiven_ShouldCallClientPutObject() throws Exception {
    // Arrange
    PutObjectRequest request =
        PutObjectRequest.builder().bucket(BUCKET).key(ANY_OBJECT_KEY).ifNoneMatch("*").build();

    PutObjectResponse putObjectResponse = mock(PutObjectResponse.class);
    when(client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
        .thenReturn(CompletableFuture.completedFuture(putObjectResponse));

    // Act
    wrapper.insert(ANY_OBJECT_KEY, ANY_DATA);

    // Assert
    ArgumentCaptor<AsyncRequestBody> requestBodyCaptor =
        ArgumentCaptor.forClass(AsyncRequestBody.class);
    verify(client).putObject(eq(request), requestBodyCaptor.capture());
    assertThat(requestBodyCaptor.getValue()).isNotNull();
  }

  @Test
  public void insert_S3ExceptionWith412Thrown_ShouldThrowPreconditionFailedException() {
    // Arrange
    CompletableFuture<PutObjectResponse> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(S3Exception.builder().statusCode(412).build());
    when(client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
        .thenReturn(failedFuture);

    // Act & Assert
    assertThatCode(() -> wrapper.insert(ANY_OBJECT_KEY, ANY_DATA))
        .isInstanceOf(PreconditionFailedException.class);
  }

  @Test
  public void insert_S3ExceptionWith409Thrown_ShouldThrowConflictOccurredException() {
    // Arrange
    CompletableFuture<PutObjectResponse> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(S3Exception.builder().statusCode(409).build());
    when(client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
        .thenReturn(failedFuture);

    // Act & Assert
    assertThatCode(() -> wrapper.insert(ANY_OBJECT_KEY, ANY_DATA))
        .isInstanceOf(com.scalar.db.storage.objectstorage.ConflictOccurredException.class);
  }

  @Test
  public void insert_OtherS3ExceptionThrown_ShouldThrowObjectStorageWrapperException() {
    // Arrange
    CompletableFuture<PutObjectResponse> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(S3Exception.builder().statusCode(500).build());
    when(client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
        .thenReturn(failedFuture);

    // Act & Assert
    assertThatCode(() -> wrapper.insert(ANY_OBJECT_KEY, ANY_DATA))
        .isInstanceOf(ObjectStorageWrapperException.class);
  }

  @Test
  public void update_ExistingObjectKeyGiven_ShouldCallClientPutObjectWithETag() throws Exception {
    // Arrange
    PutObjectRequest request =
        PutObjectRequest.builder().bucket(BUCKET).key(ANY_OBJECT_KEY).ifMatch(ANY_ETAG).build();

    PutObjectResponse putObjectResponse = mock(PutObjectResponse.class);
    when(client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
        .thenReturn(CompletableFuture.completedFuture(putObjectResponse));

    // Act
    wrapper.update(ANY_OBJECT_KEY, ANY_DATA, ANY_ETAG);

    // Assert
    ArgumentCaptor<AsyncRequestBody> requestBodyCaptor =
        ArgumentCaptor.forClass(AsyncRequestBody.class);
    verify(client).putObject(eq(request), requestBodyCaptor.capture());
    assertThat(requestBodyCaptor.getValue()).isNotNull();
  }

  @Test
  public void update_S3ExceptionWith412Thrown_ShouldThrowPreconditionFailedException() {
    // Arrange
    CompletableFuture<PutObjectResponse> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(S3Exception.builder().statusCode(412).build());
    when(client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
        .thenReturn(failedFuture);

    // Act & Assert
    assertThatCode(() -> wrapper.update(ANY_OBJECT_KEY, ANY_DATA, ANY_ETAG))
        .isInstanceOf(PreconditionFailedException.class);
  }

  @Test
  public void update_S3ExceptionWith409Thrown_ShouldThrowPreconditionFailedException() {
    // Arrange
    CompletableFuture<PutObjectResponse> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(S3Exception.builder().statusCode(409).build());
    when(client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
        .thenReturn(failedFuture);

    // Act & Assert
    assertThatCode(() -> wrapper.update(ANY_OBJECT_KEY, ANY_DATA, ANY_ETAG))
        .isInstanceOf(PreconditionFailedException.class);
  }

  @Test
  public void update_OtherS3ExceptionThrown_ShouldThrowObjectStorageWrapperException() {
    // Arrange
    CompletableFuture<PutObjectResponse> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(S3Exception.builder().statusCode(500).build());
    when(client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
        .thenReturn(failedFuture);

    // Act & Assert
    assertThatCode(() -> wrapper.update(ANY_OBJECT_KEY, ANY_DATA, ANY_ETAG))
        .isInstanceOf(ObjectStorageWrapperException.class);
  }

  @Test
  public void delete_ExistingObjectKeyGiven_ShouldCallClientDeleteObject() throws Exception {
    // Arrange
    DeleteObjectRequest request =
        DeleteObjectRequest.builder().bucket(BUCKET).key(ANY_OBJECT_KEY).ifMatch("*").build();

    DeleteObjectResponse deleteObjectResponse = mock(DeleteObjectResponse.class);
    when(client.deleteObject(any(DeleteObjectRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(deleteObjectResponse));

    // Act
    wrapper.delete(ANY_OBJECT_KEY);

    // Assert
    verify(client).deleteObject(eq(request));
  }

  @Test
  public void delete_NonExistingObjectKeyGiven_ShouldThrowPreconditionFailedException() {
    // Arrange
    CompletableFuture<DeleteObjectResponse> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(S3Exception.builder().statusCode(404).build());
    when(client.deleteObject(any(DeleteObjectRequest.class))).thenReturn(failedFuture);

    // Act & Assert
    assertThatCode(() -> wrapper.delete(ANY_OBJECT_KEY))
        .isInstanceOf(PreconditionFailedException.class);
  }

  @Test
  public void delete_OtherS3ExceptionThrown_ShouldThrowObjectStorageWrapperException() {
    // Arrange
    CompletableFuture<DeleteObjectResponse> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(S3Exception.builder().statusCode(500).build());
    when(client.deleteObject(any(DeleteObjectRequest.class))).thenReturn(failedFuture);

    // Act & Assert
    assertThatCode(() -> wrapper.delete(ANY_OBJECT_KEY))
        .isInstanceOf(ObjectStorageWrapperException.class);
  }

  @Test
  public void delete_WithVersion_ExistingObjectKeyGiven_ShouldCallClientDeleteObjectWithETag()
      throws Exception {
    // Arrange
    DeleteObjectRequest request =
        DeleteObjectRequest.builder().bucket(BUCKET).key(ANY_OBJECT_KEY).ifMatch(ANY_ETAG).build();

    DeleteObjectResponse deleteObjectResponse = mock(DeleteObjectResponse.class);
    when(client.deleteObject(any(DeleteObjectRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(deleteObjectResponse));

    // Act
    wrapper.delete(ANY_OBJECT_KEY, ANY_ETAG);

    // Assert
    verify(client).deleteObject(eq(request));
  }

  @Test
  public void
      delete_WithVersion_NonExistingObjectKeyGiven_ShouldThrowPreconditionFailedException() {
    // Arrange
    CompletableFuture<DeleteObjectResponse> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(S3Exception.builder().statusCode(404).build());
    when(client.deleteObject(any(DeleteObjectRequest.class))).thenReturn(failedFuture);

    // Act & Assert
    assertThatCode(() -> wrapper.delete(ANY_OBJECT_KEY, ANY_ETAG))
        .isInstanceOf(PreconditionFailedException.class);
  }

  @Test
  public void delete_WithVersion_S3ExceptionWith412Thrown_ShouldThrowPreconditionFailedException() {
    // Arrange
    CompletableFuture<DeleteObjectResponse> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(S3Exception.builder().statusCode(412).build());
    when(client.deleteObject(any(DeleteObjectRequest.class))).thenReturn(failedFuture);

    // Act & Assert
    assertThatCode(() -> wrapper.delete(ANY_OBJECT_KEY, ANY_ETAG))
        .isInstanceOf(PreconditionFailedException.class);
  }

  @Test
  public void delete_WithVersion_S3ExceptionWith409Thrown_ShouldThrowPreconditionFailedException() {
    // Arrange
    CompletableFuture<DeleteObjectResponse> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(S3Exception.builder().statusCode(409).build());
    when(client.deleteObject(any(DeleteObjectRequest.class))).thenReturn(failedFuture);

    // Act & Assert
    assertThatCode(() -> wrapper.delete(ANY_OBJECT_KEY, ANY_ETAG))
        .isInstanceOf(PreconditionFailedException.class);
  }

  @Test
  public void delete_WithVersion_OtherS3ExceptionThrown_ShouldThrowObjectStorageWrapperException() {
    // Arrange
    CompletableFuture<DeleteObjectResponse> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(S3Exception.builder().statusCode(500).build());
    when(client.deleteObject(any(DeleteObjectRequest.class))).thenReturn(failedFuture);

    // Act & Assert
    assertThatCode(() -> wrapper.delete(ANY_OBJECT_KEY, ANY_ETAG))
        .isInstanceOf(ObjectStorageWrapperException.class);
  }

  @Test
  public void deleteByPrefix_PrefixGiven_ShouldDeleteAllObjectsWithThePrefix() throws Exception {
    // Arrange
    String objectKey1 = ANY_PREFIX + "test1/object1";
    String objectKey2 = ANY_PREFIX + "test1/object2";
    String objectKey3 = ANY_PREFIX + "test2/object3";

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
    ListObjectsV2Publisher paginator = mock(ListObjectsV2Publisher.class);
    when(client.listObjectsV2Paginator(any(ListObjectsV2Request.class))).thenReturn(paginator);

    @SuppressWarnings("unchecked")
    Consumer<ListObjectsV2Response> consumer = any(Consumer.class);
    when(paginator.subscribe(consumer))
        .thenAnswer(
            invocation -> {
              Consumer<ListObjectsV2Response> actualConsumer = invocation.getArgument(0);
              actualConsumer.accept(response1);
              actualConsumer.accept(response2);
              return CompletableFuture.completedFuture(null);
            });

    DeleteObjectsResponse deleteObjectsResponse = mock(DeleteObjectsResponse.class);
    when(client.deleteObjects(any(DeleteObjectsRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(deleteObjectsResponse));

    // Act
    wrapper.deleteByPrefix(ANY_PREFIX);

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
    // Mock paginated response with no objects
    ListObjectsV2Response response = mock(ListObjectsV2Response.class);
    when(response.contents()).thenReturn(Collections.emptyList());

    // Mock the paginator
    ListObjectsV2Publisher paginator = mock(ListObjectsV2Publisher.class);
    when(client.listObjectsV2Paginator(any(ListObjectsV2Request.class))).thenReturn(paginator);

    @SuppressWarnings("unchecked")
    Consumer<ListObjectsV2Response> consumer = any(Consumer.class);
    when(paginator.subscribe(consumer))
        .thenAnswer(
            invocation -> {
              Consumer<ListObjectsV2Response> actualConsumer = invocation.getArgument(0);
              actualConsumer.accept(response);
              return CompletableFuture.completedFuture(null);
            });

    // Act
    wrapper.deleteByPrefix(ANY_PREFIX);

    // Assert
    verify(client, org.mockito.Mockito.never()).deleteObjects(any(DeleteObjectsRequest.class));
  }

  @Test
  public void deleteByPrefix_S3ExceptionThrown_ShouldThrowObjectStorageWrapperException() {
    // Arrange
    ListObjectsV2Publisher paginator = mock(ListObjectsV2Publisher.class);
    when(client.listObjectsV2Paginator(any(ListObjectsV2Request.class))).thenReturn(paginator);

    CompletableFuture<Void> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(S3Exception.builder().statusCode(500).build());
    @SuppressWarnings("unchecked")
    Consumer<ListObjectsV2Response> consumer = any(Consumer.class);
    when(paginator.subscribe(consumer)).thenReturn(failedFuture);

    // Act & Assert
    assertThatCode(() -> wrapper.deleteByPrefix(ANY_PREFIX))
        .isInstanceOf(ObjectStorageWrapperException.class);
  }

  @Test
  public void close_ShouldCloseTheClient() {
    // Arrange

    // Act
    wrapper.close();

    // Assert
    verify(client).close();
  }
}
