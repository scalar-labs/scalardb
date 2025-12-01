package com.scalar.db.storage.objectstorage.s3;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.storage.objectstorage.ConflictOccurredException;
import com.scalar.db.storage.objectstorage.ObjectStorageWrapper;
import com.scalar.db.storage.objectstorage.ObjectStorageWrapperException;
import com.scalar.db.storage.objectstorage.ObjectStorageWrapperResponse;
import com.scalar.db.storage.objectstorage.PreconditionFailedException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.multipart.MultipartConfiguration;

@ThreadSafe
public class S3Wrapper implements ObjectStorageWrapper {
  // DeleteObjects API has a limit of 1000 objects per request
  public static final int BATCH_DELETE_SIZE_LIMIT = 1000;

  private final S3AsyncClient client;
  private final String bucket;

  public S3Wrapper(S3Config config) {
    AwsCrtAsyncHttpClient.Builder httpClientBuilder = AwsCrtAsyncHttpClient.builder();
    if (config.getMultipartUploadMaxConcurrency().isPresent()) {
      httpClientBuilder.maxConcurrency(config.getMultipartUploadMaxConcurrency().get());
    }
    MultipartConfiguration.Builder multipartConfigBuilder = MultipartConfiguration.builder();
    if (config.getMultipartUploadPartSizeBytes().isPresent()) {
      multipartConfigBuilder.minimumPartSizeInBytes(config.getMultipartUploadPartSizeBytes().get());
    }
    if (config.getMultipartUploadThresholdSizeBytes().isPresent()) {
      multipartConfigBuilder.thresholdInBytes(config.getMultipartUploadThresholdSizeBytes().get());
    }
    ClientOverrideConfiguration.Builder overrideConfigBuilder =
        ClientOverrideConfiguration.builder();
    if (config.getRequestTimeoutSecs().isPresent()) {
      overrideConfigBuilder.apiCallTimeout(
          Duration.ofSeconds(config.getRequestTimeoutSecs().get()));
    }

    this.client =
        S3AsyncClient.builder()
            .region(Region.of(config.getRegion()))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(config.getUsername(), config.getPassword())))
            .httpClientBuilder(httpClientBuilder)
            .multipartConfiguration(multipartConfigBuilder.build())
            .overrideConfiguration(overrideConfigBuilder.build())
            .build();
    this.bucket = config.getBucket();
  }

  @VisibleForTesting
  S3Wrapper(S3Config config, S3AsyncClient client) {
    this.client = client;
    this.bucket = config.getBucket();
  }

  @Override
  public Optional<ObjectStorageWrapperResponse> get(String key)
      throws ObjectStorageWrapperException {
    try {
      ResponseBytes<GetObjectResponse> response =
          client
              .getObject(
                  GetObjectRequest.builder().bucket(bucket).key(key).build(),
                  AsyncResponseTransformer.toBytes())
              .join();
      String data = response.asUtf8String();
      String eTag = response.response().eTag();
      return Optional.of(new ObjectStorageWrapperResponse(data, eTag));
    } catch (Exception e) {
      Optional<S3Exception> s3Exception = findS3Exception(e);
      if (s3Exception.isPresent()) {
        if (s3Exception.get().statusCode() == S3ErrorCode.NOT_FOUND.get()) {
          return Optional.empty();
        }
      }
      throw new ObjectStorageWrapperException(
          String.format("Failed to get the object with key '%s'", key), e);
    }
  }

  @Override
  public Set<String> getKeys(String prefix) throws ObjectStorageWrapperException {
    try {
      List<String> keys = new ArrayList<>();

      client
          .listObjectsV2Paginator(
              ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build())
          .subscribe(response -> response.contents().forEach(s3Object -> keys.add(s3Object.key())))
          .join();

      return new HashSet<>(keys);
    } catch (Exception e) {
      throw new ObjectStorageWrapperException(
          String.format("Failed to get the object keys with prefix: '%s'", prefix), e);
    }
  }

  @Override
  public void insert(String key, String object) throws ObjectStorageWrapperException {
    try {
      client
          .putObject(
              PutObjectRequest.builder().bucket(bucket).key(key).ifNoneMatch("*").build(),
              AsyncRequestBody.fromString(object))
          .join();
    } catch (Exception e) {
      Throwable cause = e.getCause();
      if (cause instanceof S3Exception) {
        Optional<S3Exception> s3Exception = findS3Exception(e);
        if (s3Exception.isPresent()) {
          if (s3Exception.get().statusCode() == S3ErrorCode.CONFLICT.get()) {
            throw new ConflictOccurredException(
                String.format("Failed to insert the object with key '%s' due to conflict", key),
                s3Exception.get());
          }
          if (s3Exception.get().statusCode() == S3ErrorCode.PRECONDITION_FAILED.get()) {
            throw new PreconditionFailedException(
                String.format(
                    "Failed to insert the object with key '%s' due to precondition failure", key),
                s3Exception.get());
          }
        }
      }
      throw new ObjectStorageWrapperException(
          String.format("Failed to insert the object with key '%s'", key), e);
    }
  }

  @Override
  public void update(String key, String object, String version)
      throws ObjectStorageWrapperException {
    try {
      client
          .putObject(
              PutObjectRequest.builder().bucket(bucket).key(key).ifMatch(version).build(),
              AsyncRequestBody.fromString(object))
          .join();
    } catch (Exception e) {
      Throwable cause = e.getCause();
      if (cause instanceof S3Exception) {
        Optional<S3Exception> s3Exception = findS3Exception(e);
        if (s3Exception.isPresent()) {
          if (s3Exception.get().statusCode() == S3ErrorCode.CONFLICT.get()) {
            throw new ConflictOccurredException(
                String.format("Failed to update the object with key '%s' due to conflict", key),
                s3Exception.get());
          }
          if (s3Exception.get().statusCode() == S3ErrorCode.NOT_FOUND.get()
              || s3Exception.get().statusCode() == S3ErrorCode.PRECONDITION_FAILED.get()) {
            throw new PreconditionFailedException(
                String.format(
                    "Failed to update the object with key '%s' due to precondition failure", key),
                s3Exception.get());
          }
        }
      }
      throw new ObjectStorageWrapperException(
          String.format("Failed to update the object with key '%s'", key), e);
    }
  }

  @Override
  public void delete(String key) throws ObjectStorageWrapperException {
    try {
      client
          .deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).ifMatch("*").build())
          .join();
    } catch (Exception e) {
      Throwable cause = e.getCause();
      if (cause instanceof S3Exception) {
        Optional<S3Exception> s3Exception = findS3Exception(e);
        if (s3Exception.isPresent()) {
          if (s3Exception.get().statusCode() == S3ErrorCode.CONFLICT.get()) {
            throw new ConflictOccurredException(
                String.format("Failed to delete the object with key '%s' due to conflict", key),
                s3Exception.get());
          }
          if (s3Exception.get().statusCode() == S3ErrorCode.NOT_FOUND.get()) {
            throw new PreconditionFailedException(
                String.format(
                    "Failed to delete the object with key '%s' due to precondition failure", key),
                s3Exception.get());
          }
        }
      }
      throw new ObjectStorageWrapperException(
          String.format("Failed to delete the object with key '%s'", key), e);
    }
  }

  @Override
  public void delete(String key, String version) throws ObjectStorageWrapperException {
    try {
      client
          .deleteObject(
              DeleteObjectRequest.builder().bucket(bucket).key(key).ifMatch(version).build())
          .join();
    } catch (Exception e) {
      Throwable cause = e.getCause();
      if (cause instanceof S3Exception) {
        Optional<S3Exception> s3Exception = findS3Exception(e);
        if (s3Exception.isPresent()) {
          if (s3Exception.get().statusCode() == S3ErrorCode.CONFLICT.get()) {
            throw new ConflictOccurredException(
                String.format("Failed to delete the object with key '%s' due to conflict", key),
                s3Exception.get());
          }
          if (s3Exception.get().statusCode() == S3ErrorCode.NOT_FOUND.get()
              || s3Exception.get().statusCode() == S3ErrorCode.PRECONDITION_FAILED.get()) {
            throw new PreconditionFailedException(
                String.format(
                    "Failed to delete the object with key '%s' due to precondition failure", key),
                s3Exception.get());
          }
        }
      }
      throw new ObjectStorageWrapperException(
          String.format("Failed to delete the object with key '%s'", key), e);
    }
  }

  @Override
  public void deleteByPrefix(String prefix) throws ObjectStorageWrapperException {
    try {
      client
          .listObjectsV2Paginator(
              ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build())
          .subscribe(
              response -> {
                if (!response.contents().isEmpty()) {
                  List<ObjectIdentifier> pageObjects = new ArrayList<>();
                  // Collect all objects from this page
                  response
                      .contents()
                      .forEach(
                          s3Object ->
                              pageObjects.add(
                                  ObjectIdentifier.builder().key(s3Object.key()).build()));
                  // Delete objects from this page in batches
                  for (int i = 0; i < pageObjects.size(); i += BATCH_DELETE_SIZE_LIMIT) {
                    int endIndex = Math.min(i + BATCH_DELETE_SIZE_LIMIT, pageObjects.size());
                    List<ObjectIdentifier> batch = pageObjects.subList(i, endIndex);
                    client
                        .deleteObjects(
                            DeleteObjectsRequest.builder()
                                .bucket(bucket)
                                .delete(Delete.builder().objects(batch).build())
                                .build())
                        .join();
                  }
                }
              })
          .join();
    } catch (Exception e) {
      throw new ObjectStorageWrapperException(
          String.format("Failed to delete the objects with prefix '%s'", prefix), e);
    }
  }

  @Override
  public void close() throws ObjectStorageWrapperException {
    try {
      client.close();
    } catch (Exception e) {
      throw new ObjectStorageWrapperException("Failed to close the storage wrapper", e);
    }
  }

  private Optional<S3Exception> findS3Exception(Throwable throwable) {
    Throwable current = throwable;
    while (current != null) {
      if (current instanceof S3Exception) {
        return Optional.of((S3Exception) current);
      }
      current = current.getCause();
    }
    return Optional.empty();
  }
}
