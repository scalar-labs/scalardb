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

public class S3Wrapper implements ObjectStorageWrapper {
  public static final int BATCH_DELETE_SIZE_LIMIT = 1000;
  private final S3AsyncClient client;
  private final String bucket;

  public S3Wrapper(S3Config config) {
    this.client =
        S3AsyncClient.builder()
            .region(Region.of(config.getRegion()))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(config.getUsername(), config.getPassword())))
            .httpClientBuilder(
                AwsCrtAsyncHttpClient.builder()
                    .maxConcurrency(config.getParallelUploadMaxParallelism()))
            .multipartConfiguration(
                MultipartConfiguration.builder()
                    .minimumPartSizeInBytes(config.getParallelUploadBlockSizeInBytes())
                    .thresholdInBytes(config.getParallelUploadThresholdInBytes())
                    .build())
            .overrideConfiguration(
                ClientOverrideConfiguration.builder()
                    .apiCallTimeout(Duration.ofSeconds(config.getRequestTimeoutInSeconds()))
                    .build())
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
      Throwable cause = e.getCause();
      if (cause instanceof S3Exception) {
        S3Exception s3Exception = (S3Exception) cause;
        if (s3Exception.statusCode() == S3ErrorCode.NOT_FOUND.get()) {
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
        S3Exception s3Exception = (S3Exception) cause;
        if (s3Exception.statusCode() == S3ErrorCode.CONFLICT.get()) {
          throw new ConflictOccurredException(
              String.format("Failed to insert the object with key '%s' due to conflict", key),
              s3Exception);
        }
        if (s3Exception.statusCode() == S3ErrorCode.PRECONDITION_FAILED.get()) {
          throw new PreconditionFailedException(
              String.format(
                  "Failed to insert the object with key '%s' due to precondition failure", key),
              s3Exception);
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
        S3Exception s3Exception = (S3Exception) cause;
        if (s3Exception.statusCode() == S3ErrorCode.NOT_FOUND.get()
            || s3Exception.statusCode() == S3ErrorCode.CONFLICT.get()
            || s3Exception.statusCode() == S3ErrorCode.PRECONDITION_FAILED.get()) {
          throw new PreconditionFailedException(
              String.format(
                  "Failed to update the object with key '%s' due to precondition failure", key),
              s3Exception);
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
        S3Exception s3Exception = (S3Exception) cause;
        if (s3Exception.statusCode() == S3ErrorCode.NOT_FOUND.get()) {
          throw new PreconditionFailedException(
              String.format(
                  "Failed to delete the object with key '%s' due to precondition failure", key),
              s3Exception);
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
        S3Exception s3Exception = (S3Exception) cause;
        if (s3Exception.statusCode() == S3ErrorCode.NOT_FOUND.get()
            || s3Exception.statusCode() == S3ErrorCode.CONFLICT.get()
            || s3Exception.statusCode() == S3ErrorCode.PRECONDITION_FAILED.get()) {
          throw new PreconditionFailedException(
              String.format(
                  "Failed to delete the object with key '%s' due to precondition failure", key),
              s3Exception);
        }
      }
      throw new ObjectStorageWrapperException(
          String.format("Failed to delete the object with key '%s'", key), e);
    }
  }

  @Override
  public void deleteByPrefix(String prefix) throws ObjectStorageWrapperException {
    try {
      List<ObjectIdentifier> identifiers = new ArrayList<>();

      client
          .listObjectsV2Paginator(
              ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build())
          .subscribe(
              response ->
                  response
                      .contents()
                      .forEach(
                          s3Object ->
                              identifiers.add(
                                  ObjectIdentifier.builder().key(s3Object.key()).build())))
          .join();

      if (!identifiers.isEmpty()) {
        final int totalSize = identifiers.size();
        for (int i = 0; i < totalSize; i += BATCH_DELETE_SIZE_LIMIT) {
          int endIndex = Math.min(i + BATCH_DELETE_SIZE_LIMIT, totalSize);
          List<ObjectIdentifier> batch = identifiers.subList(i, endIndex);
          client
              .deleteObjects(
                  DeleteObjectsRequest.builder()
                      .bucket(bucket)
                      .delete(Delete.builder().objects(batch).build())
                      .build())
              .join();
        }
      }
    } catch (Exception e) {
      throw new ObjectStorageWrapperException(
          String.format("Failed to delete the objects with prefix '%s'", prefix), e);
    }
  }

  @Override
  public void close() {
    client.close();
  }
}
