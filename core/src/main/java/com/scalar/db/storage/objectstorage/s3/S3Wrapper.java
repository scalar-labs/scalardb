package com.scalar.db.storage.objectstorage.s3;

import com.scalar.db.storage.objectstorage.ConflictOccurredException;
import com.scalar.db.storage.objectstorage.ObjectStorageWrapper;
import com.scalar.db.storage.objectstorage.ObjectStorageWrapperException;
import com.scalar.db.storage.objectstorage.ObjectStorageWrapperResponse;
import com.scalar.db.storage.objectstorage.PreconditionFailedException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3Wrapper implements ObjectStorageWrapper {
  public final int BATCH_DELETE_SIZE = 1000;
  private final S3Client client;
  private final String bucket;

  public S3Wrapper(S3Config config) {
    S3ClientBuilder builder = S3Client.builder();
    this.client =
        builder
            .credentialsProvider(DefaultCredentialsProvider.builder().build())
            .region(Region.of(config.getRegion()))
            .forcePathStyle(true)
            .build();
    this.bucket = config.getBucket();
  }

  @Override
  public Optional<ObjectStorageWrapperResponse> get(String key)
      throws ObjectStorageWrapperException {
    try {
      ResponseBytes<GetObjectResponse> response =
          client.getObjectAsBytes(GetObjectRequest.builder().bucket(bucket).key(key).build());
      String data = response.asUtf8String();
      String eTag = response.response().eTag();
      return Optional.of(new ObjectStorageWrapperResponse(data, eTag));
    } catch (S3Exception e) {
      if (e.statusCode() == S3ErrorCode.NOT_FOUND.get()) {
        return Optional.empty();
      }
      throw new ObjectStorageWrapperException(
          String.format("Failed to get the object with key '%s'", key), e);
    } catch (Exception e) {
      throw new ObjectStorageWrapperException(
          String.format("Failed to get the object with key '%s'", key), e);
    }
  }

  @Override
  public Set<String> getKeys(String prefix) throws ObjectStorageWrapperException {
    try {
      return client
          .listObjectsV2Paginator(
              ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build())
          .stream()
          .flatMap(page -> page.contents().stream())
          .map(S3Object::key)
          .collect(Collectors.toSet());
    } catch (Exception e) {
      throw new ObjectStorageWrapperException(
          "Failed to get the object keys with prefix: " + prefix + " from S3", e);
    }
  }

  @Override
  public void insert(String key, String object) throws ObjectStorageWrapperException {
    try {
      client.putObject(
          PutObjectRequest.builder().bucket(bucket).key(key).ifNoneMatch("*").build(),
          RequestBody.fromString(object));
    } catch (S3Exception e) {
      if (e.statusCode() == S3ErrorCode.CONFLICT.get()) {
        throw new ConflictOccurredException(
            String.format("Failed to insert the object with key '%s' due to conflict", key), e);
      }
      if (e.statusCode() == S3ErrorCode.PRECONDITION_FAILED.get()) {
        throw new PreconditionFailedException(
            String.format(
                "Failed to insert the object with key '%s' due to precondition failure", key),
            e);
      }
      throw new ObjectStorageWrapperException(
          String.format("Failed to insert the object with key '%s'", key), e);
    } catch (Exception e) {
      throw new ObjectStorageWrapperException(
          String.format("Failed to insert the object with key '%s'", key), e);
    }
  }

  @Override
  public void update(String key, String object, String version)
      throws ObjectStorageWrapperException {
    try {
      client.putObject(
          PutObjectRequest.builder().bucket(bucket).key(key).ifMatch(version).build(),
          RequestBody.fromString(object));
    } catch (S3Exception e) {
      if (e.statusCode() == S3ErrorCode.NOT_FOUND.get()
          || e.statusCode() == S3ErrorCode.CONFLICT.get()
          || e.statusCode() == S3ErrorCode.PRECONDITION_FAILED.get()) {
        throw new PreconditionFailedException(
            String.format(
                "Failed to update the object with key '%s' due to precondition failure", key),
            e);
      }
      throw new ObjectStorageWrapperException(
          String.format("Failed to update the object with key '%s'", key), e);
    } catch (Exception e) {
      throw new ObjectStorageWrapperException(
          String.format("Failed to update the object with key '%s'", key), e);
    }
  }

  @Override
  public void delete(String key) throws ObjectStorageWrapperException {
    try {
      client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build());
    } catch (S3Exception e) {
      if (e.statusCode() == S3ErrorCode.NOT_FOUND.get()) {
        throw new PreconditionFailedException(
            String.format(
                "Failed to delete the object with key '%s' due to precondition failure", key),
            e);
      }
      throw new ObjectStorageWrapperException(
          String.format("Failed to delete the object with key '%s'", key), e);
    } catch (Exception e) {
      throw new ObjectStorageWrapperException(
          String.format("Failed to delete the object with key '%s'", key), e);
    }
  }

  @Override
  public void delete(String key, String version) throws ObjectStorageWrapperException {
    try {
      client.deleteObject(
          DeleteObjectRequest.builder().bucket(bucket).key(key).ifMatch(version).build());
    } catch (S3Exception e) {
      if (e.statusCode() == S3ErrorCode.NOT_FOUND.get()
          || e.statusCode() == S3ErrorCode.CONFLICT.get()
          || e.statusCode() == S3ErrorCode.PRECONDITION_FAILED.get()) {
        throw new PreconditionFailedException(
            String.format(
                "Failed to delete the object with key '%s' due to precondition failure", key),
            e);
      }
      throw new ObjectStorageWrapperException(
          String.format("Failed to delete the object with key '%s'", key), e);
    } catch (Exception e) {
      throw new ObjectStorageWrapperException(
          String.format("Failed to delete the object with key '%s'", key), e);
    }
  }

  @Override
  public void deleteByPrefix(String prefix) throws ObjectStorageWrapperException {
    try {
      List<ObjectIdentifier> identifiers =
          client
              .listObjectsV2Paginator(
                  ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build())
              .stream()
              .flatMap(page -> page.contents().stream())
              .map(s3Object -> ObjectIdentifier.builder().key(s3Object.key()).build())
              .collect(Collectors.toList());
      if (!identifiers.isEmpty()) {
        final int totalSize = identifiers.size();
        for (int i = 0; i < totalSize; i += BATCH_DELETE_SIZE) {
          int endIndex = Math.min(i + BATCH_DELETE_SIZE, totalSize);
          List<ObjectIdentifier> batch = identifiers.subList(i, endIndex);
          client.deleteObjects(
              DeleteObjectsRequest.builder()
                  .bucket(bucket)
                  .delete(Delete.builder().objects(batch).build())
                  .build());
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
