package com.scalar.db.storage.objectstorage.cloudstorage;

import com.google.cloud.ServiceOptions;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageBatch;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.storage.objectstorage.ConflictOccurredException;
import com.scalar.db.storage.objectstorage.ObjectStorageWrapper;
import com.scalar.db.storage.objectstorage.ObjectStorageWrapperException;
import com.scalar.db.storage.objectstorage.ObjectStorageWrapperResponse;
import com.scalar.db.storage.objectstorage.PreconditionFailedException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class CloudStorageWrapper implements ObjectStorageWrapper {
  // Batch API has a limit of 100 operations per request
  public static final int BATCH_DELETE_SIZE_LIMIT = 100;

  // The maximum number of retries for the get operation, which is performed when the object is
  // updated between the metadata retrieval and the payload download
  @VisibleForTesting static final int GET_MAX_RETRY_COUNT = 3;

  private final Storage storage;
  // A client that sends each request only once. See delete(String, String) for why it is needed
  private final Storage nonRetryingStorage;
  private final String bucket;
  private final Integer uploadChunkSizeBytes;

  public CloudStorageWrapper(CloudStorageConfig config) {
    this(
        config,
        StorageOptions.newBuilder()
            .setProjectId(config.getProjectId())
            .setCredentials(config.getCredentials())
            .build());
  }

  @VisibleForTesting
  CloudStorageWrapper(CloudStorageConfig config, StorageOptions options) {
    this(
        config,
        options.getService(),
        options
            .toBuilder()
            .setRetrySettings(ServiceOptions.getNoRetrySettings())
            .build()
            .getService());
  }

  @VisibleForTesting
  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public CloudStorageWrapper(
      CloudStorageConfig config, Storage storage, Storage nonRetryingStorage) {
    this.storage = storage;
    this.nonRetryingStorage = nonRetryingStorage;
    this.bucket = config.getBucket();
    uploadChunkSizeBytes = config.getUploadChunkSizeBytes().orElse(null);
  }

  @Override
  public Optional<ObjectStorageWrapperResponse> get(String key)
      throws ObjectStorageWrapperException {
    StorageException lastPreconditionFailure = null;
    for (int retryCount = 0; retryCount <= GET_MAX_RETRY_COUNT; retryCount++) {
      try {
        Blob blob = storage.get(BlobId.of(bucket, key));
        if (blob == null) {
          return Optional.empty();
        }
        long generation = blob.getGeneration();
        // Download the payload with the generation precondition instead of specifying the
        // generation in the blob ID. With the precondition, a concurrent deletion results in
        // NOT_FOUND while a concurrent update results in PRECONDITION_FAILED, which allows the two
        // cases to be distinguished. If the generation were specified in the blob ID, both cases
        // would result in NOT_FOUND since the specified generation no longer exists unless object
        // versioning is enabled.
        byte[] payload =
            storage.readAllBytes(
                BlobId.of(bucket, key), Storage.BlobSourceOption.generationMatch(generation));
        return Optional.of(
            new ObjectStorageWrapperResponse(
                new String(payload, StandardCharsets.UTF_8), String.valueOf(generation)));
      } catch (StorageException e) {
        if (e.getCode() == CloudStorageErrorCode.NOT_FOUND.get()) {
          // The object was deleted after the metadata was retrieved, so it is regarded as absent
          return Optional.empty();
        }
        if (e.getCode() != CloudStorageErrorCode.PRECONDITION_FAILED.get()) {
          throw new ObjectStorageWrapperException(
              String.format("Failed to get the object with key '%s'", key), e);
        }
        // The object was updated after the metadata was retrieved. Retry from the metadata
        // retrieval to get the payload and its generation consistently.
        lastPreconditionFailure = e;
      } catch (Exception e) {
        throw new ObjectStorageWrapperException(
            String.format("Failed to get the object with key '%s'", key), e);
      }
    }
    throw new ConflictOccurredException(
        String.format(
            "Failed to get the object with key '%s' after retrying %d times due to conflicts",
            key, GET_MAX_RETRY_COUNT),
        lastPreconditionFailure);
  }

  @Override
  public Set<String> getKeys(String prefix) throws ObjectStorageWrapperException {
    try {
      Iterable<Blob> blobs =
          storage.list(bucket, Storage.BlobListOption.prefix(prefix)).iterateAll();
      return StreamSupport.stream(blobs.spliterator(), false)
          .map(Blob::getName)
          .collect(Collectors.toSet());
    } catch (Exception e) {
      throw new ObjectStorageWrapperException(
          String.format("Failed to get the object keys with prefix '%s'", prefix), e);
    }
  }

  @Override
  public void insert(String key, String object) throws ObjectStorageWrapperException {
    try {
      Storage.BlobWriteOption precondition = Storage.BlobWriteOption.doesNotExist();
      writeData(key, object, precondition);
    } catch (StorageException e) {
      if (e.getCode() == CloudStorageErrorCode.PRECONDITION_FAILED.get()) {
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
      Storage.BlobWriteOption precondition =
          Storage.BlobWriteOption.generationMatch(Long.parseLong(version));
      writeData(key, object, precondition);
    } catch (StorageException e) {
      if (e.getCode() == CloudStorageErrorCode.PRECONDITION_FAILED.get()) {
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
      if (!storage.delete(BlobId.of(bucket, key))) {
        throw new PreconditionFailedException(
            String.format(
                "Failed to delete the object with key '%s' due to precondition failure", key));
      }
    } catch (PreconditionFailedException e) {
      throw e;
    } catch (Exception e) {
      throw new ObjectStorageWrapperException(
          String.format("Failed to delete the object with key '%s'", key), e);
    }
  }

  @Override
  public void delete(String key, String version) throws ObjectStorageWrapperException {
    try {
      // The request is sent only once. The Cloud Storage client resends a deletion with a
      // precondition after a lost response or a server error, and a resent request can fail
      // because of its own earlier, applied attempt, which would report the applied deletion as
      // not applied
      if (!nonRetryingStorage.delete(
          BlobId.of(bucket, key),
          Storage.BlobSourceOption.generationMatch(Long.parseLong(version)))) {
        throw new PreconditionFailedException(
            String.format(
                "Failed to delete the object with key '%s' due to precondition failure", key));
      }
    } catch (PreconditionFailedException e) {
      throw e;
    } catch (StorageException e) {
      if (e.getCode() == CloudStorageErrorCode.PRECONDITION_FAILED.get()) {
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
      // Collect all blob IDs with the specified prefix
      Iterable<Blob> blobs =
          storage.list(bucket, Storage.BlobListOption.prefix(prefix)).iterateAll();
      List<BlobId> blobIds =
          StreamSupport.stream(blobs.spliterator(), false)
              .map(blob -> BlobId.of(bucket, blob.getName()))
              .collect(Collectors.toList());
      // Delete blobs in batches
      for (int i = 0; i < blobIds.size(); i += BATCH_DELETE_SIZE_LIMIT) {
        int endIndex = Math.min(i + BATCH_DELETE_SIZE_LIMIT, blobIds.size());
        List<BlobId> batch = blobIds.subList(i, endIndex);
        StorageBatch storageBatch = storage.batch();
        for (BlobId blobId : batch) {
          storageBatch.delete(blobId);
        }
        storageBatch.submit();
      }
    } catch (Exception e) {
      throw new ObjectStorageWrapperException(
          String.format("Failed to delete the objects with prefix '%s'", prefix), e);
    }
  }

  @Override
  public void close() throws ObjectStorageWrapperException {
    // Closes both clients, even if closing one of them fails
    try (Storage ignored = storage;
        Storage ignoredNonRetrying = nonRetryingStorage) {
      // Nothing to do other than closing the clients
    } catch (Exception e) {
      throw new ObjectStorageWrapperException("Failed to close the storage wrapper", e);
    }
  }

  private void writeData(String key, String object, Storage.BlobWriteOption precondition)
      throws IOException {
    byte[] data = object.getBytes(StandardCharsets.UTF_8);
    BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of(bucket, key)).build();

    // Unlike a deletion with a precondition, this write can use the client that resends requests.
    // The write is a resumable upload, whose precondition is sent when the upload session starts.
    // When the response to the request that finalizes the upload is lost, the client queries the
    // state of the same session, which returns the finalized object, instead of starting a new
    // session and evaluating the precondition again
    try (WriteChannel writer = storage.writer(blobInfo, precondition)) {
      if (uploadChunkSizeBytes != null) {
        writer.setChunkSize(uploadChunkSizeBytes);
      }
      ByteBuffer buffer = ByteBuffer.wrap(data);
      while (buffer.hasRemaining()) {
        writer.write(buffer);
      }
    }
  }
}
