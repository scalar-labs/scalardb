package com.scalar.db.storage.objectstorage.blobstorage;

import com.azure.core.http.HttpHeaderName;
import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobDownloadContentResponse;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.storage.objectstorage.ObjectStorageWrapper;
import com.scalar.db.storage.objectstorage.ObjectStorageWrapperException;
import com.scalar.db.storage.objectstorage.ObjectStorageWrapperResponse;
import com.scalar.db.storage.objectstorage.PreconditionFailedException;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class BlobStorageWrapper implements ObjectStorageWrapper {
  private final BlobContainerClient client;
  private final Duration requestTimeoutSecs;
  private final ParallelTransferOptions parallelTransferOptions;

  public BlobStorageWrapper(BlobStorageConfig config) {
    this(
        config,
        new BlobServiceClientBuilder()
            .endpoint(config.getEndpoint())
            .credential(new StorageSharedKeyCredential(config.getUsername(), config.getPassword()))
            .addPolicy(new ConditionalRequestAttemptCounter())
            .buildClient()
            .getBlobContainerClient(config.getBucket()));
  }

  @VisibleForTesting
  BlobStorageWrapper(BlobStorageConfig config, BlobContainerClient client) {
    this.client = client;
    this.requestTimeoutSecs = config.getRequestTimeoutSecs().map(Duration::ofSeconds).orElse(null);
    this.parallelTransferOptions = new ParallelTransferOptions();
    if (config.getParallelUploadBlockSizeBytes().isPresent()) {
      parallelTransferOptions.setBlockSizeLong(config.getParallelUploadBlockSizeBytes().get());
    }
    if (config.getParallelUploadMaxConcurrency().isPresent()) {
      parallelTransferOptions.setMaxConcurrency(config.getParallelUploadMaxConcurrency().get());
    }
    if (config.getParallelUploadThresholdSizeBytes().isPresent()) {
      parallelTransferOptions.setMaxSingleUploadSizeLong(
          config.getParallelUploadThresholdSizeBytes().get());
    }
  }

  @Override
  public Optional<ObjectStorageWrapperResponse> get(String key)
      throws ObjectStorageWrapperException {
    try {
      BlobClient blobClient = client.getBlobClient(key);
      BlobDownloadContentResponse response =
          blobClient.downloadContentWithResponse(null, null, requestTimeoutSecs, null);
      String data = response.getValue().toString();
      String eTag = response.getHeaders().getValue(HttpHeaderName.ETAG);
      return Optional.of(new ObjectStorageWrapperResponse(data, eTag));
    } catch (BlobStorageException e) {
      if (e.getErrorCode().equals(BlobErrorCode.BLOB_NOT_FOUND)) {
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
      return client.listBlobs(new ListBlobsOptions().setPrefix(prefix), requestTimeoutSecs).stream()
          .map(BlobItem::getName)
          .collect(Collectors.toSet());
    } catch (Exception e) {
      throw new ObjectStorageWrapperException(
          String.format("Failed to get the object keys with prefix '%s'", prefix), e);
    }
  }

  @Override
  public void insert(String key, String object) throws ObjectStorageWrapperException {
    AtomicInteger attempts = new AtomicInteger();
    try {
      BlobClient blobClient = client.getBlobClient(key);
      BlobParallelUploadOptions options =
          new BlobParallelUploadOptions(BinaryData.fromString(object))
              .setRequestConditions(new BlobRequestConditions().setIfNoneMatch("*"))
              .setParallelTransferOptions(parallelTransferOptions);
      blobClient.uploadWithResponse(
          options, requestTimeoutSecs, ConditionalRequestAttemptCounter.newContext(attempts));
    } catch (BlobStorageException e) {
      // A resent request can fail because of its own earlier, applied attempt, so the write must
      // not be reported as not applied
      if (earlierAttemptMayHaveBeenApplied(attempts)) {
        throw new ObjectStorageWrapperException(
            String.format(
                "The object with key '%s' may have been inserted because the Azure SDK retried the request, so the outcome is unknown",
                key),
            e);
      }
      if (e.getErrorCode().equals(BlobErrorCode.BLOB_ALREADY_EXISTS)) {
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
    AtomicInteger attempts = new AtomicInteger();
    try {
      BlobClient blobClient = client.getBlobClient(key);
      BlobParallelUploadOptions options =
          new BlobParallelUploadOptions(BinaryData.fromString(object))
              .setRequestConditions(new BlobRequestConditions().setIfMatch(version))
              .setParallelTransferOptions(parallelTransferOptions);
      blobClient.uploadWithResponse(
          options, requestTimeoutSecs, ConditionalRequestAttemptCounter.newContext(attempts));
    } catch (BlobStorageException e) {
      if (earlierAttemptMayHaveBeenApplied(attempts)) {
        throw new ObjectStorageWrapperException(
            String.format(
                "The object with key '%s' may have been updated because the Azure SDK retried the request, so the outcome is unknown",
                key),
            e);
      }
      if (e.getErrorCode().equals(BlobErrorCode.CONDITION_NOT_MET)
          || e.getErrorCode().equals(BlobErrorCode.BLOB_NOT_FOUND)) {
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
      BlobClient blobClient = client.getBlobClient(key);
      blobClient.delete();
    } catch (BlobStorageException e) {
      if (e.getErrorCode().equals(BlobErrorCode.BLOB_NOT_FOUND)) {
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
    AtomicInteger attempts = new AtomicInteger();
    try {
      BlobClient blobClient = client.getBlobClient(key);
      blobClient.deleteWithResponse(
          null,
          new BlobRequestConditions().setIfMatch(version),
          requestTimeoutSecs,
          ConditionalRequestAttemptCounter.newContext(attempts));
    } catch (BlobStorageException e) {
      if (earlierAttemptMayHaveBeenApplied(attempts)) {
        throw new ObjectStorageWrapperException(
            String.format(
                "The object with key '%s' may have been deleted because the Azure SDK retried the request, so the outcome is unknown",
                key),
            e);
      }
      if (e.getErrorCode().equals(BlobErrorCode.CONDITION_NOT_MET)
          || e.getErrorCode().equals(BlobErrorCode.BLOB_NOT_FOUND)) {
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
      client
          .listBlobs(new ListBlobsOptions().setPrefix(prefix), requestTimeoutSecs)
          .forEach(
              blobItem -> {
                try {
                  client.getBlobClient(blobItem.getName()).delete();
                } catch (BlobStorageException e) {
                  if (!e.getErrorCode().equals(BlobErrorCode.BLOB_NOT_FOUND)) {
                    throw e;
                  }
                }
              });
    } catch (Exception e) {
      throw new ObjectStorageWrapperException(
          String.format("Failed to delete the objects with prefix '%s'", prefix), e);
    }
  }

  @Override
  public void close() {
    // BlobContainerClient does not have a close method
  }

  /**
   * Returns whether an earlier attempt of the failed conditional request may have been applied. The
   * Azure SDK resends a request after a lost response or a server error, and a conditional write
   * request carries no idempotency token, so a resent conditional write can fail because of its own
   * earlier, applied attempt.
   *
   * @param attempts the number of attempts of the conditional requests counted by {@link
   *     ConditionalRequestAttemptCounter}
   * @return whether an earlier attempt of the request may have been applied
   */
  private static boolean earlierAttemptMayHaveBeenApplied(AtomicInteger attempts) {
    return attempts.get() > 1;
  }
}
