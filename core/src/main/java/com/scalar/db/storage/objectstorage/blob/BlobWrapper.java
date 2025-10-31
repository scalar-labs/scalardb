package com.scalar.db.storage.objectstorage.blob;

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
import com.scalar.db.storage.objectstorage.ObjectStorageWrapper;
import com.scalar.db.storage.objectstorage.ObjectStorageWrapperException;
import com.scalar.db.storage.objectstorage.ObjectStorageWrapperResponse;
import com.scalar.db.storage.objectstorage.PreconditionFailedException;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class BlobWrapper implements ObjectStorageWrapper {
  private final BlobContainerClient client;
  private final Duration requestTimeoutInSeconds;
  private final ParallelTransferOptions parallelTransferOptions;

  public BlobWrapper(BlobConfig config) {
    this.client =
        new BlobServiceClientBuilder()
            .endpoint(config.getEndpoint())
            .credential(new StorageSharedKeyCredential(config.getUsername(), config.getPassword()))
            .buildClient()
            .getBlobContainerClient(config.getBucket());
    this.requestTimeoutInSeconds = Duration.ofSeconds(config.getRequestTimeoutInSeconds());
    this.parallelTransferOptions =
        new ParallelTransferOptions()
            .setBlockSizeLong(config.getParallelUploadBlockSizeInBytes())
            .setMaxConcurrency(config.getParallelUploadMaxParallelism())
            .setMaxSingleUploadSizeLong(config.getParallelUploadThresholdInBytes());
  }

  @Override
  public Optional<ObjectStorageWrapperResponse> get(String key)
      throws ObjectStorageWrapperException {
    try {
      BlobClient blobClient = client.getBlobClient(key);
      BlobDownloadContentResponse response =
          blobClient.downloadContentWithResponse(null, null, requestTimeoutInSeconds, null);
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
      return client.listBlobs(new ListBlobsOptions().setPrefix(prefix), requestTimeoutInSeconds)
          .stream()
          .map(BlobItem::getName)
          .collect(Collectors.toSet());
    } catch (Exception e) {
      throw new ObjectStorageWrapperException(
          String.format("Failed to get the object keys with prefix '%s'", prefix), e);
    }
  }

  @Override
  public void insert(String key, String object) throws ObjectStorageWrapperException {
    try {
      BlobClient blobClient = client.getBlobClient(key);
      BlobParallelUploadOptions options =
          new BlobParallelUploadOptions(BinaryData.fromString(object))
              .setRequestConditions(new BlobRequestConditions().setIfNoneMatch("*"))
              .setParallelTransferOptions(parallelTransferOptions);
      blobClient.uploadWithResponse(options, requestTimeoutInSeconds, null);
    } catch (BlobStorageException e) {
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
    try {
      BlobClient blobClient = client.getBlobClient(key);
      BlobParallelUploadOptions options =
          new BlobParallelUploadOptions(BinaryData.fromString(object))
              .setRequestConditions(new BlobRequestConditions().setIfMatch(version))
              .setParallelTransferOptions(parallelTransferOptions);
      blobClient.uploadWithResponse(options, requestTimeoutInSeconds, null);
    } catch (BlobStorageException e) {
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
    try {
      BlobClient blobClient = client.getBlobClient(key);
      blobClient.deleteWithResponse(
          null, new BlobRequestConditions().setIfMatch(version), requestTimeoutInSeconds, null);
    } catch (BlobStorageException e) {
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
          .listBlobs(new ListBlobsOptions().setPrefix(prefix), requestTimeoutInSeconds)
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
}
