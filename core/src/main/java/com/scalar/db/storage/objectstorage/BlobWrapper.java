package com.scalar.db.storage.objectstorage;

import com.azure.core.http.HttpHeaderName;
import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobDownloadContentResponse;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import java.time.Duration;
import java.util.Set;
import java.util.stream.Collectors;

public class BlobWrapper implements ObjectStorageWrapper {
  public static final String STORAGE_NAME = "blob";

  private final BlobContainerClient client;
  private final Duration requestTimeoutInSeconds;
  private final ParallelTransferOptions parallelTransferOptions;

  public BlobWrapper(BlobContainerClient client, BlobConfig config) {
    this.client = client;
    this.requestTimeoutInSeconds = Duration.ofSeconds(config.getRequestTimeoutInSeconds());
    this.parallelTransferOptions =
        new ParallelTransferOptions()
            .setBlockSizeLong(config.getParallelUploadBlockSizeInBytes())
            .setMaxConcurrency(config.getParallelUploadMaxParallelism())
            .setMaxSingleUploadSizeLong(config.getParallelUploadThresholdInBytes());
  }

  @Override
  public ObjectStorageWrapperResponse get(String key) throws ObjectStorageWrapperException {
    try {
      BlobClient blobClient = client.getBlobClient(key);
      BlobDownloadContentResponse response =
          blobClient.downloadContentWithResponse(null, null, requestTimeoutInSeconds, null);
      String data = response.getValue().toString();
      String eTag = response.getHeaders().getValue(HttpHeaderName.ETAG);
      return new ObjectStorageWrapperResponse(data, eTag);
    } catch (BlobStorageException e) {
      if (e.getErrorCode().equals(BlobErrorCode.BLOB_NOT_FOUND)) {
        throw new ObjectStorageWrapperException(
            ObjectStorageWrapperException.StatusCode.NOT_FOUND, e);
      }
      throw e;
    }
  }

  @Override
  public Set<String> getKeys(String prefix) {
    return client.listBlobs(new ListBlobsOptions().setPrefix(prefix), requestTimeoutInSeconds)
        .stream()
        .map(BlobItem::getName)
        .collect(Collectors.toSet());
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
        throw new ObjectStorageWrapperException(
            ObjectStorageWrapperException.StatusCode.ALREADY_EXISTS, e);
      }
      throw e;
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
      if (e.getErrorCode().equals(BlobErrorCode.CONDITION_NOT_MET)) {
        throw new ObjectStorageWrapperException(
            ObjectStorageWrapperException.StatusCode.VERSION_MISMATCH, e);
      }
      if (e.getErrorCode().equals(BlobErrorCode.BLOB_NOT_FOUND)) {
        throw new ObjectStorageWrapperException(
            ObjectStorageWrapperException.StatusCode.NOT_FOUND, e);
      }
      throw e;
    }
  }

  @Override
  public void delete(String key) throws ObjectStorageWrapperException {
    try {
      BlobClient blobClient = client.getBlobClient(key);
      blobClient.delete();
    } catch (BlobStorageException e) {
      if (e.getErrorCode().equals(BlobErrorCode.BLOB_NOT_FOUND)) {
        throw new ObjectStorageWrapperException(
            ObjectStorageWrapperException.StatusCode.NOT_FOUND, e);
      }
      throw e;
    }
  }

  @Override
  public void delete(String key, String version) throws ObjectStorageWrapperException {
    try {
      BlobClient blobClient = client.getBlobClient(key);
      blobClient.deleteWithResponse(
          null, new BlobRequestConditions().setIfMatch(version), requestTimeoutInSeconds, null);
    } catch (BlobStorageException e) {
      if (e.getErrorCode().equals(BlobErrorCode.CONDITION_NOT_MET)) {
        throw new ObjectStorageWrapperException(
            ObjectStorageWrapperException.StatusCode.VERSION_MISMATCH, e);
      }
      if (e.getErrorCode().equals(BlobErrorCode.BLOB_NOT_FOUND)) {
        throw new ObjectStorageWrapperException(
            ObjectStorageWrapperException.StatusCode.NOT_FOUND, e);
      }
      throw e;
    }
  }

  @Override
  public void deleteByPrefix(String prefix) {
    client
        .listBlobs(new ListBlobsOptions().setPrefix(prefix), requestTimeoutInSeconds)
        .forEach(blobItem -> client.getBlobClient(blobItem.getName()).delete());
  }

  @Override
  public void close() {
    // BlobContainerClient does not have a close method
  }
}
