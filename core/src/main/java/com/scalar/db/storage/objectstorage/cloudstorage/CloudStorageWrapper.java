package com.scalar.db.storage.objectstorage.cloudstorage;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageBatch;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.annotations.VisibleForTesting;
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

  private final Storage storage;
  private final String bucket;
  private final Integer parallelUploadBlockSizeInBytes;

  public CloudStorageWrapper(CloudStorageConfig config) {
    storage =
        StorageOptions.newBuilder()
            .setProjectId(config.getProjectId())
            .setCredentials(config.getCredentials())
            .build()
            .getService();
    bucket = config.getBucket();
    parallelUploadBlockSizeInBytes = config.getParallelUploadBlockSizeInBytes().orElse(null);
  }

  @VisibleForTesting
  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public CloudStorageWrapper(CloudStorageConfig config, Storage storage) {
    this.storage = storage;
    this.bucket = config.getBucket();
    parallelUploadBlockSizeInBytes = config.getParallelUploadBlockSizeInBytes().orElse(null);
  }

  @Override
  public Optional<ObjectStorageWrapperResponse> get(String key)
      throws ObjectStorageWrapperException {
    try {
      Blob blob = storage.get(BlobId.of(bucket, key));
      if (blob == null) {
        return Optional.empty();
      }
      String payload = new String(blob.getContent(), StandardCharsets.UTF_8);
      String generation = String.valueOf(blob.getGeneration());
      return Optional.of(new ObjectStorageWrapperResponse(payload, generation));
    } catch (Exception e) {
      throw new ObjectStorageWrapperException(
          String.format("Failed to get the object with key '%s'", key), e);
    }
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
      if (!storage.delete(
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
    try {
      storage.close();
    } catch (Exception e) {
      throw new ObjectStorageWrapperException("Failed to close the storage wrapper", e);
    }
  }

  private void writeData(String key, String object, Storage.BlobWriteOption precondition)
      throws IOException {
    byte[] data = object.getBytes(StandardCharsets.UTF_8);
    BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of(bucket, key)).build();

    try (WriteChannel writer = storage.writer(blobInfo, precondition)) {
      if (parallelUploadBlockSizeInBytes != null) {
        writer.setChunkSize(parallelUploadBlockSizeInBytes);
      }
      ByteBuffer buffer = ByteBuffer.wrap(data);
      while (buffer.hasRemaining()) {
        writer.write(buffer);
      }
    }
  }
}
