package com.scalar.db.storage.objectstorage;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.util.AdminTestUtils;
import java.util.Properties;
import java.util.stream.Collectors;

public class ObjectStorageAdminTestUtils extends AdminTestUtils {
  private final BlobContainerClient client;
  private final String metadataNamespace;

  public ObjectStorageAdminTestUtils(Properties properties) {
    super(properties);
    ObjectStorageConfig config =
        ObjectStorageUtils.getObjectStorageConfig(new DatabaseConfig(properties));
    client =
        new BlobServiceClientBuilder()
            .endpoint(config.getEndpoint())
            .credential(new StorageSharedKeyCredential(config.getUsername(), config.getPassword()))
            .buildClient()
            .getBlobContainerClient(config.getBucket());
    metadataNamespace = config.getMetadataNamespace();
  }

  @Override
  public void dropNamespacesTable() throws Exception {
    // Do nothing
    // Blob does not have a concept of table
  }

  @Override
  public void dropMetadataTable() throws Exception {
    // Do nothing
    // Blob does not have a concept of table
  }

  @Override
  public void truncateNamespacesTable() throws Exception {
    client
        .listBlobs(
            new ListBlobsOptions()
                .setPrefix(
                    ObjectStorageUtils.getObjectKey(
                        metadataNamespace, ObjectStorageAdmin.NAMESPACE_METADATA_TABLE, null)),
            null)
        .stream()
        .map(BlobItem::getName)
        .collect(Collectors.toList())
        .forEach(
            key -> {
              client.getBlobClient(key).delete();
            });
  }

  @Override
  public void truncateMetadataTable() throws Exception {
    client
        .listBlobs(
            new ListBlobsOptions()
                .setPrefix(
                    ObjectStorageUtils.getObjectKey(
                        metadataNamespace, ObjectStorageAdmin.TABLE_METADATA_TABLE, null)),
            null)
        .stream()
        .map(BlobItem::getName)
        .collect(Collectors.toList())
        .forEach(
            key -> {
              client.getBlobClient(key).delete();
            });
  }

  @Override
  public void corruptMetadata(String namespace, String table) throws Exception {
    client
        .getBlobClient(ObjectStorageUtils.getObjectKey(metadataNamespace, table, null))
        .upload(BinaryData.fromString("corrupted metadata"), true);
  }

  @Override
  public void dropNamespace(String namespace) throws Exception {
    // Do nothing
    // Blob does not have a concept of namespace
  }

  @Override
  public boolean namespaceExists(String namespace) throws Exception {
    // Blob does not have a concept of namespace
    return true;
  }

  @Override
  public boolean tableExists(String namespace, String table) throws Exception {
    // Blob does not have a concept of table
    return true;
  }

  @Override
  public void dropTable(String namespace, String table) throws Exception {
    // Do nothing
    // Blob does not have a concept of table
  }

  @Override
  public void close() throws Exception {
    // Do nothing
  }
}
