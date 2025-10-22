package com.scalar.db.storage.objectstorage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.util.AdminTestUtils;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class ObjectStorageAdminTestUtils extends AdminTestUtils {
  private final ObjectStorageWrapper wrapper;
  private final String metadataNamespace;

  public ObjectStorageAdminTestUtils(Properties properties) {
    super(properties);
    ObjectStorageConfig objectStorageConfig =
        ObjectStorageUtils.getObjectStorageConfig(new DatabaseConfig(properties));
    wrapper = ObjectStorageWrapperFactory.create(objectStorageConfig);
    metadataNamespace = objectStorageConfig.getMetadataNamespace();
  }

  @Override
  public void dropNamespacesTable() {
    // Blob does not have a concept of table
  }

  @Override
  public void dropMetadataTable() {
    // Blob does not have a concept of table
  }

  @Override
  public void truncateNamespacesTable() throws Exception {
    wrapper.delete(
        ObjectStorageUtils.getObjectKey(
            metadataNamespace, ObjectStorageAdmin.NAMESPACE_METADATA_TABLE));
  }

  @Override
  public void truncateMetadataTable() throws Exception {
    wrapper.delete(
        ObjectStorageUtils.getObjectKey(
            metadataNamespace, ObjectStorageAdmin.TABLE_METADATA_TABLE));
  }

  @Override
  public void corruptMetadata(String namespace, String table) throws Exception {
    String objectKey =
        ObjectStorageUtils.getObjectKey(metadataNamespace, ObjectStorageAdmin.TABLE_METADATA_TABLE);
    Optional<ObjectStorageWrapperResponse> response = wrapper.get(objectKey);
    if (!response.isPresent()) {
      throw new IllegalArgumentException("The specified table metadata does not exist");
    }
    Map<String, ObjectStorageTableMetadata> metadataTable =
        Serializer.deserialize(
            response.get().getPayload(),
            new TypeReference<Map<String, ObjectStorageTableMetadata>>() {});

    String tableMetadataKey =
        String.join(
            String.valueOf(ObjectStorageUtils.CONCATENATED_KEY_DELIMITER), namespace, table);
    metadataTable.put(tableMetadataKey, new ObjectStorageTableMetadata());

    wrapper.update(objectKey, Serializer.serialize(metadataTable), response.get().getVersion());
  }

  @Override
  public void dropNamespace(String namespace) {
    // Blob does not have a concept of namespace
  }

  @Override
  public boolean namespaceExists(String namespace) {
    // Blob does not have a concept of namespace
    return true;
  }

  @Override
  public boolean tableExists(String namespace, String table) {
    // Blob does not have a concept of table
    return true;
  }

  @Override
  public void dropTable(String namespace, String table) {
    // Blob does not have a concept of table
  }

  @Override
  public void close() {
    // Do nothing
  }
}
