package com.scalar.db.storage.objectstorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.objectstorage.blobstorage.BlobStorageConfig;
import com.scalar.db.storage.objectstorage.cloudstorage.CloudStorageConfig;
import com.scalar.db.storage.objectstorage.s3.S3Config;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ObjectStorageWrapperLargeObjectWriteIntegrationTest {
  private static final Logger logger =
      LoggerFactory.getLogger(ObjectStorageWrapperLargeObjectWriteIntegrationTest.class);

  private static final String TEST_NAME = "object_storage_wrapper_integration_test";
  private static final String TEST_KEY1 = "test-key1";
  private static final String TEST_KEY2 = "test-key2";
  private static final String TEST_KEY3 = "test-key3";

  private String testObject1;
  private String testObject2;
  private String testObject3;

  private ObjectStorageWrapper wrapper;

  @BeforeAll
  public void beforeAll() throws ObjectStorageWrapperException {
    Properties properties = getProperties(TEST_NAME);
    long payloadSizeBytes;

    if (ObjectStorageEnv.isBlobStorage()) {
      // Minimum block size must be greater than or equal to 256KB for Blob Storage
      Long uploadUnit = 256 * 1024L; // 256KB
      properties.setProperty(
          BlobStorageConfig.PARALLEL_UPLOAD_BLOCK_SIZE_BYTES, String.valueOf(uploadUnit));
      properties.setProperty(
          BlobStorageConfig.PARALLEL_UPLOAD_THRESHOLD_SIZE_BYTES, String.valueOf(uploadUnit * 2));
      payloadSizeBytes = uploadUnit * 2 + 1;
    } else if (ObjectStorageEnv.isCloudStorage()) {
      // Minimum block size must be greater than or equal to 256KB for Cloud Storage
      Long uploadUnit = 256 * 1024L; // 256KB
      properties.setProperty(
          CloudStorageConfig.UPLOAD_CHUNK_SIZE_BYTES, String.valueOf(uploadUnit));
      payloadSizeBytes = uploadUnit * 2 + 1;
    } else if (ObjectStorageEnv.isS3()) {
      // Minimum part size must be greater than or equal to 5MB for S3
      Long uploadUnit = 5 * 1024 * 1024L; // 5MB
      properties.setProperty(S3Config.MULTIPART_UPLOAD_PART_SIZE_BYTES, String.valueOf(uploadUnit));
      properties.setProperty(
          S3Config.MULTIPART_UPLOAD_THRESHOLD_SIZE_BYTES, String.valueOf(uploadUnit * 2));
      payloadSizeBytes = uploadUnit * 2 + 1;
    } else {
      throw new AssertionError();
    }

    char[] charArray = new char[(int) payloadSizeBytes];
    Arrays.fill(charArray, 'a');
    testObject1 = new String(charArray);
    Arrays.fill(charArray, 'b');
    testObject2 = new String(charArray);
    Arrays.fill(charArray, 'c');
    testObject3 = new String(charArray);

    ObjectStorageConfig objectStorageConfig =
        ObjectStorageUtils.getObjectStorageConfig(new DatabaseConfig(properties));
    wrapper = ObjectStorageWrapperFactory.create(objectStorageConfig);

    createObjects();
  }

  @AfterAll
  public void afterAll() {
    try {
      deleteObjects();
    } catch (Exception e) {
      logger.warn("Failed to delete objects", e);
    }

    try {
      if (wrapper != null) {
        wrapper.close();
      }
    } catch (Exception e) {
      logger.warn("Failed to close wrapper", e);
    }
  }

  protected Properties getProperties(String testName) {
    return ObjectStorageEnv.getPropertiesWithPerformanceOptions(testName);
  }

  private void createObjects() throws ObjectStorageWrapperException {
    wrapper.insert(TEST_KEY1, testObject1);
    wrapper.insert(TEST_KEY2, testObject2);
    wrapper.insert(TEST_KEY3, testObject3);
  }

  protected void deleteObjects() throws ObjectStorageWrapperException {
    wrapper.delete(TEST_KEY1);
    wrapper.delete(TEST_KEY2);
    wrapper.delete(TEST_KEY3);
  }

  @Test
  public void insert_NewObjectKeyGiven_ShouldInsertObjectSuccessfully() throws Exception {
    // Arrange
    String objectKey = "new-object-key";
    String object = "new-object";

    try {
      // Act
      wrapper.insert(objectKey, object);

      // Assert
      Optional<ObjectStorageWrapperResponse> response = wrapper.get(objectKey);
      assertThat(response.isPresent()).isTrue();
      assertThat(response.get().getPayload()).isEqualTo(object);
    } finally {
      wrapper.delete(objectKey);
    }
  }

  @Test
  public void insert_ExistingObjectKeyGiven_ShouldThrowPreconditionFailedException() {
    // Arrange

    // Act Assert
    assertThatCode(() -> wrapper.insert(TEST_KEY2, "another-object"))
        .isInstanceOf(PreconditionFailedException.class);
  }

  @Test
  public void update_ExistingObjectKeyGiven_ShouldUpdateObjectSuccessfully() throws Exception {
    // Arrange
    String updatedObject = "updated-object2";
    Optional<ObjectStorageWrapperResponse> response1 = wrapper.get(TEST_KEY2);
    assertThat(response1.isPresent()).isTrue();
    String version = response1.get().getVersion();

    try {
      // Act
      wrapper.update(TEST_KEY2, updatedObject, version);

      // Assert
      Optional<ObjectStorageWrapperResponse> response2 = wrapper.get(TEST_KEY2);
      assertThat(response2.isPresent()).isTrue();
      assertThat(response2.get().getPayload()).isEqualTo(updatedObject);
    } finally {
      wrapper.delete(TEST_KEY2);
      wrapper.insert(TEST_KEY2, testObject2);
    }
  }

  @Test
  public void update_NonExistingObjectKeyGiven_ShouldThrowPreconditionFailedException() {
    // Arrange
    String objectKey = "non-existing-key";

    // Act Assert
    assertThatCode(() -> wrapper.update(objectKey, "some-object", "123456789"))
        .isInstanceOf(PreconditionFailedException.class);
  }

  @Test
  public void update_WrongVersionGiven_ShouldThrowPreconditionFailedException() {
    // Arrange
    String wrongVersion = "123456789";

    // Act Assert
    assertThatCode(() -> wrapper.update(TEST_KEY2, "another-object", wrongVersion))
        .isInstanceOf(PreconditionFailedException.class);
  }
}
