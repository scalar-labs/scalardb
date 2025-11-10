package com.scalar.db.storage.objectstorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.scalar.db.config.DatabaseConfig;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ObjectStorageWrapperIntegrationTest {
  private static final Logger logger =
      LoggerFactory.getLogger(ObjectStorageWrapperIntegrationTest.class);

  private static final String TEST_NAME = "object_storage_wrapper_integration_test";
  private static final String TEST_KEY1 = "test-key1";
  private static final String TEST_KEY2 = "test-key2";
  private static final String TEST_KEY3 = "test-key3";
  private static final String TEST_KEY_PREFIX = "test-key";
  private static final String TEST_OBJECT1 = "test-object1";
  private static final String TEST_OBJECT2 = "test-object2";
  private static final String TEST_OBJECT3 = "test-object3";

  protected ObjectStorageWrapper wrapper;

  @BeforeAll
  public void beforeAll() throws ObjectStorageWrapperException {
    Properties properties = getProperties(TEST_NAME);
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
    return ObjectStorageEnv.getProperties(testName);
  }

  private void createObjects() throws ObjectStorageWrapperException {
    wrapper.insert(TEST_KEY1, TEST_OBJECT1);
    wrapper.insert(TEST_KEY2, TEST_OBJECT2);
    wrapper.insert(TEST_KEY3, TEST_OBJECT3);
  }

  protected void deleteObjects() throws ObjectStorageWrapperException {
    wrapper.delete(TEST_KEY1);
    wrapper.delete(TEST_KEY2);
    wrapper.delete(TEST_KEY3);
  }

  @Test
  public void get_ExistingObjectKeyGiven_ShouldReturnCorrectObject() throws Exception {
    // Arrange

    // Act
    Optional<ObjectStorageWrapperResponse> response = wrapper.get(TEST_KEY1);

    // Assert
    assertThat(response.isPresent()).isTrue();
    assertThat(response.get().getPayload()).isEqualTo(TEST_OBJECT1);
  }

  @Test
  public void get_NonExistingObjectKeyGiven_ShouldReturnEmptyOptional() throws Exception {
    // Arrange

    // Act
    Optional<ObjectStorageWrapperResponse> response = wrapper.get("non-existing-key");

    // Assert
    assertThat(response.isPresent()).isFalse();
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
      wrapper.insert(TEST_KEY2, TEST_OBJECT2);
    }
  }

  @Test
  public void update_NonExistingObjectKeyGiven_ShouldThrowPreconditionFailedException() {
    // Arrange
    String objectKey = "non-existing-key";

    // Act Assert
    assertThatCode(() -> wrapper.update(objectKey, "some-object", "some-version"))
        .isInstanceOf(PreconditionFailedException.class);
  }

  @Test
  public void update_WrongVersionGiven_ShouldThrowPreconditionFailedException() {
    // Arrange
    String wrongVersion = "wrong-version";

    // Act Assert
    assertThatCode(() -> wrapper.update(TEST_KEY2, "another-object", wrongVersion))
        .isInstanceOf(PreconditionFailedException.class);
  }

  @Test
  public void delete_ExistingObjectKeyGiven_ShouldDeleteObjectSuccessfully() throws Exception {
    // Arrange
    Optional<ObjectStorageWrapperResponse> response1 = wrapper.get(TEST_KEY3);
    assertThat(response1.isPresent()).isTrue();

    try {
      // Act
      wrapper.delete(TEST_KEY3);

      // Assert
      Optional<ObjectStorageWrapperResponse> response2 = wrapper.get(TEST_KEY3);
      assertThat(response2.isPresent()).isFalse();
    } finally {
      wrapper.insert(TEST_KEY3, TEST_OBJECT3);
    }
  }

  @Test
  public void delete_NonExistingObjectKeyGiven_ShouldThrowPreconditionFailedException() {
    // Arrange
    String objectKey = "non-existing-key";

    // Act Assert
    assertThatCode(() -> wrapper.delete(objectKey)).isInstanceOf(PreconditionFailedException.class);
  }

  @Test
  public void delete_ExistingObjectKeyWithCorrectVersionGiven_ShouldDeleteObjectSuccessfully()
      throws Exception {
    // Arrange
    Optional<ObjectStorageWrapperResponse> response1 = wrapper.get(TEST_KEY1);
    assertThat(response1.isPresent()).isTrue();
    String version = response1.get().getVersion();

    try {
      // Act
      wrapper.delete(TEST_KEY1, version);

      // Assert
      Optional<ObjectStorageWrapperResponse> response2 = wrapper.get(TEST_KEY1);
      assertThat(response2.isPresent()).isFalse();
    } finally {
      wrapper.insert(TEST_KEY1, TEST_OBJECT1);
    }
  }

  @Test
  public void delete_ExistingObjectKeyWithWrongVersionGiven_ShouldThrowPreconditionFailedException()
      throws Exception {
    // Arrange
    Optional<ObjectStorageWrapperResponse> response1 = wrapper.get(TEST_KEY1);
    assertThat(response1.isPresent()).isTrue();
    String wrongVersion = "wrong-version";

    // Act Assert
    assertThatCode(() -> wrapper.delete(TEST_KEY1, wrongVersion))
        .isInstanceOf(PreconditionFailedException.class);
  }

  @Test
  public void getKeys_WithPrefix_ShouldReturnCorrectKeys() throws Exception {
    // Arrange

    // Act
    Set<String> keys = wrapper.getKeys(TEST_KEY_PREFIX);

    // Assert
    assertThat(keys).containsExactlyInAnyOrder(TEST_KEY1, TEST_KEY2, TEST_KEY3);
  }

  @Test
  public void getKeys_WithNonExistingPrefix_ShouldReturnEmptySet() throws Exception {
    // Arrange
    String nonExistingPrefix = "non-existing-prefix";

    // Act
    Set<String> keys = wrapper.getKeys(nonExistingPrefix);

    // Assert
    assertThat(keys).isEmpty();
  }

  @Test
  public void deleteByPrefix_WithExistingPrefix_ShouldDeleteObjectsSuccessfully() throws Exception {
    // Arrange

    try {
      // Act
      wrapper.deleteByPrefix(TEST_KEY_PREFIX);

      // Assert
      Set<String> keys = wrapper.getKeys(TEST_KEY_PREFIX);
      assertThat(keys).isEmpty();
    } finally {
      createObjects();
    }
  }

  @Test
  public void deleteByPrefix_WithNonExistingPrefix_ShouldDoNothing() throws Exception {
    // Arrange
    String nonExistingPrefix = "non-existing-prefix";

    // Act
    wrapper.deleteByPrefix(nonExistingPrefix);

    // Assert
    Set<String> keys = wrapper.getKeys(TEST_KEY_PREFIX);
    assertThat(keys).containsExactlyInAnyOrder(TEST_KEY1, TEST_KEY2, TEST_KEY3);
  }

  @Test
  public void close_ShouldNotThrowException() {
    try {
      // Arrange

      // Act Assert
      assertThatCode(() -> wrapper.close()).doesNotThrowAnyException();
    } finally {
      Properties properties = getProperties(TEST_NAME);
      ObjectStorageConfig objectStorageConfig =
          ObjectStorageUtils.getObjectStorageConfig(new DatabaseConfig(properties));
      wrapper = ObjectStorageWrapperFactory.create(objectStorageConfig);
    }
  }
}
