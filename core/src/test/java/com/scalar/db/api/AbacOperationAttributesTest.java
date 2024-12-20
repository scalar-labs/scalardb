package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.io.Key;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class AbacOperationAttributesTest {

  @Test
  public void setReadTag_ShouldSetReadTag() {
    // Arrange
    Map<String, String> attributes = new HashMap<>();
    String policyName = "policyName";
    String readTag = "readTag";

    // Act
    AbacOperationAttributes.setReadTag(attributes, policyName, readTag);

    // Assert
    assertThat(attributes)
        .containsEntry(AbacOperationAttributes.READ_TAG_PREFIX + policyName, readTag);
  }

  @Test
  public void clearReadTag_ShouldClearReadTag() {
    // Arrange
    Map<String, String> attributes = new HashMap<>();
    String policyName = "policyName";
    String readTag = "readTag";
    AbacOperationAttributes.setReadTag(attributes, policyName, readTag);

    // Act
    AbacOperationAttributes.clearReadTag(attributes, policyName);

    // Assert
    assertThat(attributes).doesNotContainKey(AbacOperationAttributes.READ_TAG_PREFIX + policyName);
  }

  @Test
  public void clearReadTags_ShouldClearReadTags() {
    // Arrange
    Map<String, String> attributes = new HashMap<>();
    String policyName1 = "policyName1";
    String policyName2 = "policyName2";
    String readTag1 = "readTag1";
    String readTag2 = "readTag2";
    AbacOperationAttributes.setReadTag(attributes, policyName1, readTag1);
    AbacOperationAttributes.setReadTag(attributes, policyName2, readTag2);

    // Act
    AbacOperationAttributes.clearReadTags(attributes);

    // Assert
    assertThat(attributes).doesNotContainKey(AbacOperationAttributes.READ_TAG_PREFIX + policyName1);
    assertThat(attributes).doesNotContainKey(AbacOperationAttributes.READ_TAG_PREFIX + policyName2);
  }

  @Test
  public void setWriteTag_ShouldSetWriteTag() {
    // Arrange
    Map<String, String> attributes = new HashMap<>();
    String policyName = "policyName";
    String writeTag = "writeTag";

    // Act
    AbacOperationAttributes.setWriteTag(attributes, policyName, writeTag);

    // Assert
    assertThat(attributes)
        .containsEntry(AbacOperationAttributes.WRITE_TAG_PREFIX + policyName, writeTag);
  }

  @Test
  public void clearWriteTag_ShouldClearWriteTag() {
    // Arrange
    Map<String, String> attributes = new HashMap<>();
    String policyName = "policyName";
    String writeTag = "writeTag";
    AbacOperationAttributes.setWriteTag(attributes, policyName, writeTag);

    // Act
    AbacOperationAttributes.clearWriteTag(attributes, policyName);

    // Assert
    assertThat(attributes).doesNotContainKey(AbacOperationAttributes.WRITE_TAG_PREFIX + policyName);
  }

  @Test
  public void clearWriteTags_ShouldClearWriteTags() {
    // Arrange
    Map<String, String> attributes = new HashMap<>();
    String policyName1 = "policyName1";
    String policyName2 = "policyName2";
    String writeTag1 = "writeTag1";
    String writeTag2 = "writeTag2";
    AbacOperationAttributes.setWriteTag(attributes, policyName1, writeTag1);
    AbacOperationAttributes.setWriteTag(attributes, policyName2, writeTag2);

    // Act
    AbacOperationAttributes.clearWriteTags(attributes);

    // Assert
    assertThat(attributes)
        .doesNotContainKey(AbacOperationAttributes.WRITE_TAG_PREFIX + policyName1);
    assertThat(attributes)
        .doesNotContainKey(AbacOperationAttributes.WRITE_TAG_PREFIX + policyName2);
  }

  @Test
  public void getReadTag_ShouldReturnReadTag() {
    // Arrange
    String policyName = "policyName";
    String readTag = "readTag";
    Operation operation =
        Get.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("pk", 0))
            .readTag(policyName, readTag)
            .build();

    // Act
    Optional<String> actual = AbacOperationAttributes.getReadTag(operation, policyName);

    // Assert
    assertThat(actual).hasValue(readTag);
  }

  @Test
  public void getWriteTag_ShouldReturnWriteTag() {
    // Arrange
    String policyName = "policyName";
    String writeTag = "writeTag";
    Operation operation =
        Update.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("pk", 0))
            .writeTag(policyName, writeTag)
            .build();

    // Act
    Optional<String> actual = AbacOperationAttributes.getWriteTag(operation, policyName);

    // Assert
    assertThat(actual).hasValue(writeTag);
  }
}
