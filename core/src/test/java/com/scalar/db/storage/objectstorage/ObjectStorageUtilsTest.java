package com.scalar.db.storage.objectstorage;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class ObjectStorageUtilsTest {

  @Test
  public void getObjectKey_GivenAllNames_ShouldReturnExpectedObjectKey() {
    // Arrange
    String namespaceName = "namespace";
    String tableName = "table";
    String partitionName = "partition";

    // Act
    String actual = ObjectStorageUtils.getObjectKey(namespaceName, tableName, partitionName);

    // Assert
    assertThat(actual).isEqualTo("namespace/table/partition");
  }

  @Test
  public void getObjectKey_GivenNamespaceAndTableNames_ShouldReturnExpectedObjectKeyPrefix() {
    // Arrange
    String namespaceName = "namespace";
    String tableName = "table";
    String partitionName = "";

    // Act
    String actual = ObjectStorageUtils.getObjectKey(namespaceName, tableName, partitionName);

    // Assert
    assertThat(actual).isEqualTo("namespace/table/");
  }

  @Test
  public void getObjectKey_GivenNamespaceAndTableNames_ShouldReturnExpectedObjectKey() {
    // Arrange
    String namespaceName = "namespace";
    String tableName = "table";

    // Act
    String actual = ObjectStorageUtils.getObjectKey(namespaceName, tableName);

    // Assert
    assertThat(actual).isEqualTo("namespace/table");
  }
}
