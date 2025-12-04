package com.scalar.db.storage.objectstorage;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.DistributedStorageIntegrationTestBase;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class ObjectStorageIntegrationTest extends DistributedStorageIntegrationTestBase {

  @Override
  protected TableMetadata getTableMetadata() {
    return TableMetadata.newBuilder()
        .addColumn(getColumnName1(), DataType.INT)
        .addColumn(getColumnName2(), DataType.TEXT)
        .addColumn(getColumnName3(), DataType.INT)
        .addColumn(getColumnName4(), DataType.INT)
        .addColumn(getColumnName5(), DataType.BOOLEAN)
        .addColumn(getColumnName6(), DataType.BLOB)
        .addPartitionKey(getColumnName1())
        .addClusteringKey(getColumnName4())
        .build();
  }

  @Override
  protected Properties getProperties(String testName) {
    return ObjectStorageEnv.getProperties(testName);
  }

  @Override
  protected Map<String, String> getCreationOptions() {
    return ObjectStorageEnv.getCreationOptions();
  }

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void get_GetGivenForIndexedColumn_ShouldGet() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void get_GetGivenForIndexedColumnWithMatchedConjunctions_ShouldGet() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void get_GetGivenForIndexedColumnWithUnmatchedConjunctions_ShouldReturnEmpty() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void
      get_GetGivenForIndexedColumnMatchingMultipleRecords_ShouldThrowIllegalArgumentException() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void scan_ScanGivenForIndexedColumn_ShouldScan() {}

  @Override
  @Disabled("Object Storage does not support index-related operations")
  public void scan_ScanGivenForNonIndexedColumn_ShouldThrowIllegalArgumentException() {}

  @Test
  public void put_PutWithLargeTextValue_ShouldStoreProperly() throws ExecutionException {
    // Arrange
    int payloadSizeBytes = 20000000; // The default string length limit of Jackson
    char[] chars = new char[payloadSizeBytes + 1];
    Arrays.fill(chars, 'a');
    String largeText = new String(chars);
    Put put = preparePuts().get(0);
    put = Put.newBuilder(put).textValue(getColumnName2(), largeText).build();

    // Act
    storage.put(put);

    // Assert
    Get get = prepareGet(0, 0);
    Optional<Result> actual = storage.get(get);
    assertThat(actual.isPresent()).isTrue();
    assertThat(actual.get().contains(getColumnName2())).isTrue();
    assertThat(actual.get().getText(getColumnName2())).isEqualTo(largeText);
  }
}
