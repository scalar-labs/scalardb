package com.scalar.db.storage.dynamo;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class ScannerImplTest {
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final int ANY_LARGE_INT = 1000;
  private static final int ANY_SMALL_INT = 1;
  private static final String ANY_SMALL_TEXT = "aa";
  private static final String ANY_COLUMN_NAME_1 = "val1";

  @Before
  public void setUp() throws Exception {}

  private TableMetadata prepareMetadataForSingleClusteringKey() {
    return TableMetadata.newBuilder()
        .addColumn(ANY_NAME_1, DataType.TEXT)
        .addColumn(ANY_NAME_2, DataType.INT)
        .addColumn(ANY_NAME_3, DataType.TEXT)
        .addColumn(ANY_COLUMN_NAME_1, DataType.TEXT)
        .addPartitionKey(ANY_NAME_1)
        .addClusteringKey(ANY_NAME_2)
        .build();
  }

  private TableMetadata prepareMetadataForMultipleClusteringKeys() {
    return TableMetadata.newBuilder()
        .addColumn(ANY_NAME_1, DataType.TEXT)
        .addColumn(ANY_NAME_2, DataType.INT)
        .addColumn(ANY_NAME_3, DataType.TEXT)
        .addColumn(ANY_COLUMN_NAME_1, DataType.TEXT)
        .addPartitionKey(ANY_NAME_1)
        .addClusteringKey(ANY_NAME_2)
        .addClusteringKey(ANY_NAME_3)
        .build();
  }

  private Map<String, AttributeValue> prepareItem() {
    Map<String, AttributeValue> item = new HashMap<>();

    item.put(DynamoOperation.PARTITION_KEY, AttributeValue.builder().s(ANY_TEXT_1).build());
    item.put(ANY_NAME_1, AttributeValue.builder().s(ANY_TEXT_1).build());
    item.put(ANY_NAME_2, AttributeValue.builder().n(String.valueOf(ANY_SMALL_INT)).build());
    item.put(ANY_NAME_3, AttributeValue.builder().s(ANY_SMALL_TEXT).build());
    item.put(ANY_COLUMN_NAME_1, AttributeValue.builder().s(ANY_TEXT_2).build());

    return item;
  }

  @Test
  public void constructor_ItemsWithSingleClusteringKeyGiven_ShouldNotSortItems() {
    // Arrange
    TableMetadata metadata = prepareMetadataForSingleClusteringKey();
    Map<String, AttributeValue> item1 = prepareItem();
    item1.put(ANY_NAME_2, AttributeValue.builder().n(String.valueOf(ANY_LARGE_INT)).build());
    Map<String, AttributeValue> item2 = prepareItem();
    Scan scan = new Scan(new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1)));
    List<Map<String, AttributeValue>> items = new ArrayList<>();
    items.add(item1);
    items.add(item2);

    // Act
    ScannerImpl actual = new ScannerImpl(items, scan, metadata);

    // Assert
    assertThat(actual.one().get().getValue(ANY_NAME_2).get())
        .isEqualTo(new IntValue(ANY_NAME_2, ANY_LARGE_INT));
  }

  @Test
  public void constructor_ItemsWithMultipleClusteringKeysGiven_ShouldSortItems() {
    // Arrange
    TableMetadata metadata = prepareMetadataForMultipleClusteringKeys();
    Map<String, AttributeValue> item1 = prepareItem();
    Map<String, AttributeValue> item2 = prepareItem();
    item2.put(ANY_NAME_2, AttributeValue.builder().n(String.valueOf(ANY_LARGE_INT)).build());
    Scan scan =
        new Scan(new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1)))
            .withOrdering(new Scan.Ordering(ANY_NAME_2, Scan.Ordering.Order.DESC));
    List<Map<String, AttributeValue>> items = new ArrayList<>();
    items.add(item1);
    items.add(item2);

    // Act
    ScannerImpl actual = new ScannerImpl(items, scan, metadata);

    // Assert
    assertThat(actual.one().get().getValue(ANY_NAME_2).get())
        .isEqualTo(new IntValue(ANY_NAME_2, ANY_LARGE_INT));
  }
}
