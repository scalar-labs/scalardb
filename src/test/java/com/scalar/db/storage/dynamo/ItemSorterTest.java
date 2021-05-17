package com.scalar.db.storage.dynamo;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class ItemSorterTest {
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_NAME_4 = "name4";
  private static final String ANY_NAME_5 = "name5";
  private static final String ANY_NAME_6 = "name6";
  private static final String ANY_NAME_7 = "name7";
  private static final String ANY_NAME_8 = "name8";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final boolean LARGE_BOOLEAN = true;
  private static final boolean SMALL_BOOLEAN = false;
  private static final int ANY_LARGE_INT = 100;
  private static final int ANY_SMALL_INT = 1;
  private static final long ANY_LARGE_BIGINT = 1000L;
  private static final long ANY_SMALL_BIGINT = -1L;
  private static final float ANY_LARGE_FLOAT = 1.111f;
  private static final float ANY_SMALL_FLOAT = -1.0f;
  private static final double ANY_LARGE_DOUBLE = 1.11111;
  private static final double ANY_SMALL_DOUBLE = -10.0;
  private static final String ANY_LARGE_TEXT = "sssssss";
  private static final String ANY_SMALL_TEXT = "aa";
  private static final byte[] ANY_LARGE_BLOB = "scalar".getBytes();
  private static final byte[] ANY_SMALL_BLOB = "a".getBytes();
  private static final String ANY_COLUMN_NAME_1 = "val1";

  private TableMetadata metadata;

  @Before
  public void setUp() throws Exception {
    metadata =
        TableMetadata.newBuilder()
            .addColumn(ANY_NAME_1, DataType.TEXT)
            .addColumn(ANY_NAME_2, DataType.INT)
            .addColumn(ANY_NAME_3, DataType.BOOLEAN)
            .addColumn(ANY_NAME_4, DataType.BIGINT)
            .addColumn(ANY_NAME_5, DataType.FLOAT)
            .addColumn(ANY_NAME_6, DataType.DOUBLE)
            .addColumn(ANY_NAME_7, DataType.TEXT)
            .addColumn(ANY_NAME_8, DataType.BLOB)
            .addColumn(ANY_COLUMN_NAME_1, DataType.TEXT)
            .addPartitionKey(ANY_NAME_1)
            .addClusteringKey(ANY_NAME_2)
            .addClusteringKey(ANY_NAME_3)
            .addClusteringKey(ANY_NAME_4)
            .addClusteringKey(ANY_NAME_5)
            .addClusteringKey(ANY_NAME_6)
            .addClusteringKey(ANY_NAME_7)
            .addClusteringKey(ANY_NAME_8)
            .build();
  }

  private Map<String, AttributeValue> prepareItemWithSmallValues() {
    Map<String, AttributeValue> item = new HashMap<>();

    item.put(DynamoOperation.PARTITION_KEY, AttributeValue.builder().s(ANY_TEXT_1).build());
    item.put(ANY_NAME_1, AttributeValue.builder().s(ANY_TEXT_1).build());
    item.put(ANY_NAME_2, AttributeValue.builder().n(String.valueOf(ANY_SMALL_INT)).build());
    item.put(ANY_NAME_3, AttributeValue.builder().bool(SMALL_BOOLEAN).build());
    item.put(ANY_NAME_4, AttributeValue.builder().n(String.valueOf(ANY_SMALL_BIGINT)).build());
    item.put(ANY_NAME_5, AttributeValue.builder().n(String.valueOf(ANY_SMALL_FLOAT)).build());
    item.put(ANY_NAME_6, AttributeValue.builder().n(String.valueOf(ANY_SMALL_DOUBLE)).build());
    item.put(ANY_NAME_7, AttributeValue.builder().s(ANY_SMALL_TEXT).build());
    item.put(
        ANY_NAME_8, AttributeValue.builder().b(SdkBytes.fromByteArray(ANY_SMALL_BLOB)).build());
    item.put(ANY_COLUMN_NAME_1, AttributeValue.builder().s(ANY_TEXT_2).build());

    return item;
  }

  @Test
  public void sort_ItemsGivenWithoutOrderingAndLimit_ShouldNotSort() {
    // Arrange
    Map<String, AttributeValue> item1 = prepareItemWithSmallValues();
    item1.put(ANY_NAME_2, AttributeValue.builder().n(String.valueOf(ANY_LARGE_INT)).build());
    Map<String, AttributeValue> item2 = prepareItemWithSmallValues();
    Scan scan = new Scan(new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1)));
    List<Map<String, AttributeValue>> items = new ArrayList<>();
    items.add(item1);
    items.add(item2);
    ItemSorter sorter = new ItemSorter(scan, metadata);

    // Act
    List<Map<String, AttributeValue>> actual = sorter.sort(items);

    // Assert
    assertThat(actual.get(0).get(ANY_NAME_2).n()).isEqualTo(String.valueOf(ANY_LARGE_INT));
    assertThat(actual.get(1).get(ANY_NAME_2).n()).isEqualTo(String.valueOf(ANY_SMALL_INT));
  }

  @Test
  public void sort_ItemsGivenWithAscOrderingInt_ShouldSortProperly() {
    // Arrange
    Map<String, AttributeValue> item1 = prepareItemWithSmallValues();
    item1.put(ANY_NAME_2, AttributeValue.builder().n(String.valueOf(ANY_LARGE_INT)).build());
    Map<String, AttributeValue> item2 = prepareItemWithSmallValues();
    Scan scan =
        new Scan(new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1)))
            .withOrdering(new Scan.Ordering(ANY_NAME_2, Scan.Ordering.Order.ASC));
    List<Map<String, AttributeValue>> items = new ArrayList<>();
    items.add(item1);
    items.add(item2);
    ItemSorter sorter = new ItemSorter(scan, metadata);

    // Act
    List<Map<String, AttributeValue>> actual = sorter.sort(items);

    // Assert
    assertThat(actual.get(0).get(ANY_NAME_2).n()).isEqualTo(String.valueOf(ANY_SMALL_INT));
    assertThat(actual.get(1).get(ANY_NAME_2).n()).isEqualTo(String.valueOf(ANY_LARGE_INT));
  }

  @Test
  public void sort_ItemsGivenWithDescOrderingInt_ShouldSortProperly() {
    // Arrange
    Map<String, AttributeValue> item1 = prepareItemWithSmallValues();
    Map<String, AttributeValue> item2 = prepareItemWithSmallValues();
    item2.put(ANY_NAME_2, AttributeValue.builder().n(String.valueOf(ANY_LARGE_INT)).build());
    Scan scan =
        new Scan(new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1)))
            .withOrdering(new Scan.Ordering(ANY_NAME_2, Scan.Ordering.Order.DESC));
    List<Map<String, AttributeValue>> items = new ArrayList<>();
    items.add(item1);
    items.add(item2);
    ItemSorter sorter = new ItemSorter(scan, metadata);

    // Act
    List<Map<String, AttributeValue>> actual = sorter.sort(items);

    // Assert
    assertThat(actual.get(0).get(ANY_NAME_2).n()).isEqualTo(String.valueOf(ANY_LARGE_INT));
    assertThat(actual.get(1).get(ANY_NAME_2).n()).isEqualTo(String.valueOf(ANY_SMALL_INT));
  }

  @Test
  public void sort_ItemsGivenWithDescOrderingBoolean_ShouldSortProperly() {
    // Arrange
    Map<String, AttributeValue> item1 = prepareItemWithSmallValues();
    Map<String, AttributeValue> item2 = prepareItemWithSmallValues();
    item2.put(ANY_NAME_3, AttributeValue.builder().bool(LARGE_BOOLEAN).build());
    Scan scan =
        new Scan(new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1)))
            .withOrdering(new Scan.Ordering(ANY_NAME_3, Scan.Ordering.Order.DESC));
    List<Map<String, AttributeValue>> items = new ArrayList<>();
    items.add(item1);
    items.add(item2);
    ItemSorter sorter = new ItemSorter(scan, metadata);

    // Act
    List<Map<String, AttributeValue>> actual = sorter.sort(items);

    // Assert
    assertThat(actual.get(0).get(ANY_NAME_3).bool()).isEqualTo(LARGE_BOOLEAN);
    assertThat(actual.get(1).get(ANY_NAME_3).bool()).isEqualTo(SMALL_BOOLEAN);
  }

  @Test
  public void sort_ItemsGivenWithDescOrderingBigint_ShouldSortProperly() {
    // Arrange
    Map<String, AttributeValue> item1 = prepareItemWithSmallValues();
    Map<String, AttributeValue> item2 = prepareItemWithSmallValues();
    item2.put(ANY_NAME_4, AttributeValue.builder().n(String.valueOf(ANY_LARGE_BIGINT)).build());
    Scan scan =
        new Scan(new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1)))
            .withOrdering(new Scan.Ordering(ANY_NAME_4, Scan.Ordering.Order.DESC));
    List<Map<String, AttributeValue>> items = new ArrayList<>();
    items.add(item1);
    items.add(item2);
    ItemSorter sorter = new ItemSorter(scan, metadata);

    // Act
    List<Map<String, AttributeValue>> actual = sorter.sort(items);

    // Assert
    assertThat(actual.get(0).get(ANY_NAME_4).n()).isEqualTo(String.valueOf(ANY_LARGE_BIGINT));
    assertThat(actual.get(1).get(ANY_NAME_4).n()).isEqualTo(String.valueOf(ANY_SMALL_BIGINT));
  }

  @Test
  public void sort_ItemsGivenWithDescOrderingFloat_ShouldSortProperly() {
    // Arrange
    Map<String, AttributeValue> item1 = prepareItemWithSmallValues();
    Map<String, AttributeValue> item2 = prepareItemWithSmallValues();
    item2.put(ANY_NAME_5, AttributeValue.builder().n(String.valueOf(ANY_LARGE_FLOAT)).build());
    Scan scan =
        new Scan(new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1)))
            .withOrdering(new Scan.Ordering(ANY_NAME_5, Scan.Ordering.Order.DESC));
    List<Map<String, AttributeValue>> items = new ArrayList<>();
    items.add(item1);
    items.add(item2);
    ItemSorter sorter = new ItemSorter(scan, metadata);

    // Act
    List<Map<String, AttributeValue>> actual = sorter.sort(items);

    // Assert
    assertThat(actual.get(0).get(ANY_NAME_5).n()).isEqualTo(String.valueOf(ANY_LARGE_FLOAT));
    assertThat(actual.get(1).get(ANY_NAME_5).n()).isEqualTo(String.valueOf(ANY_SMALL_FLOAT));
  }

  @Test
  public void sort_ItemsGivenWithDescOrderingDouble_ShouldSortProperly() {
    // Arrange
    Map<String, AttributeValue> item1 = prepareItemWithSmallValues();
    Map<String, AttributeValue> item2 = prepareItemWithSmallValues();
    item2.put(ANY_NAME_6, AttributeValue.builder().n(String.valueOf(ANY_LARGE_DOUBLE)).build());
    Scan scan =
        new Scan(new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1)))
            .withOrdering(new Scan.Ordering(ANY_NAME_6, Scan.Ordering.Order.DESC));
    List<Map<String, AttributeValue>> items = new ArrayList<>();
    items.add(item1);
    items.add(item2);
    ItemSorter sorter = new ItemSorter(scan, metadata);

    // Act
    List<Map<String, AttributeValue>> actual = sorter.sort(items);

    // Assert
    assertThat(actual.get(0).get(ANY_NAME_6).n()).isEqualTo(String.valueOf(ANY_LARGE_DOUBLE));
    assertThat(actual.get(1).get(ANY_NAME_6).n()).isEqualTo(String.valueOf(ANY_SMALL_DOUBLE));
  }

  @Test
  public void sort_ItemsGivenWithDescOrderingText_ShouldSortProperly() {
    // Arrange
    Map<String, AttributeValue> item1 = prepareItemWithSmallValues();
    Map<String, AttributeValue> item2 = prepareItemWithSmallValues();
    item2.put(ANY_NAME_7, AttributeValue.builder().s(ANY_LARGE_TEXT).build());
    Scan scan =
        new Scan(new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1)))
            .withOrdering(new Scan.Ordering(ANY_NAME_7, Scan.Ordering.Order.DESC));
    List<Map<String, AttributeValue>> items = new ArrayList<>();
    items.add(item1);
    items.add(item2);
    ItemSorter sorter = new ItemSorter(scan, metadata);

    // Act
    List<Map<String, AttributeValue>> actual = sorter.sort(items);

    // Assert
    assertThat(actual.get(0).get(ANY_NAME_7).s()).isEqualTo(ANY_LARGE_TEXT);
    assertThat(actual.get(1).get(ANY_NAME_7).s()).isEqualTo(ANY_SMALL_TEXT);
  }

  @Test
  public void sort_ItemsGivenWithDescOrderingBlob_ShouldSortProperly() {
    // Arrange
    Map<String, AttributeValue> item1 = prepareItemWithSmallValues();
    Map<String, AttributeValue> item2 = prepareItemWithSmallValues();
    item2.put(
        ANY_NAME_8, AttributeValue.builder().b(SdkBytes.fromByteArray(ANY_LARGE_BLOB)).build());
    Scan scan =
        new Scan(new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1)))
            .withOrdering(new Scan.Ordering(ANY_NAME_8, Scan.Ordering.Order.DESC));
    List<Map<String, AttributeValue>> items = new ArrayList<>();
    items.add(item1);
    items.add(item2);
    ItemSorter sorter = new ItemSorter(scan, metadata);

    // Act
    List<Map<String, AttributeValue>> actual = sorter.sort(items);

    // Assert
    assertThat(actual.get(0).get(ANY_NAME_8).b().asByteArray()).isEqualTo(ANY_LARGE_BLOB);
    assertThat(actual.get(1).get(ANY_NAME_8).b().asByteArray()).isEqualTo(ANY_SMALL_BLOB);
  }

  @Test
  public void sort_ItemsGivenWithDescOrderingIntAndLimit_ShouldResultLargerOne() {
    // Arrange
    Map<String, AttributeValue> item1 = prepareItemWithSmallValues();
    Map<String, AttributeValue> item2 = prepareItemWithSmallValues();
    item2.put(ANY_NAME_2, AttributeValue.builder().n(String.valueOf(ANY_LARGE_INT)).build());
    Scan scan =
        new Scan(new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1)))
            .withOrdering(new Scan.Ordering(ANY_NAME_2, Scan.Ordering.Order.DESC))
            .withLimit(1);
    List<Map<String, AttributeValue>> items = new ArrayList<>();
    items.add(item1);
    items.add(item2);
    ItemSorter sorter = new ItemSorter(scan, metadata);

    // Act
    List<Map<String, AttributeValue>> actual = sorter.sort(items);

    // Assert
    assertThat(actual.size()).isEqualTo(1);
    assertThat(actual.get(0).get(ANY_NAME_2).n()).isEqualTo(String.valueOf(ANY_LARGE_INT));
  }

  @Test
  public void sort_ItemsGivenWithMultipleOrderingIntAndText_ShouldSortProperly() {
    // Arrange
    Map<String, AttributeValue> item1 = prepareItemWithSmallValues();
    Map<String, AttributeValue> item2 = prepareItemWithSmallValues();
    item2.put(ANY_NAME_2, AttributeValue.builder().n(String.valueOf(ANY_LARGE_INT)).build());
    Map<String, AttributeValue> item3 = prepareItemWithSmallValues();
    item3.put(ANY_NAME_7, AttributeValue.builder().s(ANY_LARGE_TEXT).build());
    Scan scan =
        new Scan(new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1)))
            .withOrdering(new Scan.Ordering(ANY_NAME_2, Scan.Ordering.Order.DESC))
            .withOrdering(new Scan.Ordering(ANY_NAME_7, Scan.Ordering.Order.DESC));
    List<Map<String, AttributeValue>> items = new ArrayList<>();
    items.add(item1);
    items.add(item2);
    items.add(item3);
    ItemSorter sorter = new ItemSorter(scan, metadata);

    // Act
    List<Map<String, AttributeValue>> actual = sorter.sort(items);

    // Assert
    assertThat(actual.get(0).get(ANY_NAME_2).n()).isEqualTo(String.valueOf(ANY_LARGE_INT));
    assertThat(actual.get(0).get(ANY_NAME_7).s()).isEqualTo(ANY_SMALL_TEXT);
    assertThat(actual.get(1).get(ANY_NAME_2).n()).isEqualTo(String.valueOf(ANY_SMALL_INT));
    assertThat(actual.get(1).get(ANY_NAME_7).s()).isEqualTo(ANY_LARGE_TEXT);
    assertThat(actual.get(2).get(ANY_NAME_2).n()).isEqualTo(String.valueOf(ANY_SMALL_INT));
    assertThat(actual.get(2).get(ANY_NAME_7).s()).isEqualTo(ANY_SMALL_TEXT);
  }
}
