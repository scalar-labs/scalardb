package com.scalar.db.storage.dynamo;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
  private static final String ANY_LARGE_TEXT = "sssssss";
  private static final String ANY_SMALL_TEXT = "aa";
  private static final String ANY_COLUMN_NAME_1 = "val1";

  private TableMetadata metadata;

  @Before
  public void setUp() throws Exception {
    Map<String, AttributeValue> metadataMap = new HashMap<>();
    metadataMap.put("partitionKey", AttributeValue.builder().ss(ANY_NAME_1).build());
    metadataMap.put("clusteringKey", AttributeValue.builder().ss(ANY_NAME_2, ANY_NAME_3).build());
    Map<String, AttributeValue> columns = new HashMap<>();
    columns.put(ANY_NAME_1, AttributeValue.builder().s("text").build());
    columns.put(ANY_NAME_2, AttributeValue.builder().s("int").build());
    columns.put(ANY_NAME_3, AttributeValue.builder().s("text").build());
    columns.put(ANY_COLUMN_NAME_1, AttributeValue.builder().s("text").build());
    metadataMap.put("columns", AttributeValue.builder().m(columns).build());
    metadata = new TableMetadata(metadataMap);
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
  public void one_MultipleItemsGivenWithoutOrderingAndLimit_ShouldReturnSmallResult() {
    // Arrange
    Map<String, AttributeValue> item1 = prepareItem();
    Map<String, AttributeValue> item2 = prepareItem();
    item2.put(ANY_NAME_2, AttributeValue.builder().n(String.valueOf(ANY_LARGE_INT)).build());
    Scan scan = new Scan(new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1)));
    List<Map<String, AttributeValue>> items = new ArrayList<>();
    items.add(item1);
    items.add(item2);
    ScannerImpl scanner = new ScannerImpl(items, scan, metadata);

    // Act
    Optional<Result> actual = scanner.one();

    // Assert
    assertThat(actual.get().getValue(ANY_NAME_2).get())
        .isEqualTo(new IntValue(ANY_NAME_2, ANY_SMALL_INT));
  }

  @Test
  public void one_MultipleItemsGivenWithDescOrdering_ShouldReturnLargeResult() {
    // Arrange
    Map<String, AttributeValue> item1 = prepareItem();
    Map<String, AttributeValue> item2 = prepareItem();
    item2.put(ANY_NAME_2, AttributeValue.builder().n(String.valueOf(ANY_LARGE_INT)).build());
    List<Map<String, AttributeValue>> items = new ArrayList<>();
    items.add(item1);
    items.add(item2);
    Scan scan =
        new Scan(new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1)))
            .withOrdering(new Scan.Ordering(ANY_NAME_2, Scan.Ordering.Order.DESC));

    ScannerImpl scanner = new ScannerImpl(items, scan, metadata);

    // Act
    Optional<Result> actual = scanner.one();

    // Assert
    assertThat(actual.get().getValue(ANY_NAME_2).get())
        .isEqualTo(new IntValue(ANY_NAME_2, ANY_LARGE_INT));
  }

  @Test
  public void all_MultipleItemsGivenWithoutOrderingAndLimit_ShouldReturnAllResults() {
    // Arrange
    Map<String, AttributeValue> item1 = prepareItem();
    Map<String, AttributeValue> item2 = prepareItem();
    item2.put(ANY_NAME_2, AttributeValue.builder().n(String.valueOf(ANY_LARGE_INT)).build());
    List<Map<String, AttributeValue>> items = new ArrayList<>();
    items.add(item1);
    items.add(item2);
    Scan scan = new Scan(new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1)));

    ScannerImpl scanner = new ScannerImpl(items, scan, metadata);

    // Act
    List<Result> actual = scanner.all();

    // Assert
    assertThat(actual.size()).isEqualTo(2);
  }

  @Test
  public void all_MultipleItemsGivenWithMultipleOrdering_ShouldReturnLargeResult() {
    // Arrange
    Map<String, AttributeValue> item1 = prepareItem();
    Map<String, AttributeValue> item2 = prepareItem();
    item2.put(ANY_NAME_3, AttributeValue.builder().s(String.valueOf(ANY_LARGE_TEXT)).build());
    List<Map<String, AttributeValue>> items = new ArrayList<>();
    items.add(item1);
    items.add(item2);
    Scan scan =
        new Scan(new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1)))
            .withOrdering(new Scan.Ordering(ANY_NAME_2, Scan.Ordering.Order.DESC))
            .withOrdering(new Scan.Ordering(ANY_NAME_3, Scan.Ordering.Order.DESC));

    ScannerImpl scanner = new ScannerImpl(items, scan, metadata);

    // Act
    List<Result> actual = scanner.all();

    // Assert
    assertThat(actual.size()).isEqualTo(2);
    assertThat(actual.get(0).getValue(ANY_NAME_3).get())
        .isEqualTo(new TextValue(ANY_NAME_3, ANY_LARGE_TEXT));
  }

  @Test
  public void all_MultipleItemsGivenWithLimit_ShouldReturnLargeResult() {
    // Arrange
    Map<String, AttributeValue> item1 = prepareItem();
    Map<String, AttributeValue> item2 = prepareItem();
    item2.put(ANY_NAME_2, AttributeValue.builder().n(String.valueOf(ANY_LARGE_INT)).build());
    List<Map<String, AttributeValue>> items = new ArrayList<>();
    items.add(item1);
    items.add(item2);
    Scan scan = new Scan(new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1))).withLimit(1);

    ScannerImpl scanner = new ScannerImpl(items, scan, metadata);

    // Act
    List<Result> actual = scanner.all();

    // Assert
    assertThat(actual.size()).isEqualTo(1);
  }
}
