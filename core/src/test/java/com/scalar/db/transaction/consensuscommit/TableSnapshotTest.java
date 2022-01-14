package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

public class TableSnapshotTest {

  private static final int CLUSTERING_KEY_NUM = 10;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  @Test
  public void put_Once_WithTableMetadataWithClusteringKey_ShouldPutProperly() {
    // Arrange
    TableSnapshot tableSnapshot = new TableSnapshot(getTableMetadataWithClusteringKey());

    Key partitionKey = new Key("pKey", 1);
    Optional<Key> clusteringKey = Optional.of(new Key("cKey1", 2, "cKey2", 3));
    Map<String, Value<?>> values =
        ImmutableMap.of(
            "col1",
            new IntValue("col1", 4),
            "col2",
            new TextValue("col2", "aaa"),
            "col3",
            new TextValue("col3", "bbb"));

    // Act
    tableSnapshot.put(partitionKey, clusteringKey, values);

    // Assert
    Optional<Result> result = tableSnapshot.get(partitionKey, clusteringKey);
    assertThat(result).isPresent();
    assertThat(result.get().getPartitionKey()).isEqualTo(Optional.of(partitionKey));
    assertThat(result.get().getClusteringKey()).isEqualTo(clusteringKey);
    assertThat(result.get().getValue("col1").isPresent()).isTrue();
    assertThat(result.get().getValue("col1").get().getAsInt()).isEqualTo(4);
    assertThat(result.get().getValue("col2").isPresent()).isTrue();
    assertThat(result.get().getValue("col2").get().getAsString()).isEqualTo(Optional.of("aaa"));
    assertThat(result.get().getValue("col3").isPresent()).isTrue();
    assertThat(result.get().getValue("col3").get().getAsString()).isEqualTo(Optional.of("bbb"));
  }

  @Test
  public void put_Twice_WithTableMetadataWithClusteringKey_ShouldPutProperly() {
    // Arrange
    TableSnapshot tableSnapshot = new TableSnapshot(getTableMetadataWithClusteringKey());

    Key partitionKey = new Key("pKey", 1);
    Optional<Key> clusteringKey = Optional.of(new Key("cKey1", 2, "cKey2", 3));
    Map<String, Value<?>> values1 =
        ImmutableMap.of(
            "col1",
            new IntValue("col1", 4),
            "col2",
            new TextValue("col2", "aaa"),
            "col3",
            new TextValue("col3", "bbb"));
    Map<String, Value<?>> values2 =
        ImmutableMap.of("col2", new TextValue("col2", "bbb"), "col3", new TextValue("col3", "ccc"));

    // Act
    tableSnapshot.put(partitionKey, clusteringKey, values1);
    tableSnapshot.put(partitionKey, clusteringKey, values2);

    // Assert
    Optional<Result> result = tableSnapshot.get(partitionKey, clusteringKey);
    assertThat(result).isPresent();
    assertThat(result.get().getPartitionKey()).isEqualTo(Optional.of(partitionKey));
    assertThat(result.get().getClusteringKey()).isEqualTo(clusteringKey);
    assertThat(result.get().getValue("col1").isPresent()).isTrue();
    assertThat(result.get().getValue("col1").get().getAsInt()).isEqualTo(4);
    assertThat(result.get().getValue("col2").isPresent()).isTrue();
    assertThat(result.get().getValue("col2").get().getAsString()).isEqualTo(Optional.of("bbb"));
    assertThat(result.get().getValue("col3").isPresent()).isTrue();
    assertThat(result.get().getValue("col3").get().getAsString()).isEqualTo(Optional.of("ccc"));
  }

  @Test
  public void put_WithTableMetadataWithoutClusteringKey_ShouldPutProperly() {
    // Arrange
    TableSnapshot tableSnapshot = new TableSnapshot(getTableMetadataWithoutClusteringKey());

    Key partitionKey = new Key("pKey", 1);
    Optional<Key> clusteringKey = Optional.empty();
    Map<String, Value<?>> values =
        ImmutableMap.of(
            "col1",
            new IntValue("col1", 3),
            "col2",
            new TextValue("col2", "aaa"),
            "col3",
            new TextValue("col3", "bbb"));

    // Act
    tableSnapshot.put(partitionKey, clusteringKey, values);

    // Assert
    Optional<Result> result = tableSnapshot.get(partitionKey, clusteringKey);
    assertThat(result).isPresent();
    assertThat(result.get().getPartitionKey()).isEqualTo(Optional.of(partitionKey));
    assertThat(result.get().getClusteringKey()).isNotPresent();
    assertThat(result.get().getValue("col1").isPresent()).isTrue();
    assertThat(result.get().getValue("col1").get().getAsInt()).isEqualTo(3);
    assertThat(result.get().getValue("col2").isPresent()).isTrue();
    assertThat(result.get().getValue("col2").get().getAsString()).isEqualTo(Optional.of("aaa"));
    assertThat(result.get().getValue("col3").isPresent()).isTrue();
    assertThat(result.get().getValue("col3").get().getAsString()).isEqualTo(Optional.of("bbb"));
  }

  @Test
  public void delete_WithTableMetadataWithClusteringKey_ShouldDeleteProperly() {
    // Arrange
    TableSnapshot tableSnapshot = new TableSnapshot(getTableMetadataWithClusteringKey());

    Key partitionKey = new Key("pKey", 1);
    Optional<Key> clusteringKey = Optional.of(new Key("cKey1", 2, "cKey2", 3));
    Map<String, Value<?>> values =
        ImmutableMap.of(
            "col1",
            new IntValue("col1", 4),
            "col2",
            new TextValue("col2", "aaa"),
            "col3",
            new TextValue("col3", "bbb"));
    tableSnapshot.put(partitionKey, clusteringKey, values);

    // Act
    tableSnapshot.delete(partitionKey, clusteringKey);

    // Assert
    Optional<Result> result = tableSnapshot.get(partitionKey, clusteringKey);
    assertThat(result).isNotPresent();
  }

  @Test
  public void delete_WithTableMetadataWithoutClusteringKey_ShouldDeleteProperly() {
    // Arrange
    TableSnapshot tableSnapshot = new TableSnapshot(getTableMetadataWithoutClusteringKey());

    Key partitionKey = new Key("pKey", 1);
    Optional<Key> clusteringKey = Optional.empty();
    Map<String, Value<?>> values =
        ImmutableMap.of(
            "col1",
            new IntValue("col1", 3),
            "col2",
            new TextValue("col2", "aaa"),
            "col3",
            new TextValue("col3", "bbb"));
    tableSnapshot.put(partitionKey, clusteringKey, values);

    // Act
    tableSnapshot.delete(partitionKey, clusteringKey);

    // Assert
    Optional<Result> result = tableSnapshot.get(partitionKey, clusteringKey);
    assertThat(result).isNotPresent();
  }

  @Test
  public void get_WithTableMetadataWithClusteringKey_ShouldGetProperly() {
    // Arrange
    TableSnapshot tableSnapshot = new TableSnapshot(getTableMetadataWithClusteringKey());

    Key partitionKey = new Key("pKey", 1);
    Optional<Key> clusteringKey = Optional.of(new Key("cKey1", 2, "cKey2", 3));
    Map<String, Value<?>> values =
        ImmutableMap.of(
            "col1",
            new IntValue("col1", 4),
            "col2",
            new TextValue("col2", "aaa"),
            "col3",
            new TextValue("col3", "bbb"));
    tableSnapshot.put(partitionKey, clusteringKey, values);

    // Act
    Optional<Result> result = tableSnapshot.get(partitionKey, clusteringKey);

    // Assert
    assertThat(result).isPresent();
    assertThat(result.get().getPartitionKey()).isEqualTo(Optional.of(partitionKey));
    assertThat(result.get().getClusteringKey()).isEqualTo(clusteringKey);
    assertThat(result.get().getValue("col1").isPresent()).isTrue();
    assertThat(result.get().getValue("col1").get().getAsInt()).isEqualTo(4);
    assertThat(result.get().getValue("col2").isPresent()).isTrue();
    assertThat(result.get().getValue("col2").get().getAsString()).isEqualTo(Optional.of("aaa"));
    assertThat(result.get().getValue("col3").isPresent()).isTrue();
    assertThat(result.get().getValue("col3").get().getAsString()).isEqualTo(Optional.of("bbb"));
  }

  @Test
  public void get_WithTableMetadataWithoutClusteringKey_ShouldGetProperly() {
    // Arrange
    TableSnapshot tableSnapshot = new TableSnapshot(getTableMetadataWithoutClusteringKey());

    Key partitionKey = new Key("pKey", 1);
    Optional<Key> clusteringKey = Optional.empty();
    Map<String, Value<?>> values =
        ImmutableMap.of(
            "col1",
            new IntValue("col1", 4),
            "col2",
            new TextValue("col2", "aaa"),
            "col3",
            new TextValue("col3", "bbb"));
    tableSnapshot.put(partitionKey, clusteringKey, values);

    // Act
    Optional<Result> result = tableSnapshot.get(partitionKey, clusteringKey);

    // Assert
    assertThat(result).isPresent();
    assertThat(result.get().getPartitionKey()).isEqualTo(Optional.of(partitionKey));
    assertThat(result.get().getClusteringKey()).isNotPresent();
    assertThat(result.get().getValue("col1").isPresent()).isTrue();
    assertThat(result.get().getValue("col1").get().getAsInt()).isEqualTo(4);
    assertThat(result.get().getValue("col2").isPresent()).isTrue();
    assertThat(result.get().getValue("col2").get().getAsString()).isEqualTo(Optional.of("aaa"));
    assertThat(result.get().getValue("col3").isPresent()).isTrue();
    assertThat(result.get().getValue("col3").get().getAsString()).isEqualTo(Optional.of("bbb"));
  }

  @Test
  public void scan_WithTableMetadataWithClusteringKey_ShouldScanProperly() {
    for (Order clusteringOrder1 : Order.values()) {
      for (Order clusteringOrder2 : Order.values()) {
        scan_WithTableMetadataWithClusteringKey_ShouldScanProperly(
            clusteringOrder1, clusteringOrder2);
      }
    }
  }

  private void scan_WithTableMetadataWithClusteringKey_ShouldScanProperly(
      Order clusteringOrder1, Order clusteringOrder2) {
    // Arrange
    TableSnapshot tableSnapshot =
        new TableSnapshot(getTableMetadataWithClusteringKey(clusteringOrder1, clusteringOrder2));

    Key partitionKey = new Key("pKey", 1);
    prepareForScan(tableSnapshot, partitionKey);

    Optional<Key> startClusteringKey = Optional.empty();
    boolean startInclusive = true;
    Optional<Key> endClusteringKey = Optional.empty();
    boolean endInclusive = true;
    List<Ordering> orderings = Collections.emptyList();
    int limit = 0;

    // Act
    List<Result> results =
        tableSnapshot.scan(
            partitionKey,
            startClusteringKey,
            startInclusive,
            endClusteringKey,
            endInclusive,
            orderings,
            limit);

    // Assert
    assertThat(results.size()).isEqualTo(CLUSTERING_KEY_NUM * CLUSTERING_KEY_NUM);
    for (int i = 0; i < results.size(); i++) {
      int index1 = i / CLUSTERING_KEY_NUM;
      int index2 = i % CLUSTERING_KEY_NUM;
      assertResult(
          partitionKey,
          results.get(i),
          clusteringOrder1 == Order.ASC ? index1 : CLUSTERING_KEY_NUM - 1 - index1,
          clusteringOrder2 == Order.ASC ? index2 : CLUSTERING_KEY_NUM - 1 - index2);
    }
  }

  @Test
  public void scan_WithOrdering_WithTableMetadataWithClusteringKey_ShouldScanProperly() {
    for (Order clusteringOrder1 : Order.values()) {
      for (Order clusteringOrder2 : Order.values()) {
        scan_WithOrdering_WithTableMetadataWithClusteringKey_ShouldScanProperly(
            clusteringOrder1, clusteringOrder2, false);
        scan_WithOrdering_WithTableMetadataWithClusteringKey_ShouldScanProperly(
            clusteringOrder1, clusteringOrder2, true);
      }
    }
  }

  private void scan_WithOrdering_WithTableMetadataWithClusteringKey_ShouldScanProperly(
      Order clusteringOrder1, Order clusteringOrder2, boolean reverse) {
    // Arrange
    TableSnapshot tableSnapshot =
        new TableSnapshot(getTableMetadataWithClusteringKey(clusteringOrder1, clusteringOrder2));

    Key partitionKey = new Key("pKey", 1);
    prepareForScan(tableSnapshot, partitionKey);

    Optional<Key> startClusteringKey = Optional.empty();
    boolean startInclusive = true;
    Optional<Key> endClusteringKey = Optional.empty();
    boolean endInclusive = true;

    Order order1 = reverse ? reverse(clusteringOrder1) : clusteringOrder1;
    Order order2 = reverse ? reverse(clusteringOrder2) : clusteringOrder2;
    List<Ordering> orderings =
        Arrays.asList(new Ordering("cKey1", order1), new Ordering("cKey2", order2));
    int limit = 0;

    // Act
    List<Result> results =
        tableSnapshot.scan(
            partitionKey,
            startClusteringKey,
            startInclusive,
            endClusteringKey,
            endInclusive,
            orderings,
            limit);

    // Assert
    assertThat(results.size()).isEqualTo(CLUSTERING_KEY_NUM * CLUSTERING_KEY_NUM);
    for (int i = 0; i < results.size(); i++) {
      int index1 = i / CLUSTERING_KEY_NUM;
      int index2 = i % CLUSTERING_KEY_NUM;
      assertResult(
          partitionKey,
          results.get(i),
          order1 == Order.ASC ? index1 : CLUSTERING_KEY_NUM - 1 - index1,
          order2 == Order.ASC ? index2 : CLUSTERING_KEY_NUM - 1 - index2);
    }
  }

  @Test
  public void scan_WithLimit_WithTableMetadataWithClusteringKey_ShouldScanProperly() {
    for (Order clusteringOrder1 : Order.values()) {
      for (Order clusteringOrder2 : Order.values()) {
        scan_WithLimit_WithTableMetadataWithClusteringKey_ShouldScanProperly(
            clusteringOrder1, clusteringOrder2);
      }
    }
  }

  private void scan_WithLimit_WithTableMetadataWithClusteringKey_ShouldScanProperly(
      Order clusteringOrder1, Order clusteringOrder2) {
    // Arrange
    TableSnapshot tableSnapshot =
        new TableSnapshot(getTableMetadataWithClusteringKey(clusteringOrder1, clusteringOrder2));

    Key partitionKey = new Key("pKey", 1);
    prepareForScan(tableSnapshot, partitionKey);

    Optional<Key> startClusteringKey = Optional.empty();
    boolean startInclusive = true;
    Optional<Key> endClusteringKey = Optional.empty();
    boolean endInclusive = true;
    List<Ordering> orderings = Collections.emptyList();
    int limit = 30;

    // Act
    List<Result> results =
        tableSnapshot.scan(
            partitionKey,
            startClusteringKey,
            startInclusive,
            endClusteringKey,
            endInclusive,
            orderings,
            limit);

    // Assert
    assertThat(results.size()).isEqualTo(limit);
    for (int i = 0; i < limit; i++) {
      int index1 = i / CLUSTERING_KEY_NUM;
      int index2 = i % CLUSTERING_KEY_NUM;
      assertResult(
          partitionKey,
          results.get(i),
          clusteringOrder1 == Order.ASC ? index1 : CLUSTERING_KEY_NUM - 1 - index1,
          clusteringOrder2 == Order.ASC ? index2 : CLUSTERING_KEY_NUM - 1 - index2);
    }
  }

  @Test
  public void scan_WithStartClusteringKey_WithTableMetadataWithClusteringKey_ShouldScanProperly() {
    for (Order clusteringOrder1 : Order.values()) {
      for (Order clusteringOrder2 : Order.values()) {
        scan_WithStartClusteringKey_WithTableMetadataWithClusteringKey_ShouldScanProperly(
            clusteringOrder1, clusteringOrder2, true);
        scan_WithStartClusteringKey_WithTableMetadataWithClusteringKey_ShouldScanProperly(
            clusteringOrder1, clusteringOrder2, false);
      }
    }
  }

  public void scan_WithStartClusteringKey_WithTableMetadataWithClusteringKey_ShouldScanProperly(
      Order clusteringOrder1, Order clusteringOrder2, boolean startInclusive) {
    TableSnapshot tableSnapshot =
        new TableSnapshot(getTableMetadataWithClusteringKey(clusteringOrder1, clusteringOrder2));

    // Arrange
    Key partitionKey = new Key("pKey", 1);
    prepareForScan(tableSnapshot, partitionKey);

    int cKey1Value = 2;
    int cKey2Value = 4;
    Optional<Key> startClusteringKey =
        Optional.of(new Key("cKey1", cKey1Value, "cKey2", cKey2Value));

    Optional<Key> endClusteringKey = Optional.empty();
    boolean endInclusive = true;

    List<Ordering> orderings = Collections.emptyList();
    int limit = 0;

    // Act
    List<Result> results =
        tableSnapshot.scan(
            partitionKey,
            startClusteringKey,
            startInclusive,
            endClusteringKey,
            endInclusive,
            orderings,
            limit);

    // Assert
    assertThat(results.size())
        .isEqualTo(CLUSTERING_KEY_NUM - cKey2Value - (startInclusive ? 0 : 1));
    for (int i = 0; i < results.size(); i++) {
      assertResult(
          partitionKey,
          results.get(i),
          cKey1Value,
          clusteringOrder2 == Order.ASC
              ? i + cKey2Value + (startInclusive ? 0 : 1)
              : CLUSTERING_KEY_NUM - 1 - i);
    }
  }

  // TODO Add more tests

  private TableMetadata getTableMetadataWithClusteringKey() {
    return getTableMetadataWithClusteringKey(Order.ASC, Order.ASC);
  }

  private TableMetadata getTableMetadataWithClusteringKey(
      Order clusteringOrder1, Order clusteringOrder2) {
    return TableMetadata.newBuilder()
        .addColumn("pKey", DataType.INT)
        .addColumn("cKey1", DataType.INT)
        .addColumn("cKey2", DataType.INT)
        .addColumn("col1", DataType.INT)
        .addColumn("col2", DataType.TEXT)
        .addColumn("col3", DataType.TEXT)
        .addPartitionKey("pKey")
        .addClusteringKey("cKey1", clusteringOrder1)
        .addClusteringKey("cKey2", clusteringOrder2)
        .build();
  }

  private TableMetadata getTableMetadataWithoutClusteringKey() {
    return TableMetadata.newBuilder()
        .addColumn("pKey", DataType.INT)
        .addColumn("col1", DataType.INT)
        .addColumn("col2", DataType.TEXT)
        .addColumn("col3", DataType.TEXT)
        .addPartitionKey("pKey")
        .build();
  }

  private void prepareForScan(TableSnapshot tableSnapshot, Key partitionKey) {
    for (int index1 = 0; index1 < CLUSTERING_KEY_NUM; index1++) {
      for (int index2 = 0; index2 < CLUSTERING_KEY_NUM; index2++) {
        Optional<Key> clusteringKey = Optional.of(new Key("cKey1", index1, "cKey2", index2));
        Map<String, Value<?>> values =
            ImmutableMap.of(
                "col1",
                new IntValue("col1", index1 + index2),
                "col2",
                new TextValue("col2", "val" + index1),
                "col3",
                new TextValue("col3", "val" + index2));
        tableSnapshot.put(partitionKey, clusteringKey, values);
      }
    }
  }

  private Order reverse(Order order) {
    switch (order) {
      case ASC:
        return Order.DESC;
      case DESC:
        return Order.ASC;
      default:
        throw new AssertionError();
    }
  }

  private void assertResult(Key partitionKey, Result result, int index1, int index2) {
    assertThat(result.getPartitionKey()).isEqualTo(Optional.of(partitionKey));
    assertThat(result.getClusteringKey())
        .isEqualTo(Optional.of(new Key("cKey1", index1, "cKey2", index2)));
    assertThat(result.getValue("col1").isPresent()).isTrue();
    assertThat(result.getValue("col1").get().getAsInt()).isEqualTo(index1 + index2);
    assertThat(result.getValue("col2").isPresent()).isTrue();
    assertThat(result.getValue("col2").get().getAsString()).isEqualTo(Optional.of("val" + index1));
    assertThat(result.getValue("col3").isPresent()).isTrue();
    assertThat(result.getValue("col3").get().getAsString()).isEqualTo(Optional.of("val" + index2));
  }
}
