package com.scalar.db.schemaloader.alteration;

import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TableMetadataAlterationProcessorTest {

  private static final String NAMESPACE = "ns";
  private static final String TABLE = "tbl";
  private TableMetadataAlterationProcessor processor;

  @BeforeEach
  public void setUp() {
    processor = new TableMetadataAlterationProcessor();
  }

  @Test
  public void computeAlteration_WithAddedPartitionKey_ShouldThrowIllegalArgumentException() {
    // Arrange
    TableMetadata oldMetadata =
        TableMetadata.newBuilder().addPartitionKey("pk1").addColumn("pk1", DataType.TEXT).build();
    TableMetadata newMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addPartitionKey("pk2")
            .addColumn("pk1", DataType.TEXT)
            .addColumn("pk2", DataType.TEXT)
            .build();

    // Act Assert
    assertThatThrownBy(
            () -> processor.computeAlteration(NAMESPACE, TABLE, oldMetadata, newMetadata))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("partition keys");
  }

  @Test
  public void computeAlteration_WithAddedClusteringKey_ShouldThrowIllegalArgumentException() {
    // Arrange
    TableMetadata oldMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addClusteringKey("ck1")
            .addColumn("pk1", DataType.TEXT)
            .addColumn("ck1", DataType.TEXT)
            .build();
    TableMetadata newMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addClusteringKey("ck1")
            .addClusteringKey("ck2")
            .addColumn("pk1", DataType.TEXT)
            .addColumn("ck1", DataType.TEXT)
            .addColumn("ck2", DataType.TEXT)
            .build();

    // Act Assert
    assertThatThrownBy(
            () -> processor.computeAlteration(NAMESPACE, TABLE, oldMetadata, newMetadata))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("clustering keys");
  }

  @Test
  public void
      computeAlteration_WithModifiedClusteringKeySortOrdering_ShouldThrowIllegalArgumentException() {
    // Arrange
    TableMetadata oldMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addClusteringKey("ck1", Order.ASC)
            .addColumn("pk1", DataType.TEXT)
            .addColumn("ck1", DataType.TEXT)
            .build();
    TableMetadata newMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addClusteringKey("ck1", Order.DESC)
            .addColumn("pk1", DataType.TEXT)
            .addColumn("ck1", DataType.TEXT)
            .build();

    // Act Assert
    assertThatThrownBy(
            () -> processor.computeAlteration(NAMESPACE, TABLE, oldMetadata, newMetadata))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("clustering ordering");
  }

  @Test
  public void computeAlteration_WithDeletedColumns_ShouldThrowIllegalArgumentException() {
    // Arrange
    TableMetadata oldMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addClusteringKey("ck1")
            .addColumn("pk1", DataType.TEXT)
            .addColumn("ck1", DataType.TEXT)
            .addColumn("c1", DataType.TEXT)
            .build();
    TableMetadata newMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addClusteringKey("ck1")
            .addColumn("pk1", DataType.TEXT)
            .addColumn("ck1", DataType.TEXT)
            .addColumn("c2", DataType.TEXT)
            .build();

    // Act Assert
    assertThatThrownBy(
            () -> processor.computeAlteration(NAMESPACE, TABLE, oldMetadata, newMetadata))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("deleted");
  }

  @Test
  public void computeAlteration_WithModifiedDataType_ShouldThrowIllegalArgumentException() {
    // Arrange
    TableMetadata oldMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addClusteringKey("ck1")
            .addColumn("pk1", DataType.TEXT)
            .addColumn("ck1", DataType.TEXT)
            .addColumn("c1", DataType.TEXT)
            .build();
    TableMetadata newMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addClusteringKey("ck1")
            .addColumn("pk1", DataType.TEXT)
            .addColumn("ck1", DataType.TEXT)
            .addColumn("c1", DataType.BOOLEAN)
            .build();

    // Act Assert
    assertThatThrownBy(
            () -> processor.computeAlteration(NAMESPACE, TABLE, oldMetadata, newMetadata))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("data type");
  }

  @Test
  public void computeAlteration_WithoutChanges_ShouldComputeCorrectAlteration() {
    // Arrange
    TableMetadata oldMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addColumn("pk1", DataType.TEXT)
            .addColumn("c1", DataType.TEXT)
            .build();
    TableMetadata newMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addColumn("pk1", DataType.TEXT)
            .addColumn("c1", DataType.TEXT)
            .build();

    // Act
    TableMetadataAlteration alteration =
        processor.computeAlteration(NAMESPACE, TABLE, oldMetadata, newMetadata);

    // Assert
    assertThat(alteration.getAddedColumnNames()).isEmpty();
    assertThat(alteration.getAddedColumnDataTypes()).isEmpty();
    assertThat(alteration.getAddedSecondaryIndexNames()).isEmpty();
    assertThat(alteration.getDeletedSecondaryIndexNames()).isEmpty();
  }

  @Test
  public void computeAlteration_WithAddedColumn_ShouldComputeCorrectAlteration() {
    // Arrange
    TableMetadata oldMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addColumn("pk1", DataType.TEXT)
            .addColumn("c1", DataType.TEXT)
            .build();
    TableMetadata newMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addColumn("pk1", DataType.TEXT)
            .addColumn("c1", DataType.TEXT)
            .addColumn("c2", DataType.BOOLEAN)
            .addColumn("c3", DataType.FLOAT)
            .build();

    // Act
    TableMetadataAlteration alteration =
        processor.computeAlteration(NAMESPACE, TABLE, oldMetadata, newMetadata);

    // Assert
    assertThat(alteration.getAddedColumnNames()).containsExactly("c2", "c3");
    assertThat(alteration.getAddedColumnDataTypes())
        .containsOnly(entry("c2", DataType.BOOLEAN), entry("c3", DataType.FLOAT));
    assertThat(alteration.getAddedSecondaryIndexNames()).isEmpty();
    assertThat(alteration.getDeletedSecondaryIndexNames()).isEmpty();
  }

  @Test
  public void computeAlteration_WithAddedSecondaryIndexes_ShouldComputeCorrectAlteration() {
    // Arrange
    TableMetadata oldMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addColumn("pk1", DataType.TEXT)
            .addColumn("c1", DataType.TEXT)
            .addColumn("c2", DataType.FLOAT)
            .addColumn("c3", DataType.BIGINT)
            .build();
    TableMetadata newMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addColumn("pk1", DataType.TEXT)
            .addColumn("c1", DataType.TEXT)
            .addColumn("c2", DataType.FLOAT)
            .addColumn("c3", DataType.BIGINT)
            .addSecondaryIndex("c3")
            .addSecondaryIndex("c1")
            .build();

    // Act
    TableMetadataAlteration alteration =
        processor.computeAlteration(NAMESPACE, TABLE, oldMetadata, newMetadata);

    // Assert
    assertThat(alteration.getAddedColumnNames()).isEmpty();
    assertThat(alteration.getAddedColumnDataTypes()).isEmpty();
    assertThat(alteration.getAddedSecondaryIndexNames()).containsOnly("c1", "c3");
    assertThat(alteration.getDeletedSecondaryIndexNames()).isEmpty();
  }

  @Test
  public void computeAlteration_WithDeletedSecondaryIndexes_ShouldComputeCorrectAlteration() {
    // Arrange
    TableMetadata oldMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addColumn("pk1", DataType.TEXT)
            .addColumn("c1", DataType.TEXT)
            .addColumn("c2", DataType.FLOAT)
            .addColumn("c3", DataType.BIGINT)
            .addSecondaryIndex("c3")
            .addSecondaryIndex("c1")
            .build();
    TableMetadata newMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addColumn("pk1", DataType.TEXT)
            .addColumn("c1", DataType.TEXT)
            .addColumn("c2", DataType.FLOAT)
            .addColumn("c3", DataType.BIGINT)
            .addSecondaryIndex("c1")
            .build();

    // Act
    TableMetadataAlteration alteration =
        processor.computeAlteration(NAMESPACE, TABLE, oldMetadata, newMetadata);

    // Assert
    assertThat(alteration.getAddedColumnNames()).isEmpty();
    assertThat(alteration.getAddedColumnDataTypes()).isEmpty();
    assertThat(alteration.getAddedSecondaryIndexNames()).isEmpty();
    assertThat(alteration.getDeletedSecondaryIndexNames()).containsOnly("c3");
  }

  @Test
  public void computeAlteration_WithAllPossibleAlterations_ShouldComputeCorrectAlteration() {
    // Arrange
    TableMetadata oldMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addColumn("pk1", DataType.TEXT)
            .addColumn("c1", DataType.TEXT)
            .addColumn("c2", DataType.FLOAT)
            .addSecondaryIndex("c1")
            .addSecondaryIndex("c2")
            .build();
    TableMetadata newMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addColumn("pk1", DataType.TEXT)
            .addColumn("c1", DataType.TEXT)
            .addColumn("c2", DataType.FLOAT)
            .addColumn("c3", DataType.BIGINT)
            .addColumn("c4", DataType.BLOB)
            .addSecondaryIndex("c1")
            .addSecondaryIndex("c3")
            .addSecondaryIndex("c4")
            .build();

    // Act
    TableMetadataAlteration alteration =
        processor.computeAlteration(NAMESPACE, TABLE, oldMetadata, newMetadata);

    // Assert
    assertThat(alteration.getAddedColumnNames()).containsExactly("c3", "c4");
    assertThat(alteration.getAddedColumnDataTypes())
        .containsOnly(entry("c3", DataType.BIGINT), entry("c4", DataType.BLOB));
    assertThat(alteration.getAddedSecondaryIndexNames()).containsOnly("c3", "c4");
    assertThat(alteration.getDeletedSecondaryIndexNames()).containsOnly("c2");
  }
}
