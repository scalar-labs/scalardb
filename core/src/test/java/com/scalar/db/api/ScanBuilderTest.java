package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.io.Key;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ScanBuilderTest {
  private static final String NAMESPACE_1 = "namespace1";
  private static final String NAMESPACE_2 = "namespace2";

  private static final String TABLE_1 = "table1";
  private static final String TABLE_2 = "table2";
  @Mock private Key partitionKey1;
  @Mock private Key partitionKey2;
  @Mock private Key startClusteringKey1;
  @Mock private Key startClusteringKey2;
  @Mock private Key endClusteringKey1;
  @Mock private Key endClusteringKey2;
  @Mock private Scan.Ordering ordering1;
  @Mock private Scan.Ordering ordering2;
  @Mock private Scan.Ordering ordering3;
  @Mock private Scan.Ordering ordering4;
  @Mock private Scan.Ordering ordering5;
  @Mock private Key indexKey1;
  @Mock private Key indexKey2;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  @Test
  public void buildScan_WithMandatoryParameters_ShouldBuildScanWithMandatoryParameters() {
    // Arrange Act
    Scan actual = Scan.newBuilder().table(TABLE_1).partitionKey(partitionKey1).build();

    // Assert
    assertThat(actual).isEqualTo(new Scan(partitionKey1).forTable(TABLE_1));
  }

  @Test
  public void buildScan_ScanWithAllParameters_ShouldBuildScanCorrectly() {
    // Arrange Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .start(startClusteringKey1)
            .end(endClusteringKey1, true)
            .ordering(ordering1)
            .orderings(Arrays.asList(ordering2, ordering3))
            .orderings(ordering4, ordering5)
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();

    // Assert
    assertThat(scan)
        .isEqualTo(
            new Scan(partitionKey1)
                .forNamespace(NAMESPACE_1)
                .forTable(TABLE_1)
                .withConsistency(Consistency.EVENTUAL)
                .withStart(startClusteringKey1)
                .withEnd(endClusteringKey1)
                .withOrdering(ordering1)
                .withOrdering(ordering2)
                .withOrdering(ordering3)
                .withOrdering(ordering4)
                .withOrdering(ordering5)
                .withLimit(10)
                .withProjections(Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"))
                .withConsistency(Consistency.EVENTUAL));
  }

  @Test
  public void buildScan_ScanWithInclusiveStartAndEnd_ShouldBuildScanCorrectly() {
    // Arrange Act
    Scan scan1 =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .start(startClusteringKey1)
            .end(endClusteringKey1)
            .build();
    Scan scan2 =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .start(startClusteringKey1, true)
            .end(endClusteringKey1, true)
            .build();

    // Assert
    Scan expectedScan =
        new Scan(partitionKey1)
            .forNamespace(NAMESPACE_1)
            .forTable(TABLE_1)
            .withStart(startClusteringKey1, true)
            .withEnd(endClusteringKey1, true);
    assertThat(scan1).isEqualTo(expectedScan);
    assertThat(scan2).isEqualTo(expectedScan);
  }

  @Test
  public void buildScan_ScanWithExclusiveStartAndEnd_ShouldBuildScanCorrectly() {
    // Arrange Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .start(startClusteringKey1, false)
            .end(endClusteringKey1, false)
            .build();

    // Assert
    Scan expectedScan =
        new Scan(partitionKey1)
            .forNamespace(NAMESPACE_1)
            .forTable(TABLE_1)
            .withStart(startClusteringKey1, false)
            .withEnd(endClusteringKey1, false);
    assertThat(scan).isEqualTo(expectedScan);
  }

  @Test
  public void buildScan_FromExistingWithoutChange_ShouldCopy() {
    // Arrange
    Scan existingScan =
        new Scan(partitionKey1)
            .forNamespace(NAMESPACE_1)
            .forTable(TABLE_1)
            .withConsistency(Consistency.EVENTUAL)
            .withStart(startClusteringKey1)
            .withEnd(endClusteringKey1)
            .withOrdering(ordering1)
            .withOrdering(ordering2)
            .withLimit(10)
            .withProjections(Arrays.asList("pk1", "ck1"))
            .withConsistency(Consistency.EVENTUAL);

    // Act
    Scan newScan = Scan.newBuilder(existingScan).build();

    // Assert
    assertThat(newScan).isEqualTo(existingScan);
  }

  @Test
  public void buildScan_FromExistingAndUpdateAllParameters_ShouldBuildScanWithUpdatedParameters() {
    // Arrange
    Scan existingScan =
        new Scan(partitionKey1)
            .forNamespace(NAMESPACE_1)
            .forTable(TABLE_1)
            .withConsistency(Consistency.EVENTUAL)
            .withStart(startClusteringKey1)
            .withEnd(endClusteringKey1)
            .withOrdering(ordering1)
            .withOrdering(ordering2)
            .withLimit(10)
            .withProjections(Arrays.asList("pk1", "ck1", "ck2"))
            .withConsistency(Consistency.EVENTUAL);

    // Act
    Scan newScan =
        Scan.newBuilder(existingScan)
            .namespace(NAMESPACE_2)
            .table(TABLE_2)
            .partitionKey(partitionKey2)
            .start(startClusteringKey2, false)
            .end(endClusteringKey2, false)
            .clearOrderings()
            .ordering(ordering3)
            .orderings(Arrays.asList(ordering4, ordering5))
            .orderings(ordering1, ordering2)
            .limit(5)
            .clearProjections()
            .projections(Arrays.asList("pk2", "ck2"))
            .projection("ck3")
            .projections("ck4", "ck5")
            .consistency(Consistency.LINEARIZABLE)
            .build();

    // Assert
    assertThat(newScan)
        .isEqualTo(
            new Scan(partitionKey2)
                .forNamespace(NAMESPACE_2)
                .forTable(TABLE_2)
                .withStart(startClusteringKey2, false)
                .withEnd(endClusteringKey2, false)
                .withOrdering(ordering3)
                .withOrdering(ordering4)
                .withOrdering(ordering5)
                .withOrdering(ordering1)
                .withOrdering(ordering2)
                .withLimit(5)
                .withProjections(Arrays.asList("pk2", "ck2", "ck3", "ck4", "ck5"))
                .withConsistency(Consistency.LINEARIZABLE));
  }

  @Test
  public void buildScan_FromExistingAndClearBoundaries_ShouldBuildScanWithoutBoundaries() {
    // Arrange
    Scan existingScan =
        new Scan(partitionKey1)
            .forNamespace(NAMESPACE_1)
            .forTable(TABLE_1)
            .withStart(startClusteringKey1)
            .withEnd(endClusteringKey1);

    // Act
    Scan newScan = Scan.newBuilder(existingScan).clearStart().clearEnd().build();

    // Assert
    assertThat(newScan)
        .isEqualTo(new Scan(partitionKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1));
  }

  @Test
  public void buildScan_FromExistingAndClearNamespace_ShouldBuildScanWithoutNamespace() {
    // Arrange
    Scan existingScan = new Scan(partitionKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1);

    // Act
    Scan newScan = Scan.newBuilder(existingScan).clearNamespace().build();

    // Assert
    assertThat(newScan).isEqualTo(new Scan(partitionKey1).forTable(TABLE_1));
  }

  @Test
  public void
      buildScan_FromExistingWithUnsupportedOperation_ShouldThrowUnsupportedOperationException() {
    // Arrange
    Scan existingScan = new Scan(partitionKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1);

    // Act Assert
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).indexKey(indexKey1))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void buildScanAll_WithMandatoryParameters_ShouldBuildScanWithMandatoryParameters() {
    // Arrange Act
    Scan actual = Scan.newBuilder().table(TABLE_1).all().build();

    // Assert
    assertThat(actual).isEqualTo(new ScanAll().forTable(TABLE_1));
  }

  @Test
  public void buildScanAll_ScanWithAllParameters_ShouldBuildScanCorrectly() {
    // Arrange Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .all()
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();

    // Assert
    assertThat(scan)
        .isEqualTo(
            new ScanAll()
                .forNamespace(NAMESPACE_1)
                .forTable(TABLE_1)
                .withConsistency(Consistency.EVENTUAL)
                .withLimit(10)
                .withProjections(Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"))
                .withConsistency(Consistency.EVENTUAL));
  }

  @Test
  public void buildScanAll_FromExistingWithoutChange_ShouldCopy() {
    // Arrange
    Scan existingScan =
        new ScanAll()
            .forNamespace(NAMESPACE_1)
            .forTable(TABLE_1)
            .withConsistency(Consistency.EVENTUAL)
            .withLimit(10)
            .withProjections(Arrays.asList("pk1", "ck1"))
            .withConsistency(Consistency.EVENTUAL);

    // Act
    Scan newScan = Scan.newBuilder(existingScan).build();

    // Assert
    assertThat(newScan).isEqualTo(existingScan);
  }

  @Test
  public void
      buildScanAll_FromExistingAndUpdateAllParameters_ShouldBuildScanWithUpdatedParameters() {
    // Arrange
    Scan existingScan =
        new ScanAll()
            .forNamespace(NAMESPACE_1)
            .forTable(TABLE_1)
            .withConsistency(Consistency.EVENTUAL)
            .withLimit(10)
            .withProjections(Arrays.asList("pk1", "ck1"))
            .withConsistency(Consistency.EVENTUAL);

    // Act
    Scan newScan =
        Scan.newBuilder(existingScan)
            .namespace(NAMESPACE_2)
            .table(TABLE_2)
            .limit(5)
            .clearProjections()
            .projections(Arrays.asList("pk2", "ck2"))
            .projection("ck3")
            .projections("ck4", "ck5")
            .consistency(Consistency.LINEARIZABLE)
            .build();

    // Assert
    assertThat(newScan)
        .isEqualTo(
            new ScanAll()
                .forNamespace(NAMESPACE_2)
                .forTable(TABLE_2)
                .withLimit(5)
                .withProjections(Arrays.asList("pk2", "ck2", "ck3", "ck4", "ck5"))
                .withConsistency(Consistency.LINEARIZABLE));
  }

  @Test
  public void
      buildScanAll_FromExistingWithUnsupportedOperation_ShouldThrowUnsupportedOperationException() {
    // Arrange
    Scan existingScan = new ScanAll().forNamespace(NAMESPACE_1).forTable(TABLE_1);

    // Act Assert
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).partitionKey(partitionKey1))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).indexKey(indexKey1))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).clearOrderings())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).start(startClusteringKey1))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).start(startClusteringKey1, false))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).end(endClusteringKey1, false))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).end(endClusteringKey1))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).ordering(ordering1))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).clearStart())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).clearEnd())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void buildScanAll_FromExistingAndClearNamespace_ShouldBuildScanWithoutNamespace() {
    // Arrange
    ScanAll existingScan = new ScanAll().forNamespace(NAMESPACE_1).forTable(TABLE_1);

    // Act
    Scan newScan = Scan.newBuilder(existingScan).clearNamespace().build();

    // Assert
    assertThat(newScan).isEqualTo(new ScanAll().forTable(TABLE_1));
  }

  @Test
  public void buildScanWithIndex_WithMandatoryParameters_ShouldBuildScanWithMandatoryParameters() {
    // Arrange Act
    Scan actual = Scan.newBuilder().table(TABLE_1).indexKey(indexKey1).build();

    // Assert
    assertThat(actual).isEqualTo(new ScanWithIndex(indexKey1).forTable(TABLE_1));
  }

  @Test
  public void buildScanWithIndex_ScanWithAllParameters_ShouldBuildScanCorrectly() {
    // Arrange Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();

    // Assert
    assertThat(scan)
        .isEqualTo(
            new ScanWithIndex(indexKey1)
                .forNamespace(NAMESPACE_1)
                .forTable(TABLE_1)
                .withConsistency(Consistency.EVENTUAL)
                .withLimit(10)
                .withProjections(Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"))
                .withConsistency(Consistency.EVENTUAL));
  }

  @Test
  public void buildScanWithIndex_FromExistingWithoutChange_ShouldCopy() {
    // Arrange
    Scan existingScan =
        new ScanWithIndex(indexKey1)
            .forNamespace(NAMESPACE_1)
            .forTable(TABLE_1)
            .withConsistency(Consistency.EVENTUAL)
            .withLimit(10)
            .withProjections(Arrays.asList("pk1", "ck1"))
            .withConsistency(Consistency.EVENTUAL);

    // Act
    Scan newScan = Scan.newBuilder(existingScan).build();

    // Assert
    assertThat(newScan).isEqualTo(existingScan);
  }

  @Test
  public void
      buildScanWithIndex_FromExistingAndUpdateAllParameters_ShouldBuildScanWithUpdatedParameters() {
    // Arrange
    Scan existingScan =
        new ScanWithIndex(indexKey1)
            .forNamespace(NAMESPACE_1)
            .forTable(TABLE_1)
            .withConsistency(Consistency.EVENTUAL)
            .withLimit(10)
            .withProjections(Arrays.asList("pk1", "ck1"))
            .withConsistency(Consistency.EVENTUAL);

    // Act
    Scan newScan =
        Scan.newBuilder(existingScan)
            .namespace(NAMESPACE_2)
            .table(TABLE_2)
            .indexKey(indexKey2)
            .limit(5)
            .clearProjections()
            .projections(Arrays.asList("pk2", "ck2"))
            .projection("ck3")
            .projections("ck4", "ck5")
            .consistency(Consistency.LINEARIZABLE)
            .build();

    // Assert
    assertThat(newScan)
        .isEqualTo(
            new ScanWithIndex(indexKey2)
                .forNamespace(NAMESPACE_2)
                .forTable(TABLE_2)
                .withLimit(5)
                .withProjections(Arrays.asList("pk2", "ck2", "ck3", "ck4", "ck5"))
                .withConsistency(Consistency.LINEARIZABLE));
  }

  @Test
  public void
      buildScanWithIndex_FromExistingWithUnsupportedOperation_ShouldThrowUnsupportedOperationException() {
    // Arrange
    Scan existingScan = new ScanWithIndex(indexKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1);

    // Act Assert
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).partitionKey(partitionKey1))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).clearOrderings())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).start(startClusteringKey1))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).start(startClusteringKey1, false))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).end(endClusteringKey1, false))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).end(endClusteringKey1))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).ordering(ordering1))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).clearStart())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).clearEnd())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void buildScanWithIndex_FromExistingAndClearNamespace_ShouldBuildScanWithoutNamespace() {
    // Arrange
    ScanWithIndex existingScan =
        new ScanWithIndex(indexKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1);

    // Act
    Scan newScan = Scan.newBuilder(existingScan).clearNamespace().build();

    // Assert
    assertThat(newScan).isEqualTo(new ScanWithIndex(indexKey1).forTable(TABLE_1));
  }
}
