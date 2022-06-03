package com.scalar.db.api.builder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.api.Consistency;
import com.scalar.db.api.Scan;
import com.scalar.db.api.ScanAll;
import com.scalar.db.io.Key;
import java.util.Arrays;
import java.util.Collections;
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

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  @Test
  public void buildScan_WithMandatoryParameters_ShouldBuildScanWithMandatoryParameters() {
    // Arrange Act
    Scan actual =
        Scan.newBuilder().namespace(NAMESPACE_1).table(TABLE_1).partitionKey(partitionKey1).build();

    // Assert
    assertThat(actual)
        .isEqualTo(new Scan(partitionKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1));
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
            .ordering(ordering2)
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
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
                .withLimit(10)
                .withProjections(Arrays.asList("pk1", "ck1", "ck2"))
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
            .ordering(ordering4)
            .limit(5)
            .clearProjections()
            .projections(Arrays.asList("pk2", "ck2"))
            .projection("ck3")
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
                .withLimit(5)
                .withProjections(Arrays.asList("pk2", "ck2", "ck3"))
                .withConsistency(Consistency.LINEARIZABLE));
  }

  @Test
  public void buildScanAll_WithMandatoryParameters_ShouldBuildScanWithMandatoryParameters() {
    // Arrange Act
    Scan actual = Scan.newBuilder().namespace(NAMESPACE_1).table(TABLE_1).all().build();

    // Assert
    assertThat(actual).isEqualTo(new ScanAll().forNamespace(NAMESPACE_1).forTable(TABLE_1));
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
                .withProjections(Arrays.asList("pk1", "ck1", "ck2"))
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
    Scan newScan = Scan.newBuilder(existingScan).all().build();

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
            .all()
            .namespace(NAMESPACE_2)
            .table(TABLE_2)
            .limit(5)
            .clearProjections()
            .projections(Arrays.asList("pk2", "ck2"))
            .projection("ck3")
            .consistency(Consistency.LINEARIZABLE)
            .build();

    // Assert
    assertThat(newScan)
        .isEqualTo(
            new ScanAll()
                .forNamespace(NAMESPACE_2)
                .forTable(TABLE_2)
                .withLimit(5)
                .withProjections(Arrays.asList("pk2", "ck2", "ck3"))
                .withConsistency(Consistency.LINEARIZABLE));
  }

  @Test
  public void buildScanAll_FromExistingScan_ShouldThrowIllegalArgumentException() {
    Scan existingScan = new Scan(partitionKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1);

    // Act Assert
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).all())
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void buildScan_FromExistingScanAll_ShouldThrowIllegalArgumentException() {
    // Arrange
    Scan existingScanAll = new ScanAll().forNamespace(NAMESPACE_1).forTable(TABLE_1);

    // Act Assert
    assertThatThrownBy(() -> Scan.newBuilder(existingScanAll).partitionKey(partitionKey1))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScanAll).namespace(NAMESPACE_1))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScanAll).table(TABLE_1))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScanAll).clearOrderings())
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScanAll).clearProjections())
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScanAll).start(startClusteringKey1))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScanAll).start(startClusteringKey1, false))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScanAll).end(endClusteringKey1, false))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScanAll).end(endClusteringKey1))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScanAll).consistency(Consistency.EVENTUAL))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScanAll).limit(10))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScanAll).ordering(ordering1))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScanAll).projection(""))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScanAll).projections(Collections.emptyList()))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScanAll).build())
        .isInstanceOf(IllegalStateException.class);
  }
}
