package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import java.util.Collections;
import java.util.Optional;
import org.junit.Test;

public class ScanTest {
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_TEXT_3 = "text3";
  private static final String ANY_TEXT_4 = "text4";

  private Scan prepareScan() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key startClusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    Key endClusteringKey = new Key(ANY_NAME_2, ANY_TEXT_3);
    Scan.Ordering ordering = new Scan.Ordering(ANY_NAME_2, Scan.Ordering.Order.ASC);

    return new Scan(partitionKey)
        .withStart(startClusteringKey, false)
        .withEnd(endClusteringKey, false)
        .withProjection(ANY_NAME_1)
        .withOrdering(ordering)
        .withLimit(100);
  }

  private Scan prepareAnotherScan() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key startClusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    Key endClusteringKey = new Key(ANY_NAME_2, ANY_TEXT_4);
    Scan.Ordering ordering = new Scan.Ordering(ANY_NAME_2, Scan.Ordering.Order.ASC);

    return new Scan(partitionKey)
        .withStart(startClusteringKey, false)
        .withEnd(endClusteringKey, false)
        .withProjection(ANY_NAME_1)
        .withOrdering(ordering)
        .withLimit(100);
  }

  @Test
  public void constructorAndSetters_AllSet_ShouldGetWhatsSet() {
    // Arrange
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key startClusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    Key endClusteringKey = new Key(ANY_NAME_2, ANY_TEXT_3);
    Scan.Ordering ordering = new Scan.Ordering(ANY_NAME_2, Scan.Ordering.Order.ASC);

    // Act
    Scan scan =
        new Scan(partitionKey)
            .withStart(startClusteringKey, false)
            .withEnd(endClusteringKey, false)
            .withProjection(ANY_NAME_1)
            .withOrdering(ordering)
            .withLimit(100);

    // Assert
    assertThat((Iterable<? extends Value<?>>) scan.getPartitionKey()).isEqualTo(partitionKey);
    assertThat(scan.getStartClusteringKey()).isEqualTo(Optional.of(startClusteringKey));
    assertThat(scan.getEndClusteringKey()).isEqualTo(Optional.of(endClusteringKey));
    assertThat(scan.getProjections()).isEqualTo(Collections.singletonList(ANY_NAME_1));
    assertThat(scan.getStartInclusive()).isFalse();
    assertThat(scan.getEndInclusive()).isFalse();
    assertThat(scan.getOrderings()).isEqualTo(Collections.singletonList(ordering));
    assertThat(scan.getLimit()).isEqualTo(100);
  }

  @Test
  public void constructor_NullGiven_ShouldThrowNullPointerException() {
    // Act Assert
    assertThatThrownBy(() -> new Scan((Key) null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void constructor_ScanGiven_ShouldCopyProperly() {
    // Arrange
    Scan scan =
        prepareScan()
            .withLimit(100)
            .withConsistency(Consistency.EVENTUAL)
            .forNamespace("n1")
            .forTable("t1");

    // Act
    Scan actual = new Scan(scan);

    // Assert
    assertThat(actual).isEqualTo(scan);
  }

  @Test
  public void equals_SameInstanceGiven_ShouldReturnTrue() {
    // Arrange
    Scan scan = prepareScan();

    // Act
    @SuppressWarnings("SelfEquals")
    boolean ret = scan.equals(scan);

    // Assert
    assertThat(ret).isTrue();
  }

  @Test
  public void equals_SameScanGiven_ShouldReturnTrue() {
    // Arrange
    Scan scan = prepareScan();
    Scan another = prepareScan();

    // Act
    boolean ret = scan.equals(another);

    // Assert
    assertThat(ret).isTrue();
    assertThat(scan.hashCode()).isEqualTo(another.hashCode());
  }

  @Test
  public void equals_ScanWithDifferentClusteringKeyGiven_ShouldReturnFalse() {
    // Arrange
    Scan scan = prepareScan();
    Scan another = prepareAnotherScan();

    // Act
    boolean ret = scan.equals(another);

    // Assert
    assertThat(ret).isFalse();
  }

  @Test
  public void equals_ScanWithDifferentOrderingGiven_ShouldReturnFalse() {
    // Arrange
    Scan scan = prepareScan();
    scan.withOrdering(new Scan.Ordering(ANY_NAME_2, Scan.Ordering.Order.DESC));
    Scan another = prepareScan();
    another.withOrdering(new Scan.Ordering(ANY_NAME_2, Scan.Ordering.Order.ASC));

    // Act
    boolean ret = scan.equals(another);

    // Assert
    assertThat(ret).isFalse();
  }

  @Test
  public void equals_ScanWithDifferentLimitGiven_ShouldReturnFalse() {
    // Arrange
    Scan scan = prepareScan();
    scan.withLimit(10);
    Scan another = prepareScan();
    another.withLimit(100);

    // Act
    boolean ret = scan.equals(another);

    // Assert
    assertThat(ret).isFalse();
  }
}
