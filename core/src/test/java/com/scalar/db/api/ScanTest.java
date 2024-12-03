package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Selection.Conjunction;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import java.util.Collections;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class ScanTest {
  private static final String ANY_NAMESPACE = "namespace";
  private static final String ANY_TABLE = "table";
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
    Scan.Ordering ordering = Scan.Ordering.asc(ANY_NAME_2);

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
    Scan.Ordering ordering = Scan.Ordering.asc(ANY_NAME_2);

    return new Scan(partitionKey)
        .withStart(startClusteringKey, false)
        .withEnd(endClusteringKey, false)
        .withProjection(ANY_NAME_1)
        .withOrdering(ordering)
        .withLimit(100);
  }

  private Scan prepareScanWithCondition(ConditionalExpression condition) {
    return Scan.newBuilder()
        .namespace(ANY_NAMESPACE)
        .table(ANY_TABLE)
        .all()
        .where(condition)
        .ordering(Scan.Ordering.asc(ANY_NAME_2))
        .limit(100)
        .build();
  }

  private Conjunction prepareConjunction() {
    return Conjunction.of(
        ConditionBuilder.column(ANY_NAME_1).isEqualToText(ANY_TEXT_1),
        ConditionBuilder.column(ANY_NAME_2).isEqualToText(ANY_TEXT_1));
  }

  private Conjunction prepareAnotherConjunction() {
    return Conjunction.of(
        ConditionBuilder.column(ANY_NAME_1).isEqualToText(ANY_TEXT_1),
        ConditionBuilder.column(ANY_NAME_2).isEqualToText(ANY_TEXT_2));
  }

  private Conjunction prepareConjunctionWithDifferentConditionOrder() {
    return Conjunction.of(
        ConditionBuilder.column(ANY_NAME_2).isEqualToText(ANY_TEXT_1),
        ConditionBuilder.column(ANY_NAME_1).isEqualToText(ANY_TEXT_1));
  }

  @Test
  public void constructorAndSetters_AllSet_ShouldGetWhatsSet() {
    // Arrange
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key startClusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    Key endClusteringKey = new Key(ANY_NAME_2, ANY_TEXT_3);
    Scan.Ordering ordering = Scan.Ordering.asc(ANY_NAME_2);

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
    scan.withOrdering(Scan.Ordering.desc(ANY_NAME_2));
    Scan another = prepareScan();
    another.withOrdering(Scan.Ordering.asc(ANY_NAME_2));

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

  @Test
  public void equals_ScanWithDifferentConjunctionsGiven_ShouldReturnFalse() {
    // Arrange
    Scan scan =
        prepareScanWithCondition(ConditionBuilder.column(ANY_NAME_1).isEqualToText(ANY_TEXT_1));
    Scan another =
        prepareScanWithCondition(ConditionBuilder.column(ANY_NAME_1).isEqualToText(ANY_TEXT_2));

    // Act
    boolean ret = scan.equals(another);

    // Assert
    assertThat(ret).isFalse();
  }

  @Test
  public void equals_SameConjunctionInstanceGiven_ShouldReturnTrue() {
    // Arrange
    Conjunction conjunction = prepareConjunction();

    // Act
    @SuppressWarnings("SelfEquals")
    boolean ret = conjunction.equals(conjunction);

    // Assert
    assertThat(ret).isTrue();
  }

  @Test
  public void equals_SameConjunctionGiven_ShouldReturnTrue() {
    // Arrange
    Conjunction conjunction = prepareConjunction();
    Conjunction another = prepareConjunction();

    // Act
    boolean ret = conjunction.equals(another);

    // Assert
    assertThat(ret).isTrue();
    assertThat(conjunction.hashCode()).isEqualTo(another.hashCode());
  }

  @Test
  public void equals_ConjunctionWithDifferentConditionGiven_ShouldReturnFalse() {
    // Arrange
    Conjunction conjunction = prepareConjunction();
    Conjunction another = prepareAnotherConjunction();

    // Act
    boolean ret = conjunction.equals(another);

    // Assert
    assertThat(ret).isFalse();
  }

  @Test
  public void equals_ConjunctionWithDifferentConditionOrderGiven_ShouldReturnTrue() {
    // Arrange
    Conjunction conjunction = prepareConjunction();
    Conjunction another = prepareConjunctionWithDifferentConditionOrder();

    // Act
    boolean ret = conjunction.equals(another);

    // Assert
    assertThat(ret).isTrue();
  }

  @Test
  public void getAttribute_ShouldReturnProperValues() {
    // Arrange
    Scan scan =
        Scan.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofText("pk", "pv"))
            .attribute("a1", "v1")
            .attributes(ImmutableMap.of("a2", "v2", "a3", "v3"))
            .build();
    Scan scanAll =
        Scan.newBuilder()
            .namespace("ns")
            .table("tbl")
            .all()
            .attribute("a1", "v1")
            .attributes(ImmutableMap.of("a2", "v2", "a3", "v3"))
            .build();
    Scan scanWithIndex =
        Scan.newBuilder()
            .namespace("ns")
            .table("tbl")
            .indexKey(Key.ofText("pk", "pv"))
            .attribute("a1", "v1")
            .attributes(ImmutableMap.of("a2", "v2", "a3", "v3"))
            .build();

    // Act Assert
    assertThat(scan.getAttribute("a1")).hasValue("v1");
    assertThat(scan.getAttribute("a2")).hasValue("v2");
    assertThat(scan.getAttribute("a3")).hasValue("v3");
    assertThat(scan.getAttributes()).isEqualTo(ImmutableMap.of("a1", "v1", "a2", "v2", "a3", "v3"));

    assertThat(scanAll.getAttribute("a1")).hasValue("v1");
    assertThat(scanAll.getAttribute("a2")).hasValue("v2");
    assertThat(scanAll.getAttribute("a3")).hasValue("v3");
    assertThat(scanAll.getAttributes())
        .isEqualTo(ImmutableMap.of("a1", "v1", "a2", "v2", "a3", "v3"));

    assertThat(scanWithIndex.getAttribute("a1")).hasValue("v1");
    assertThat(scanWithIndex.getAttribute("a2")).hasValue("v2");
    assertThat(scanWithIndex.getAttribute("a3")).hasValue("v3");
    assertThat(scanWithIndex.getAttributes())
        .isEqualTo(ImmutableMap.of("a1", "v1", "a2", "v2", "a3", "v3"));
  }
}
