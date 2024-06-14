package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.Selection.Conjunction;
import com.scalar.db.io.Key;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class GetBuilderTest {
  private static final String NAMESPACE_1 = "namespace1";
  private static final String NAMESPACE_2 = "namespace2";

  private static final String TABLE_1 = "table1";
  private static final String TABLE_2 = "table2";
  @Mock private Key partitionKey1;
  @Mock private Key partitionKey2;
  @Mock private Key clusteringKey1;
  @Mock private Key clusteringKey2;
  @Mock private Key indexKey1;
  @Mock private Key indexKey2;
  @Mock private ConditionalExpression condition;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  @Test
  public void buildGet_WithMandatoryParameters_ShouldBuildGetWithMandatoryParameters() {
    // Arrange Act
    Get actual = Get.newBuilder().table(TABLE_1).partitionKey(partitionKey1).build();

    // Assert
    assertThat(actual).isEqualTo(new Get(partitionKey1).forTable(TABLE_1));
  }

  @Test
  public void buildGet_WithClusteringKey_ShouldBuildGetWithClusteringKey() {
    // Arrange Act
    Get get =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .clusteringKey(clusteringKey1)
            .build();

    // Assert
    assertThat(get)
        .isEqualTo(
            new Get(partitionKey1, clusteringKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1));
  }

  @Test
  public void
      buildGet_WithAllParametersExceptForConjunctions_ShouldBuildGetWithAllParametersExceptForConjunctions() {
    // Arrange Act
    Get get =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .clusteringKey(clusteringKey1)
            .consistency(Consistency.EVENTUAL)
            .projection("c1")
            .projection("c2")
            .projections(Arrays.asList("c3", "c4"))
            .projections("c5", "c6")
            .build();

    // Assert
    assertThat(get)
        .isEqualTo(
            new Get(partitionKey1, clusteringKey1)
                .forNamespace(NAMESPACE_1)
                .forTable(TABLE_1)
                .withProjections(Arrays.asList("c1", "c2", "c3", "c4", "c5", "c6"))
                .withConsistency(Consistency.EVENTUAL));
  }

  @Test
  public void buildGet_WithConjunctiveNormalForm_ShouldBuildGetWithConditionsCorrectly() {
    // Arrange Act
    Get get1 =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .clusteringKey(clusteringKey1)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();
    Get get2 =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .clusteringKey(clusteringKey1)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();
    Get get3 =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .clusteringKey(clusteringKey1)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .consistency(Consistency.EVENTUAL)
            .build();
    Get get4 =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .clusteringKey(clusteringKey1)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .build();

    // Assert
    Get expected =
        new Get(partitionKey1, clusteringKey1)
            .forNamespace(NAMESPACE_1)
            .forTable(TABLE_1)
            .withConjunctions(
                ImmutableSet.of(
                    Conjunction.of(
                        ConditionBuilder.column("ck1").isGreaterThanInt(10),
                        ConditionBuilder.column("ck2").isGreaterThanInt(10),
                        ConditionBuilder.column("ck3").isGreaterThanInt(10),
                        ConditionBuilder.column("col1").isGreaterThanInt(10)),
                    Conjunction.of(
                        ConditionBuilder.column("ck1").isGreaterThanInt(10),
                        ConditionBuilder.column("ck2").isGreaterThanInt(10),
                        ConditionBuilder.column("ck4").isGreaterThanInt(10),
                        ConditionBuilder.column("col1").isGreaterThanInt(10))))
            .withProjections(Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"))
            .withConsistency(Consistency.EVENTUAL);

    assertThat(get1).isEqualTo(expected);
    assertThat(get2).isEqualTo(expected);
    assertThat(get3).isEqualTo(expected);
    assertThat(get4).isEqualTo(expected);
  }

  @Test
  public void buildGet_WithConditionAndConditionSet_ShouldBuildGetWithConditionsCorrectly() {
    // Arrange Act
    Get get =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();

    // Assert
    assertThat(get)
        .isEqualTo(
            new Get(partitionKey1)
                .forNamespace(NAMESPACE_1)
                .forTable(TABLE_1)
                .withConjunctions(
                    ImmutableSet.of(
                        Conjunction.of(
                            ConditionBuilder.column("ck1").isGreaterThanInt(10),
                            ConditionBuilder.column("ck3").isGreaterThanInt(10),
                            ConditionBuilder.column("col1").isGreaterThanInt(10)),
                        Conjunction.of(
                            ConditionBuilder.column("ck1").isGreaterThanInt(10),
                            ConditionBuilder.column("ck4").isGreaterThanInt(10),
                            ConditionBuilder.column("col1").isGreaterThanInt(10))))
                .withProjections(Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"))
                .withConsistency(Consistency.EVENTUAL));
  }

  @Test
  public void buildGet_WithDisjunctiveNormalForm_ShouldBuildGetWithConditionsCorrectly() {
    // Arrange Act
    Get get1 =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .or(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .or(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .or(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .clusteringKey(clusteringKey1)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();
    Get get2 =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .clusteringKey(clusteringKey1)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .or(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .or(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .or(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();
    Get get3 =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .clusteringKey(clusteringKey1)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .or(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .or(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .or(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .consistency(Consistency.EVENTUAL)
            .build();
    Get get4 =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .clusteringKey(clusteringKey1)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .or(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .or(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .or(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .build();

    // Assert
    Get expected =
        new Get(partitionKey1, clusteringKey1)
            .forNamespace(NAMESPACE_1)
            .forTable(TABLE_1)
            .withConjunctions(
                ImmutableSet.of(
                    Conjunction.of(ConditionBuilder.column("ck1").isGreaterThanInt(10)),
                    Conjunction.of(ConditionBuilder.column("ck2").isGreaterThanInt(10)),
                    Conjunction.of(
                        ConditionBuilder.column("ck3").isGreaterThanInt(10),
                        ConditionBuilder.column("ck4").isGreaterThanInt(10)),
                    Conjunction.of(ConditionBuilder.column("col1").isGreaterThanInt(10))))
            .withProjections(Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"))
            .withConsistency(Consistency.EVENTUAL);

    assertThat(get1).isEqualTo(expected);
    assertThat(get2).isEqualTo(expected);
    assertThat(get3).isEqualTo(expected);
    assertThat(get4).isEqualTo(expected);
  }

  @Test
  public void buildGet_FromExistingAndAddTwoOrConditions_ShouldBuildGetWithUpdatedParameters() {
    // Arrange
    Get get =
        Get.newBuilder().namespace(NAMESPACE_1).table(TABLE_1).partitionKey(partitionKey1).build();

    // Act
    Get newGet =
        Get.newBuilder(get)
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .or(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .build();

    // Assert
    assertThat(newGet)
        .isEqualTo(
            new Get(partitionKey1)
                .forNamespace(NAMESPACE_1)
                .forTable(TABLE_1)
                .withConjunctions(
                    ImmutableSet.of(
                        Conjunction.of(ConditionBuilder.column("ck1").isGreaterThanInt(10)),
                        Conjunction.of(ConditionBuilder.column("ck2").isGreaterThanInt(10))))
                .withConsistency(Consistency.SEQUENTIAL));
  }

  @Test
  public void buildGet_WithConditionOrConditionSet_ShouldBuildGetWithConditionsCorrectly() {
    // Arrange Act
    Get get =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .or(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .or(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();

    // Assert
    assertThat(get)
        .isEqualTo(
            new Get(partitionKey1)
                .forNamespace(NAMESPACE_1)
                .forTable(TABLE_1)
                .withConjunctions(
                    ImmutableSet.of(
                        Conjunction.of(ConditionBuilder.column("ck1").isGreaterThanInt(10)),
                        Conjunction.of(
                            ConditionBuilder.column("ck3").isGreaterThanInt(10),
                            ConditionBuilder.column("ck4").isGreaterThanInt(10)),
                        Conjunction.of(ConditionBuilder.column("col1").isGreaterThanInt(10))))
                .withProjections(Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"))
                .withConsistency(Consistency.EVENTUAL));
  }

  @Test
  public void buildGet_WithTwoOrConditionSet_ShouldBuildGetWithConditionsCorrectly() {
    // Arrange Act
    Get get =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .where(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .or(
                ConditionSetBuilder.condition(ConditionBuilder.column("col1").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("col2").isGreaterThanInt(10))
                    .build())
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();

    // Assert
    assertThat(get)
        .isEqualTo(
            new Get(partitionKey1)
                .forNamespace(NAMESPACE_1)
                .forTable(TABLE_1)
                .withConjunctions(
                    ImmutableSet.of(
                        Conjunction.of(
                            ConditionBuilder.column("ck3").isGreaterThanInt(10),
                            ConditionBuilder.column("ck4").isGreaterThanInt(10)),
                        Conjunction.of(
                            ConditionBuilder.column("col1").isGreaterThanInt(10),
                            ConditionBuilder.column("col2").isGreaterThanInt(10))))
                .withProjections(Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"))
                .withConsistency(Consistency.EVENTUAL));
  }

  @Test
  public void buildGet_WithOrConditionSets_ShouldBuildGetWithConditionsCorrectly() {
    // Arrange Act
    Get get =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .whereAnd(
                ImmutableSet.of(
                    ConditionSetBuilder.orConditionSet(
                            ImmutableSet.of(
                                ConditionBuilder.column("ck1").isGreaterThanInt(10),
                                ConditionBuilder.column("ck2").isGreaterThanInt(10)))
                        .build(),
                    ConditionSetBuilder.orConditionSet(
                            ImmutableSet.of(ConditionBuilder.column("ck3").isGreaterThanInt(10)))
                        .build()))
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();

    // Assert
    assertThat(get)
        .isEqualTo(
            new Get(partitionKey1)
                .forNamespace(NAMESPACE_1)
                .forTable(TABLE_1)
                .withConjunctions(
                    ImmutableSet.of(
                        Conjunction.of(
                            ConditionBuilder.column("ck1").isGreaterThanInt(10),
                            ConditionBuilder.column("ck3").isGreaterThanInt(10)),
                        Conjunction.of(
                            ConditionBuilder.column("ck2").isGreaterThanInt(10),
                            ConditionBuilder.column("ck3").isGreaterThanInt(10))))
                .withProjections(Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"))
                .withConsistency(Consistency.EVENTUAL));
  }

  @Test
  public void buildGet_WithAndConditionSets_ShouldBuildGetWithConditionsCorrectly() {
    // Arrange Act
    Get get =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .whereOr(
                ImmutableSet.of(
                    ConditionSetBuilder.andConditionSet(
                            ImmutableSet.of(
                                ConditionBuilder.column("ck1").isGreaterThanInt(10),
                                ConditionBuilder.column("ck2").isGreaterThanInt(10)))
                        .build(),
                    ConditionSetBuilder.andConditionSet(
                            ImmutableSet.of(ConditionBuilder.column("ck3").isGreaterThanInt(10)))
                        .build()))
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();

    // Assert
    assertThat(get)
        .isEqualTo(
            new Get(partitionKey1)
                .forNamespace(NAMESPACE_1)
                .forTable(TABLE_1)
                .withConjunctions(
                    ImmutableSet.of(
                        Conjunction.of(
                            ConditionBuilder.column("ck1").isGreaterThanInt(10),
                            ConditionBuilder.column("ck2").isGreaterThanInt(10)),
                        Conjunction.of(ConditionBuilder.column("ck3").isGreaterThanInt(10))))
                .withProjections(Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"))
                .withConsistency(Consistency.EVENTUAL));
  }

  @Test
  public void buildGet_WithEmptyOrConditionSet_ShouldBuildGetWithoutConjunctionCorrectly() {
    // Arrange Act
    Get get =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .where(ConditionSetBuilder.orConditionSet(ImmutableSet.of()).build())
            .and(ConditionSetBuilder.orConditionSet(ImmutableSet.of()).build())
            .build();

    // Assert
    assertThat(get).isEqualTo(new Get(partitionKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1));
  }

  @Test
  public void buildGet_WithEmptyAndConditionSet_ShouldBuildGetWithoutConjunctionCorrectly() {
    // Arrange Act
    Get get =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .where(ConditionSetBuilder.andConditionSet(ImmutableSet.of()).build())
            .or(ConditionSetBuilder.andConditionSet(ImmutableSet.of()).build())
            .build();

    // Assert
    assertThat(get).isEqualTo(new Get(partitionKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1));
  }

  @Test
  public void buildGet_WithEmptyOrConditionSets_ShouldBuildGetWithoutConjunctionCorrectly() {
    // Arrange Act
    Get get =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .whereAnd(ImmutableSet.of())
            .build();

    // Assert
    assertThat(get).isEqualTo(new Get(partitionKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1));
  }

  @Test
  public void buildGet_WithEmptyAndConditionSets_ShouldBuildGetWithoutConjunctionCorrectly() {
    // Arrange Act
    Get get =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .whereOr(ImmutableSet.of())
            .build();

    // Assert
    assertThat(get).isEqualTo(new Get(partitionKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1));
  }

  @Test
  public void buildGet_FromExistingWithoutChange_ShouldCopy() {
    // Arrange
    Get existingGet =
        new Get(partitionKey1, clusteringKey1)
            .forNamespace(NAMESPACE_1)
            .forTable(TABLE_1)
            .withProjections(Arrays.asList("c1", "c2"))
            .withConsistency(Consistency.LINEARIZABLE);

    // Act
    Get newGet = Get.newBuilder(existingGet).build();

    // Assert
    assertThat(newGet).isEqualTo(existingGet);
  }

  @Test
  public void
      buildGet_FromExistingAndUpdateAllParametersExceptConjunctions_ShouldBuildGetWithUpdatedParameters() {
    // Arrange
    Get existingGet =
        new Get(partitionKey1, clusteringKey1)
            .forNamespace(NAMESPACE_1)
            .forTable(TABLE_1)
            .withProjections(Arrays.asList("c1", "c2"))
            .withConsistency(Consistency.LINEARIZABLE)
            .withConjunctions(
                ImmutableSet.of(
                    Conjunction.of(
                        ConditionBuilder.column("ck3").isGreaterThanInt(10),
                        ConditionBuilder.column("col1").isGreaterThanInt(10)),
                    Conjunction.of(
                        ConditionBuilder.column("ck4").isGreaterThanInt(10),
                        ConditionBuilder.column("col1").isGreaterThanInt(10))));

    // Act
    Get newGet =
        Get.newBuilder(existingGet)
            .partitionKey(partitionKey2)
            .clusteringKey(clusteringKey2)
            .namespace(NAMESPACE_2)
            .table(TABLE_2)
            .consistency(Consistency.EVENTUAL)
            .clearProjections()
            .projections(Arrays.asList("c3", "c4"))
            .projection("c5")
            .projections("c6", "c7")
            .build();

    // Assert
    assertThat(newGet)
        .isEqualTo(
            new Get(partitionKey2, clusteringKey2)
                .forNamespace(NAMESPACE_2)
                .forTable(TABLE_2)
                .withConsistency(Consistency.EVENTUAL)
                .withProjections(Arrays.asList("c3", "c4", "c5", "c6", "c7"))
                .withConjunctions(
                    ImmutableSet.of(
                        Conjunction.of(
                            ConditionBuilder.column("ck3").isGreaterThanInt(10),
                            ConditionBuilder.column("col1").isGreaterThanInt(10)),
                        Conjunction.of(
                            ConditionBuilder.column("ck4").isGreaterThanInt(10),
                            ConditionBuilder.column("col1").isGreaterThanInt(10)))));
  }

  @Test
  public void
      buildGet_FromExistingWithConditionsAndUpdateAllParameters_ShouldBuildGetWithUpdatedParameters() {
    // Arrange
    Get get =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .clusteringKey(clusteringKey1)
            .where(ConditionBuilder.column("pk1").isGreaterThanInt(10))
            .projection("pk1")
            .consistency(Consistency.EVENTUAL)
            .build();
    Get expected =
        new Get(partitionKey2, clusteringKey2)
            .forNamespace(NAMESPACE_2)
            .forTable(TABLE_2)
            .withConjunctions(
                ImmutableSet.of(
                    Conjunction.of(
                        ConditionBuilder.column("ck1").isGreaterThanInt(10),
                        ConditionBuilder.column("ck2").isGreaterThanInt(10),
                        ConditionBuilder.column("ck3").isGreaterThanInt(10),
                        ConditionBuilder.column("col1").isGreaterThanInt(10)),
                    Conjunction.of(
                        ConditionBuilder.column("ck1").isGreaterThanInt(10),
                        ConditionBuilder.column("ck2").isGreaterThanInt(10),
                        ConditionBuilder.column("ck4").isGreaterThanInt(10),
                        ConditionBuilder.column("col1").isGreaterThanInt(10))))
            .withProjections(Arrays.asList("ck1", "ck2", "ck3", "ck4", "ck5"))
            .withConsistency(Consistency.LINEARIZABLE);

    // Act
    Get newGet1 =
        Get.newBuilder(get)
            .namespace(NAMESPACE_2)
            .table(TABLE_2)
            .clearConditions()
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .partitionKey(partitionKey2)
            .clusteringKey(clusteringKey2)
            .clearProjections()
            .projections(Arrays.asList("ck1", "ck2"))
            .projection("ck3")
            .projections("ck4", "ck5")
            .consistency(Consistency.LINEARIZABLE)
            .build();
    Get newGet2 =
        Get.newBuilder(get)
            .namespace(NAMESPACE_2)
            .table(TABLE_2)
            .clearConditions()
            .partitionKey(partitionKey2)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .clusteringKey(clusteringKey2)
            .clearProjections()
            .projections(Arrays.asList("ck1", "ck2"))
            .projection("ck3")
            .projections("ck4", "ck5")
            .consistency(Consistency.LINEARIZABLE)
            .build();
    Get newGet3 =
        Get.newBuilder(get)
            .namespace(NAMESPACE_2)
            .table(TABLE_2)
            .clearConditions()
            .partitionKey(partitionKey2)
            .clusteringKey(clusteringKey2)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .clearProjections()
            .projections(Arrays.asList("ck1", "ck2"))
            .projection("ck3")
            .projections("ck4", "ck5")
            .consistency(Consistency.LINEARIZABLE)
            .build();
    Get newGet4 =
        Get.newBuilder(get)
            .namespace(NAMESPACE_2)
            .table(TABLE_2)
            .clearConditions()
            .partitionKey(partitionKey2)
            .clusteringKey(clusteringKey2)
            .clearProjections()
            .projections(Arrays.asList("ck1", "ck2"))
            .projection("ck3")
            .projections("ck4", "ck5")
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .consistency(Consistency.LINEARIZABLE)
            .build();
    Get newGet5 =
        Get.newBuilder(get)
            .namespace(NAMESPACE_2)
            .table(TABLE_2)
            .clearConditions()
            .partitionKey(partitionKey2)
            .clusteringKey(clusteringKey2)
            .clearProjections()
            .projections(Arrays.asList("ck1", "ck2"))
            .projection("ck3")
            .projections("ck4", "ck5")
            .consistency(Consistency.LINEARIZABLE)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .build();

    // Assert
    assertThat(newGet1).isEqualTo(expected);
    assertThat(newGet2).isEqualTo(expected);
    assertThat(newGet3).isEqualTo(expected);
    assertThat(newGet4).isEqualTo(expected);
    assertThat(newGet5).isEqualTo(expected);
  }

  @Test
  public void
      buildGet_FromExistingAndAddConditionAndConditionSet_ShouldBuildGetWithUpdatedParameters() {
    // Arrange
    Get get =
        Get.newBuilder().namespace(NAMESPACE_1).table(TABLE_1).partitionKey(partitionKey1).build();

    // Act
    Get newGet =
        Get.newBuilder(get)
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .build();

    // Assert
    assertThat(newGet)
        .isEqualTo(
            new Get(partitionKey1)
                .forNamespace(NAMESPACE_1)
                .forTable(TABLE_1)
                .withConjunctions(
                    ImmutableSet.of(
                        Conjunction.of(
                            ConditionBuilder.column("ck1").isGreaterThanInt(10),
                            ConditionBuilder.column("ck3").isGreaterThanInt(10),
                            ConditionBuilder.column("col1").isGreaterThanInt(10)),
                        Conjunction.of(
                            ConditionBuilder.column("ck1").isGreaterThanInt(10),
                            ConditionBuilder.column("ck4").isGreaterThanInt(10),
                            ConditionBuilder.column("col1").isGreaterThanInt(10))))
                .withConsistency(Consistency.SEQUENTIAL));
  }

  @Test
  public void
      buildGet_FromExistingAndAddConditionOrConditionSet_ShouldBuildGetWithUpdatedParameters() {
    // Arrange
    Get get =
        Get.newBuilder().namespace(NAMESPACE_1).table(TABLE_1).partitionKey(partitionKey1).build();

    // Act
    Get newGet =
        Get.newBuilder(get)
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .or(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .or(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .build();

    // Assert
    assertThat(newGet)
        .isEqualTo(
            new Get(partitionKey1)
                .forNamespace(NAMESPACE_1)
                .forTable(TABLE_1)
                .withConjunctions(
                    ImmutableSet.of(
                        Conjunction.of(ConditionBuilder.column("ck1").isGreaterThanInt(10)),
                        Conjunction.of(
                            ConditionBuilder.column("ck3").isGreaterThanInt(10),
                            ConditionBuilder.column("ck4").isGreaterThanInt(10)),
                        Conjunction.of(ConditionBuilder.column("col1").isGreaterThanInt(10))))
                .withConsistency(Consistency.SEQUENTIAL));
  }

  @Test
  public void buildGet_FromExistingAndAddTwoAndConditionSet_ShouldBuildGetWithUpdatedParameters() {
    // Arrange
    Get get =
        Get.newBuilder().namespace(NAMESPACE_1).table(TABLE_1).partitionKey(partitionKey1).build();

    // Act
    Get newGet =
        Get.newBuilder(get)
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .where(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("col1").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("col2").isGreaterThanInt(10))
                    .build())
            .build();

    // Assert
    assertThat(newGet)
        .isEqualTo(
            new Get(partitionKey1)
                .forNamespace(NAMESPACE_1)
                .forTable(TABLE_1)
                .withConjunctions(
                    ImmutableSet.of(
                        Conjunction.of(
                            ConditionBuilder.column("ck3").isGreaterThanInt(10),
                            ConditionBuilder.column("col1").isGreaterThanInt(10)),
                        Conjunction.of(
                            ConditionBuilder.column("ck4").isGreaterThanInt(10),
                            ConditionBuilder.column("col1").isGreaterThanInt(10)),
                        Conjunction.of(
                            ConditionBuilder.column("ck3").isGreaterThanInt(10),
                            ConditionBuilder.column("col2").isGreaterThanInt(10)),
                        Conjunction.of(
                            ConditionBuilder.column("ck4").isGreaterThanInt(10),
                            ConditionBuilder.column("col2").isGreaterThanInt(10))))
                .withConsistency(Consistency.SEQUENTIAL));
  }

  @Test
  public void buildGet_FromExistingAndAddTwoOrConditionSet_ShouldBuildGetWithUpdatedParameters() {
    // Arrange
    Get get =
        Get.newBuilder().namespace(NAMESPACE_1).table(TABLE_1).partitionKey(partitionKey1).build();

    // Act
    Get newGet =
        Get.newBuilder(get)
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .where(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .or(
                ConditionSetBuilder.condition(ConditionBuilder.column("col1").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("col2").isGreaterThanInt(10))
                    .build())
            .build();

    // Assert
    assertThat(newGet)
        .isEqualTo(
            new Get(partitionKey1)
                .forNamespace(NAMESPACE_1)
                .forTable(TABLE_1)
                .withConjunctions(
                    ImmutableSet.of(
                        Conjunction.of(
                            ConditionBuilder.column("ck3").isGreaterThanInt(10),
                            ConditionBuilder.column("ck4").isGreaterThanInt(10)),
                        Conjunction.of(
                            ConditionBuilder.column("col1").isGreaterThanInt(10),
                            ConditionBuilder.column("col2").isGreaterThanInt(10))))
                .withConsistency(Consistency.SEQUENTIAL));
  }

  @Test
  public void buildGet_FromExistingWithOrConditionSets_ShouldBuildGetWithUpdatedParameters() {
    // Arrange
    Get get =
        Get.newBuilder().namespace(NAMESPACE_1).table(TABLE_1).partitionKey(partitionKey1).build();

    // Act
    Get newGet =
        Get.newBuilder(get)
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .whereAnd(
                ImmutableSet.of(
                    ConditionSetBuilder.orConditionSet(
                            ImmutableSet.of(
                                ConditionBuilder.column("ck1").isGreaterThanInt(10),
                                ConditionBuilder.column("ck2").isGreaterThanInt(10)))
                        .build(),
                    ConditionSetBuilder.orConditionSet(
                            ImmutableSet.of(ConditionBuilder.column("ck3").isGreaterThanInt(10)))
                        .build()))
            .build();

    // Assert
    assertThat(newGet)
        .isEqualTo(
            new Get(partitionKey1)
                .forNamespace(NAMESPACE_1)
                .forTable(TABLE_1)
                .withConjunctions(
                    ImmutableSet.of(
                        Conjunction.of(
                            ConditionBuilder.column("ck1").isGreaterThanInt(10),
                            ConditionBuilder.column("ck3").isGreaterThanInt(10)),
                        Conjunction.of(
                            ConditionBuilder.column("ck2").isGreaterThanInt(10),
                            ConditionBuilder.column("ck3").isGreaterThanInt(10))))
                .withConsistency(Consistency.SEQUENTIAL));
  }

  @Test
  public void buildGet_FromExistingWithAndConditionSets_ShouldBuildGetWithUpdatedParameters() {
    // Arrange
    Get get =
        Get.newBuilder().namespace(NAMESPACE_1).table(TABLE_1).partitionKey(partitionKey1).build();

    // Act
    Get newGet =
        Get.newBuilder(get)
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .whereOr(
                ImmutableSet.of(
                    ConditionSetBuilder.andConditionSet(
                            ImmutableSet.of(
                                ConditionBuilder.column("ck1").isGreaterThanInt(10),
                                ConditionBuilder.column("ck2").isGreaterThanInt(10)))
                        .build(),
                    ConditionSetBuilder.andConditionSet(
                            ImmutableSet.of(ConditionBuilder.column("ck3").isGreaterThanInt(10)))
                        .build()))
            .build();

    // Assert
    assertThat(newGet)
        .isEqualTo(
            new Get(partitionKey1)
                .forNamespace(NAMESPACE_1)
                .forTable(TABLE_1)
                .withConjunctions(
                    ImmutableSet.of(
                        Conjunction.of(
                            ConditionBuilder.column("ck1").isGreaterThanInt(10),
                            ConditionBuilder.column("ck2").isGreaterThanInt(10)),
                        Conjunction.of(ConditionBuilder.column("ck3").isGreaterThanInt(10))))
                .withConsistency(Consistency.SEQUENTIAL));
  }

  @Test
  public void
      buildGet_FromExistingAndUpdateNamespaceAndTableAfterWhere_ShouldBuildGetWithNewNamespaceAndTable() {
    // Arrange
    Get get =
        Get.newBuilder().namespace(NAMESPACE_1).table(TABLE_1).partitionKey(partitionKey1).build();

    // Act
    Get newGet =
        Get.newBuilder(get)
            .clearConditions()
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .namespace(NAMESPACE_2)
            .table(TABLE_2)
            .build();

    // Assert
    assertThat(newGet)
        .isEqualTo(
            new Get(partitionKey1)
                .forNamespace(NAMESPACE_2)
                .forTable(TABLE_2)
                .withConjunctions(
                    ImmutableSet.of(
                        Conjunction.of(ConditionBuilder.column("ck1").isGreaterThanInt(10)))));
  }

  @Test
  public void buildGet_FromExistingAndClearNamespaceAfterWhere_ShouldBuildGetWithoutNamespace() {
    // Arrange
    Get get =
        Get.newBuilder().namespace(NAMESPACE_1).table(TABLE_1).partitionKey(partitionKey1).build();

    // Act
    Get newGet =
        Get.newBuilder(get)
            .clearConditions()
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .clearNamespace()
            .build();

    // Assert
    assertThat(newGet)
        .isEqualTo(
            new Get(partitionKey1)
                .forTable(TABLE_1)
                .withConjunctions(
                    ImmutableSet.of(
                        Conjunction.of(ConditionBuilder.column("ck1").isGreaterThanInt(10)))));
  }

  @Test
  public void buildGet_FromExistingAndClearClusteringKey_ShouldBuildGetWithoutClusteringKey() {
    // Arrange
    Get existingGet =
        new Get(partitionKey1, clusteringKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1);

    // Act
    Get newGet = Get.newBuilder(existingGet).clearClusteringKey().build();

    // Assert
    assertThat(newGet)
        .isEqualTo(new Get(partitionKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1));
  }

  @Test
  public void
      buildGet_FromExistingWithUnsupportedOperation_ShouldThrowUnsupportedOperationException() {
    // Arrange
    Get existingGet =
        new Get(partitionKey1, clusteringKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1);

    // Act Assert
    assertThatThrownBy(() -> Get.newBuilder(existingGet).indexKey(indexKey1))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void
      buildGet_FromExistingWithConditionAndCallWhereBeforeClearingCondition_ShouldThrowUnsupportedOperationException() {
    // Arrange
    Get existingGet =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .where(ConditionBuilder.column("pk1").isGreaterThanInt(10))
            .build();

    // Act Assert
    assertThatThrownBy(() -> Get.newBuilder(existingGet).where(condition))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void buildGetWithIndex_WithMandatoryParameters_ShouldBuildGetWithMandatoryParameters() {
    // Arrange Act
    Get actual = Get.newBuilder().table(TABLE_1).indexKey(indexKey1).build();

    // Assert
    assertThat(actual).isEqualTo(new GetWithIndex(indexKey1).forTable(TABLE_1));
  }

  @Test
  public void
      buildGetWithIndex_WithAllParametersExceptForConjunctions_ShouldBuildGetWithAllParametersExceptForConjunctions() {
    // Arrange Act
    Get get =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .consistency(Consistency.EVENTUAL)
            .projection("c1")
            .projection("c2")
            .projections(Arrays.asList("c3", "c4"))
            .projections("c5", "c6")
            .build();

    // Assert
    assertThat(get)
        .isEqualTo(
            new GetWithIndex(indexKey1)
                .forNamespace(NAMESPACE_1)
                .forTable(TABLE_1)
                .withProjections(Arrays.asList("c1", "c2", "c3", "c4", "c5", "c6"))
                .withConsistency(Consistency.EVENTUAL));
  }

  @Test
  public void buildGetWithIndex_WithConjunctiveNormalForm_ShouldBuildGetWithConditionsCorrectly() {
    // Arrange Act
    Get get1 =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();
    Get get2 =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .consistency(Consistency.EVENTUAL)
            .build();
    Get get3 =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .build();

    // Assert
    Get expected =
        new GetWithIndex(indexKey1)
            .forNamespace(NAMESPACE_1)
            .forTable(TABLE_1)
            .withConjunctions(
                ImmutableSet.of(
                    Conjunction.of(
                        ConditionBuilder.column("ck1").isGreaterThanInt(10),
                        ConditionBuilder.column("ck2").isGreaterThanInt(10),
                        ConditionBuilder.column("ck3").isGreaterThanInt(10),
                        ConditionBuilder.column("col1").isGreaterThanInt(10)),
                    Conjunction.of(
                        ConditionBuilder.column("ck1").isGreaterThanInt(10),
                        ConditionBuilder.column("ck2").isGreaterThanInt(10),
                        ConditionBuilder.column("ck4").isGreaterThanInt(10),
                        ConditionBuilder.column("col1").isGreaterThanInt(10))))
            .withProjections(Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"))
            .withConsistency(Consistency.EVENTUAL);

    assertThat(get1).isEqualTo(expected);
    assertThat(get2).isEqualTo(expected);
    assertThat(get3).isEqualTo(expected);
  }

  @Test
  public void
      buildGetWithIndex_WithConditionAndConditionSet_ShouldBuildGetWithConditionsCorrectly() {
    // Arrange Act
    Get get =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();

    // Assert
    assertThat(get)
        .isEqualTo(
            new GetWithIndex(indexKey1)
                .forNamespace(NAMESPACE_1)
                .forTable(TABLE_1)
                .withConjunctions(
                    ImmutableSet.of(
                        Conjunction.of(
                            ConditionBuilder.column("ck1").isGreaterThanInt(10),
                            ConditionBuilder.column("ck3").isGreaterThanInt(10),
                            ConditionBuilder.column("col1").isGreaterThanInt(10)),
                        Conjunction.of(
                            ConditionBuilder.column("ck1").isGreaterThanInt(10),
                            ConditionBuilder.column("ck4").isGreaterThanInt(10),
                            ConditionBuilder.column("col1").isGreaterThanInt(10))))
                .withProjections(Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"))
                .withConsistency(Consistency.EVENTUAL));
  }

  @Test
  public void buildGetWithIndex_WithDisjunctiveNormalForm_ShouldBuildGetWithConditionsCorrectly() {
    // Arrange Act
    Get get1 =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .or(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .or(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .or(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();
    Get get2 =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .or(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .or(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .or(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .consistency(Consistency.EVENTUAL)
            .build();
    Get get3 =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .or(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .or(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .or(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .build();

    // Assert
    Get expected =
        new GetWithIndex(indexKey1)
            .forNamespace(NAMESPACE_1)
            .forTable(TABLE_1)
            .withConjunctions(
                ImmutableSet.of(
                    Conjunction.of(ConditionBuilder.column("ck1").isGreaterThanInt(10)),
                    Conjunction.of(ConditionBuilder.column("ck2").isGreaterThanInt(10)),
                    Conjunction.of(
                        ConditionBuilder.column("ck3").isGreaterThanInt(10),
                        ConditionBuilder.column("ck4").isGreaterThanInt(10)),
                    Conjunction.of(ConditionBuilder.column("col1").isGreaterThanInt(10))))
            .withProjections(Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"))
            .withConsistency(Consistency.EVENTUAL);

    assertThat(get1).isEqualTo(expected);
    assertThat(get2).isEqualTo(expected);
    assertThat(get3).isEqualTo(expected);
  }

  @Test
  public void
      buildGetWithIndex_WithConditionOrConditionSet_ShouldBuildGetWithConditionsCorrectly() {
    // Arrange Act
    Get get =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .or(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .or(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();

    // Assert
    assertThat(get)
        .isEqualTo(
            new GetWithIndex(indexKey1)
                .forNamespace(NAMESPACE_1)
                .forTable(TABLE_1)
                .withConjunctions(
                    ImmutableSet.of(
                        Conjunction.of(ConditionBuilder.column("ck1").isGreaterThanInt(10)),
                        Conjunction.of(
                            ConditionBuilder.column("ck3").isGreaterThanInt(10),
                            ConditionBuilder.column("ck4").isGreaterThanInt(10)),
                        Conjunction.of(ConditionBuilder.column("col1").isGreaterThanInt(10))))
                .withProjections(Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"))
                .withConsistency(Consistency.EVENTUAL));
  }

  @Test
  public void buildGetWithIndex_WithTwoOrConditionSet_ShouldBuildGetWithConditionsCorrectly() {
    // Arrange Act
    Get get =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .where(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .or(
                ConditionSetBuilder.condition(ConditionBuilder.column("col1").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("col2").isGreaterThanInt(10))
                    .build())
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();

    // Assert
    assertThat(get)
        .isEqualTo(
            new GetWithIndex(indexKey1)
                .forNamespace(NAMESPACE_1)
                .forTable(TABLE_1)
                .withConjunctions(
                    ImmutableSet.of(
                        Conjunction.of(
                            ConditionBuilder.column("ck3").isGreaterThanInt(10),
                            ConditionBuilder.column("ck4").isGreaterThanInt(10)),
                        Conjunction.of(
                            ConditionBuilder.column("col1").isGreaterThanInt(10),
                            ConditionBuilder.column("col2").isGreaterThanInt(10))))
                .withProjections(Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"))
                .withConsistency(Consistency.EVENTUAL));
  }

  @Test
  public void buildGetWithIndex_WithOrConditionSets_ShouldBuildGetWithConditionsCorrectly() {
    // Arrange Act
    Get get =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .whereAnd(
                ImmutableSet.of(
                    ConditionSetBuilder.orConditionSet(
                            ImmutableSet.of(
                                ConditionBuilder.column("ck1").isGreaterThanInt(10),
                                ConditionBuilder.column("ck2").isGreaterThanInt(10)))
                        .build(),
                    ConditionSetBuilder.orConditionSet(
                            ImmutableSet.of(ConditionBuilder.column("ck3").isGreaterThanInt(10)))
                        .build()))
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();

    // Assert
    assertThat(get)
        .isEqualTo(
            new GetWithIndex(indexKey1)
                .forNamespace(NAMESPACE_1)
                .forTable(TABLE_1)
                .withConjunctions(
                    ImmutableSet.of(
                        Conjunction.of(
                            ConditionBuilder.column("ck1").isGreaterThanInt(10),
                            ConditionBuilder.column("ck3").isGreaterThanInt(10)),
                        Conjunction.of(
                            ConditionBuilder.column("ck2").isGreaterThanInt(10),
                            ConditionBuilder.column("ck3").isGreaterThanInt(10))))
                .withProjections(Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"))
                .withConsistency(Consistency.EVENTUAL));
  }

  @Test
  public void buildGetWithIndex_WithAndConditionSets_ShouldBuildGetWithConditionsCorrectly() {
    // Arrange Act
    Get get =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .whereOr(
                ImmutableSet.of(
                    ConditionSetBuilder.andConditionSet(
                            ImmutableSet.of(
                                ConditionBuilder.column("ck1").isGreaterThanInt(10),
                                ConditionBuilder.column("ck2").isGreaterThanInt(10)))
                        .build(),
                    ConditionSetBuilder.andConditionSet(
                            ImmutableSet.of(ConditionBuilder.column("ck3").isGreaterThanInt(10)))
                        .build()))
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();

    // Assert
    assertThat(get)
        .isEqualTo(
            new GetWithIndex(indexKey1)
                .forNamespace(NAMESPACE_1)
                .forTable(TABLE_1)
                .withConjunctions(
                    ImmutableSet.of(
                        Conjunction.of(
                            ConditionBuilder.column("ck1").isGreaterThanInt(10),
                            ConditionBuilder.column("ck2").isGreaterThanInt(10)),
                        Conjunction.of(ConditionBuilder.column("ck3").isGreaterThanInt(10))))
                .withProjections(Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"))
                .withConsistency(Consistency.EVENTUAL));
  }

  @Test
  public void
      buildGetWithIndex_WithEmptyOrConditionSet_ShouldBuildGetWithoutConjunctionCorrectly() {
    // Arrange Act
    Get get =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .where(ConditionSetBuilder.orConditionSet(ImmutableSet.of()).build())
            .and(ConditionSetBuilder.orConditionSet(ImmutableSet.of()).build())
            .build();

    // Assert
    assertThat(get)
        .isEqualTo(new GetWithIndex(indexKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1));
  }

  @Test
  public void
      buildGetWithIndex_WithEmptyAndConditionSet_ShouldBuildGetWithoutConjunctionCorrectly() {
    // Arrange Act
    Get get =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .where(ConditionSetBuilder.andConditionSet(ImmutableSet.of()).build())
            .or(ConditionSetBuilder.andConditionSet(ImmutableSet.of()).build())
            .build();

    // Assert
    assertThat(get)
        .isEqualTo(new GetWithIndex(indexKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1));
  }

  @Test
  public void
      buildGetWithIndex_WithEmptyOrConditionSets_ShouldBuildGetWithoutConjunctionCorrectly() {
    // Arrange Act
    Get get =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .whereAnd(ImmutableSet.of())
            .build();

    // Assert
    assertThat(get)
        .isEqualTo(new GetWithIndex(indexKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1));
  }

  @Test
  public void
      buildGetWithIndex_WithEmptyAndConditionSets_ShouldBuildGetWithoutConjunctionCorrectly() {
    // Arrange Act
    Get get =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .whereOr(ImmutableSet.of())
            .build();

    // Assert
    assertThat(get)
        .isEqualTo(new GetWithIndex(indexKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1));
  }

  @Test
  public void buildGetWithIndex_FromExistingWithoutChange_ShouldCopy() {
    // Arrange
    GetWithIndex existingGet =
        new GetWithIndex(indexKey1)
            .forNamespace(NAMESPACE_1)
            .forTable(TABLE_1)
            .withProjections(Arrays.asList("c1", "c2"))
            .withConsistency(Consistency.LINEARIZABLE);

    // Act
    Get newGet = Get.newBuilder(existingGet).build();

    // Assert
    assertThat(newGet).isEqualTo(existingGet);
  }

  @Test
  public void
      buildGetWithIndex_FromExistingAndUpdateAllParameters_ShouldBuildGetWithUpdatedParameters() {
    // Arrange
    GetWithIndex existingGet =
        new GetWithIndex(indexKey1)
            .forNamespace(NAMESPACE_1)
            .forTable(TABLE_1)
            .withProjections(Arrays.asList("c1", "c2"))
            .withConsistency(Consistency.LINEARIZABLE);

    // Act
    Get newGet =
        Get.newBuilder(existingGet)
            .indexKey(indexKey2)
            .namespace(NAMESPACE_2)
            .table(TABLE_2)
            .consistency(Consistency.EVENTUAL)
            .clearProjections()
            .projections(Arrays.asList("c3", "c4"))
            .projection("c5")
            .projections("c6", "c7")
            .build();

    // Assert
    assertThat(newGet)
        .isEqualTo(
            new GetWithIndex(indexKey2)
                .forNamespace(NAMESPACE_2)
                .forTable(TABLE_2)
                .withConsistency(Consistency.EVENTUAL)
                .withProjections(Arrays.asList("c3", "c4", "c5", "c6", "c7")));
  }

  @Test
  public void
      buildGetWithIndex_FromExistingWithConditionsAndUpdateAllParameters_ShouldBuildGetWithUpdatedParameters() {
    // Arrange
    Get get =
        Get.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .where(ConditionBuilder.column("pk1").isGreaterThanInt(10))
            .projection("pk1")
            .consistency(Consistency.EVENTUAL)
            .build();
    Get expected =
        new GetWithIndex(indexKey2)
            .forNamespace(NAMESPACE_2)
            .forTable(TABLE_2)
            .withConjunctions(
                ImmutableSet.of(
                    Conjunction.of(
                        ConditionBuilder.column("ck1").isGreaterThanInt(10),
                        ConditionBuilder.column("ck2").isGreaterThanInt(10),
                        ConditionBuilder.column("ck3").isGreaterThanInt(10),
                        ConditionBuilder.column("col1").isGreaterThanInt(10)),
                    Conjunction.of(
                        ConditionBuilder.column("ck1").isGreaterThanInt(10),
                        ConditionBuilder.column("ck2").isGreaterThanInt(10),
                        ConditionBuilder.column("ck4").isGreaterThanInt(10),
                        ConditionBuilder.column("col1").isGreaterThanInt(10))))
            .withProjections(Arrays.asList("ck1", "ck2", "ck3", "ck4", "ck5"))
            .withConsistency(Consistency.LINEARIZABLE);

    // Act
    Get newGet1 =
        Get.newBuilder(get)
            .namespace(NAMESPACE_2)
            .table(TABLE_2)
            .clearConditions()
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .indexKey(indexKey2)
            .clearProjections()
            .projections(Arrays.asList("ck1", "ck2"))
            .projection("ck3")
            .projections("ck4", "ck5")
            .consistency(Consistency.LINEARIZABLE)
            .build();
    Get newGet2 =
        Get.newBuilder(get)
            .namespace(NAMESPACE_2)
            .table(TABLE_2)
            .indexKey(indexKey2)
            .clearProjections()
            .projections(Arrays.asList("ck1", "ck2"))
            .projection("ck3")
            .projections("ck4", "ck5")
            .clearConditions()
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .consistency(Consistency.LINEARIZABLE)
            .build();
    Get newGet3 =
        Get.newBuilder(get)
            .namespace(NAMESPACE_2)
            .table(TABLE_2)
            .indexKey(indexKey2)
            .clearProjections()
            .projections(Arrays.asList("ck1", "ck2"))
            .projection("ck3")
            .projections("ck4", "ck5")
            .consistency(Consistency.LINEARIZABLE)
            .clearConditions()
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .build();

    // Assert
    assertThat(newGet1).isEqualTo(expected);
    assertThat(newGet2).isEqualTo(expected);
    assertThat(newGet3).isEqualTo(expected);
  }

  @Test
  public void
      buildGetWithIndex_FromExistingWithUnsupportedOperation_ShouldThrowUnsupportedOperationException() {
    // Arrange
    GetWithIndex existingGet =
        new GetWithIndex(indexKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1);

    // Act Assert
    assertThatThrownBy(() -> Get.newBuilder(existingGet).partitionKey(partitionKey1))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> Get.newBuilder(existingGet).clusteringKey(clusteringKey1))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> Get.newBuilder(existingGet).clearClusteringKey())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void buildGet_FromExistingAndClearNamespace_ShouldBuildGetWithoutNamespace() {
    // Arrange
    Get existingGet =
        new Get(indexKey1)
            .forNamespace(NAMESPACE_1)
            .forTable(TABLE_1)
            .withProjections(Arrays.asList("c1", "c2"))
            .withConsistency(Consistency.LINEARIZABLE);

    // Act
    Get newGet = Get.newBuilder(existingGet).clearNamespace().build();

    // Assert
    assertThat(newGet)
        .isEqualTo(
            new Get(indexKey1)
                .forTable(TABLE_1)
                .withProjections(Arrays.asList("c1", "c2"))
                .withConsistency(Consistency.LINEARIZABLE));
  }

  @Test
  public void buildGetWithIndex_FromExistingAndClearNamespace_ShouldBuildGetWithoutNamespace() {
    // Arrange
    GetWithIndex existingGet =
        new GetWithIndex(indexKey1)
            .forNamespace(NAMESPACE_1)
            .forTable(TABLE_1)
            .withProjections(Arrays.asList("c1", "c2"))
            .withConsistency(Consistency.LINEARIZABLE);

    // Act
    Get newGet = Get.newBuilder(existingGet).clearNamespace().build();

    // Assert
    assertThat(newGet)
        .isEqualTo(
            new GetWithIndex(indexKey1)
                .forTable(TABLE_1)
                .withProjections(Arrays.asList("c1", "c2"))
                .withConsistency(Consistency.LINEARIZABLE));
  }
}
