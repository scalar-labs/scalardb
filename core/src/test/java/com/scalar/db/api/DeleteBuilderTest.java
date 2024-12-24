package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.io.Key;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class DeleteBuilderTest {
  private static final String NAMESPACE_1 = "namespace1";
  private static final String NAMESPACE_2 = "namespace2";

  private static final String TABLE_1 = "table1";
  private static final String TABLE_2 = "table2";
  @Mock private Key partitionKey1;
  @Mock private Key partitionKey2;
  @Mock private Key clusteringKey1;
  @Mock private Key clusteringKey2;
  @Mock private MutationCondition condition1;
  @Mock private MutationCondition condition2;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  @Test
  public void build_WithMandatoryParameters_ShouldBuildDeleteWithMandatoryParameters() {
    // Arrange Act
    Delete actual = Delete.newBuilder().table(TABLE_1).partitionKey(partitionKey1).build();

    // Assert
    assertThat(actual).isEqualTo(new Delete(partitionKey1).forTable(TABLE_1));
  }

  @Test
  public void build_WithClusteringKey_ShouldBuildDeleteWithClusteringKey() {
    // Arrange Act
    Delete delete =
        Delete.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .clusteringKey(clusteringKey1)
            .build();

    // Assert
    assertThat(delete)
        .isEqualTo(
            new Delete(partitionKey1, clusteringKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1));
  }

  @Test
  public void build_WithAllParameters_ShouldBuildDeleteWithAllParameters() {
    // Arrange Act
    Delete delete =
        Delete.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .clusteringKey(clusteringKey1)
            .consistency(Consistency.EVENTUAL)
            .condition(condition1)
            .attribute("a1", "v1")
            .attributes(ImmutableMap.of("a2", "v2", "a3", "v3"))
            .readTag("policyName1", "readTag")
            .writeTag("policyName2", "writeTag")
            .build();

    // Assert
    assertThat(delete)
        .isEqualTo(
            new Delete(
                NAMESPACE_1,
                TABLE_1,
                partitionKey1,
                clusteringKey1,
                Consistency.EVENTUAL,
                ImmutableMap.of(
                    "a1",
                    "v1",
                    "a2",
                    "v2",
                    "a3",
                    "v3",
                    AbacOperationAttributes.READ_TAG_PREFIX + "policyName1",
                    "readTag",
                    AbacOperationAttributes.WRITE_TAG_PREFIX + "policyName2",
                    "writeTag"),
                condition1));
  }

  @Test
  public void build_FromExistingWithoutChange_ShouldCopy() {
    // Arrange
    Delete existingDelete =
        new Delete(partitionKey1, clusteringKey1)
            .forNamespace(NAMESPACE_1)
            .forTable(TABLE_1)
            .withCondition(condition1)
            .withConsistency(Consistency.LINEARIZABLE);

    // Act
    Delete newDelete = Delete.newBuilder(existingDelete).build();

    // Assert
    assertThat(newDelete).isEqualTo(existingDelete);
  }

  @Test
  public void build_FromExistingAndUpdateAllParameters_ShouldBuildDeleteWithUpdatedParameters() {
    // Arrange
    Delete existingDelete1 =
        new Delete(
            NAMESPACE_1,
            TABLE_1,
            partitionKey1,
            clusteringKey1,
            Consistency.LINEARIZABLE,
            ImmutableMap.of("a1", "v1", "a2", "v2", "a3", "v3"),
            condition1);
    Delete existingDelete2 =
        new Delete(
            NAMESPACE_1,
            TABLE_1,
            partitionKey1,
            clusteringKey1,
            Consistency.LINEARIZABLE,
            ImmutableMap.of(
                AbacOperationAttributes.READ_TAG_PREFIX + "policyName1",
                "readTag",
                AbacOperationAttributes.WRITE_TAG_PREFIX + "policyName2",
                "writeTag"),
            condition1);

    // Act
    Delete newDelete1 =
        Delete.newBuilder(existingDelete1)
            .partitionKey(partitionKey2)
            .clusteringKey(clusteringKey2)
            .namespace(NAMESPACE_2)
            .table(TABLE_2)
            .consistency(Consistency.EVENTUAL)
            .condition(condition2)
            .clearAttributes()
            .attribute("a4", "v4")
            .attributes(ImmutableMap.of("a5", "v5", "a6", "v6", "a7", "v7"))
            .clearAttribute("a7")
            .readTag("policyName1", "readTag")
            .writeTag("policyName2", "writeTag")
            .build();
    Delete newDelete2 =
        Delete.newBuilder(existingDelete2)
            .clearReadTag("policyName1")
            .clearWriteTag("policyName2")
            .build();

    // Assert
    assertThat(newDelete1)
        .isEqualTo(
            new Delete(
                NAMESPACE_2,
                TABLE_2,
                partitionKey2,
                clusteringKey2,
                Consistency.EVENTUAL,
                ImmutableMap.of(
                    "a4",
                    "v4",
                    "a5",
                    "v5",
                    "a6",
                    "v6",
                    AbacOperationAttributes.READ_TAG_PREFIX + "policyName1",
                    "readTag",
                    AbacOperationAttributes.WRITE_TAG_PREFIX + "policyName2",
                    "writeTag"),
                condition2));
    assertThat(newDelete2)
        .isEqualTo(
            new Delete(
                NAMESPACE_1,
                TABLE_1,
                partitionKey1,
                clusteringKey1,
                Consistency.LINEARIZABLE,
                ImmutableMap.of(),
                condition1));
  }

  @Test
  public void build_FromExistingAndClearCondition_ShouldBuildDeleteWithoutCondition() {
    // Arrange
    Delete existingDelete =
        new Delete(partitionKey1)
            .forNamespace(NAMESPACE_1)
            .forTable(TABLE_1)
            .withCondition(condition1);

    // Act
    Delete newDelete = Delete.newBuilder(existingDelete).clearCondition().build();

    // Assert
    assertThat(newDelete)
        .isEqualTo(new Delete(partitionKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1));
  }

  @Test
  public void build_FromExistingAndClearClusteringKey_ShouldBuildDeleteWithoutClusteringKey() {
    // Arrange
    Delete existingDelete =
        new Delete(partitionKey1, clusteringKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1);

    // Act
    Delete newDelete = Delete.newBuilder(existingDelete).clearClusteringKey().build();

    // Assert
    assertThat(newDelete)
        .isEqualTo(new Delete(partitionKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1));
  }

  @Test
  public void build_FromExistingAndClearNamespace_ShouldBuildDeleteWithoutNamespace() {
    // Arrange
    Delete existingDelete =
        new Delete(partitionKey1, clusteringKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1);

    // Act
    Delete newDelete = Delete.newBuilder(existingDelete).clearNamespace().build();

    // Assert
    assertThat(newDelete).isEqualTo(new Delete(partitionKey1, clusteringKey1).forTable(TABLE_1));
  }
}
