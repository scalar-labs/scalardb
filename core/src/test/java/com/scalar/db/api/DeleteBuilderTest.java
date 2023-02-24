package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;

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
            .build();

    // Assert
    assertThat(delete)
        .isEqualTo(
            new Delete(partitionKey1, clusteringKey1)
                .forNamespace(NAMESPACE_1)
                .forTable(TABLE_1)
                .withCondition(condition1)
                .withConsistency(Consistency.EVENTUAL));
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
    Delete existingDelete =
        new Delete(partitionKey1, clusteringKey1)
            .forNamespace(NAMESPACE_1)
            .forTable(TABLE_1)
            .withCondition(condition1)
            .withConsistency(Consistency.LINEARIZABLE);

    // Act
    Delete newDelete =
        Delete.newBuilder(existingDelete)
            .partitionKey(partitionKey2)
            .clusteringKey(clusteringKey2)
            .namespace(NAMESPACE_2)
            .table(TABLE_2)
            .consistency(Consistency.EVENTUAL)
            .condition(condition2)
            .build();

    // Assert
    assertThat(newDelete)
        .isEqualTo(
            new Delete(partitionKey2, clusteringKey2)
                .forNamespace(NAMESPACE_2)
                .forTable(TABLE_2)
                .withConsistency(Consistency.EVENTUAL)
                .withCondition(condition2));
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
