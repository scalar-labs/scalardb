package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
  public void buildGet_WithAllParameters_ShouldBuildGetWithAllParameters() {
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
  public void buildGet_FromExistingAndUpdateAllParameters_ShouldBuildGetWithUpdatedParameters() {
    // Arrange
    Get existingGet =
        new Get(partitionKey1, clusteringKey1)
            .forNamespace(NAMESPACE_1)
            .forTable(TABLE_1)
            .withProjections(Arrays.asList("c1", "c2"))
            .withConsistency(Consistency.LINEARIZABLE);

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
                .withProjections(Arrays.asList("c3", "c4", "c5", "c6", "c7")));
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
  public void buildGetWithIndex_WithMandatoryParameters_ShouldBuildGetWithMandatoryParameters() {
    // Arrange Act
    Get actual = Get.newBuilder().table(TABLE_1).indexKey(indexKey1).build();

    // Assert
    assertThat(actual).isEqualTo(new GetWithIndex(indexKey1).forTable(TABLE_1));
  }

  @Test
  public void buildGetWithIndex_WithAllParameters_ShouldBuildGetWithAllParameters() {
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
