package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class GetTest {
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_TEXT_3 = "text3";

  private Get prepareGet() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Get(partitionKey, clusteringKey);
  }

  private Get prepareAnotherGet() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_3);
    return new Get(partitionKey, clusteringKey);
  }

  private Put preparePut() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Put(partitionKey, clusteringKey);
  }

  @Test
  public void getPartitionKey_ProperKeyGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    Key expected = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    Get get = new Get(expected, clusteringKey);

    // Act
    Key actual = get.getPartitionKey();

    // Assert
    assertThat((Iterable<? extends Value<?>>) expected).isEqualTo(actual);
  }

  @Test
  public void getClusteringKey_ProperKeyGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key expected = new Key(ANY_NAME_1, ANY_TEXT_2);
    Get get = new Get(partitionKey, expected);

    // Act
    Optional<Key> actual = get.getClusteringKey();

    // Assert
    assertThat(actual).isEqualTo(Optional.of(expected));
  }

  @Test
  public void getClusteringKey_ClusteringKeyNotGivenInConstructor_ShouldReturnNull() {
    // Arrange
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Get get = new Get(partitionKey);

    // Act
    Optional<Key> actual = get.getClusteringKey();

    // Assert
    assertThat(actual.isPresent()).isFalse();
  }

  @Test
  public void addProjection_ProperValueGiven_ShouldReturnWhatsSet() {
    // Arrange
    Get get = prepareGet();
    String projection1 = ANY_NAME_1;
    String projection2 = ANY_NAME_2;

    // Act
    get.withProjection(projection1).withProjection(projection2);

    // Assert
    assertThat(get.getProjections()).isEqualTo(Arrays.asList(projection1, projection2));
  }

  @Test
  public void addProjections_ProperValueGiven_ShouldReturnWhatsSet() {
    // Arrange
    Get get = prepareGet();
    String projection1 = ANY_NAME_1;
    String projection2 = ANY_NAME_2;

    // Act
    get.withProjections(Arrays.asList(projection1, projection2));

    // Assert
    assertThat(get.getProjections()).isEqualTo(Arrays.asList(projection1, projection2));
  }

  @Test
  public void getProjections_TryToModifyReturned_ShouldThrowException() {
    // Arrange
    Get get = prepareGet();
    String projection1 = ANY_NAME_1;
    String projection2 = ANY_NAME_2;
    get.withProjections(Arrays.asList(projection1, projection2));

    // Act Assert
    List<String> projections = get.getProjections();
    assertThatThrownBy(() -> projections.add(ANY_NAME_3))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void setConsistency_ProperValueGiven_ShouldReturnWhatsSet() {
    // Arrange
    Get get = prepareGet();
    Consistency expected = Consistency.EVENTUAL;

    // Act
    get.withConsistency(expected);

    // Assert
    assertThat(expected).isEqualTo(get.getConsistency());
  }

  @Test
  public void constructor_NullGiven_ShouldThrowNullPointerException() {
    // Act Assert
    assertThatThrownBy(() -> new Get((Key) null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void constructor_GetGiven_ShouldCopyProperly() {
    // Arrange
    Get get =
        prepareGet()
            .withProjection("c1")
            .withConsistency(Consistency.EVENTUAL)
            .forNamespace("n1")
            .forTable("t1");

    // Act
    Get actual = new Get(get);

    // Assert
    assertThat(actual).isEqualTo(get);
  }

  @Test
  public void equals_SameInstanceGiven_ShouldReturnTrue() {
    // Arrange
    Get get = prepareGet();

    // Act
    @SuppressWarnings("SelfEquals")
    boolean ret = get.equals(get);

    // Assert
    assertThat(ret).isTrue();
  }

  @Test
  public void equals_SameGetGiven_ShouldReturnTrue() {
    // Arrange
    Get get = prepareGet();
    Get another = prepareGet();

    // Act
    boolean ret = get.equals(another);

    // Assert
    assertThat(ret).isTrue();
    assertThat(get.hashCode()).isEqualTo(another.hashCode());
  }

  @Test
  public void equals_GetWithDifferentClusteringKeyGiven_ShouldReturnFalse() {
    // Arrange
    Get get = prepareGet();
    Get another = prepareAnotherGet();

    // Act
    boolean ret = get.equals(another);

    // Assert
    assertThat(ret).isFalse();
  }

  @Test
  public void equals_GetWithDifferentNamespaceGiven_ShouldReturnFalse() {
    // Arrange
    Get get = prepareGet();
    get.forNamespace("ANY_NAMESPACE1");
    Get another = prepareGet();
    another.forNamespace("ANY_NAMESPACE2");

    // Act
    boolean ret = get.equals(another);

    // Assert
    assertThat(ret).isFalse();
  }

  @Test
  public void equals_GetWithDifferentTableNameGiven_ShouldReturnFalse() {
    // Arrange
    Get get = prepareGet();
    get.forTable("ANY_TABLE1");
    Get another = prepareGet();
    another.forTable("ANY_TABLE2");

    // Act
    boolean ret = get.equals(another);

    // Assert
    assertThat(ret).isFalse();
  }

  @Test
  public void equals_GetWithDifferentConsistencyGiven_ShouldReturnFalse() {
    // Arrange
    Get get = prepareGet();
    get.withConsistency(Consistency.SEQUENTIAL);
    Get another = prepareGet();
    another.withConsistency(Consistency.EVENTUAL);

    // Act
    boolean ret = get.equals(another);

    // Assert
    assertThat(ret).isFalse();
  }

  @Test
  public void equals_GetWithDifferentProjectionsGiven_ShouldReturnFalse() {
    // Arrange
    Get get = prepareGet();
    get.withProjection(ANY_NAME_3);
    Get another = prepareGet();
    another.withProjection(ANY_NAME_2);

    // Act
    boolean ret = get.equals(another);

    // Assert
    assertThat(ret).isFalse();
  }

  @Test
  public void equals_AnotherOperationGiven_ShouldReturnFalse() {
    // Arrange
    Get get = prepareGet();
    Put put = preparePut();

    // Act
    boolean ret = get.equals(put);

    // Assert
    assertThat(ret).isFalse();
  }

  @Test
  public void getAttribute_ShouldReturnProperValues() {
    // Arrange
    Get get =
        Get.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofText("pk", "pv"))
            .attribute("a1", "v1")
            .attributes(ImmutableMap.of("a2", "v2", "a3", "v3"))
            .build();
    Get getWithIndex =
        Get.newBuilder()
            .namespace("ns")
            .table("tbl")
            .indexKey(Key.ofText("pk", "pv"))
            .attribute("a1", "v1")
            .attributes(ImmutableMap.of("a2", "v2", "a3", "v3"))
            .build();

    // Act Assert
    assertThat(get.getAttribute("a1")).hasValue("v1");
    assertThat(get.getAttribute("a2")).hasValue("v2");
    assertThat(get.getAttribute("a3")).hasValue("v3");
    assertThat(get.getAttributes()).isEqualTo(ImmutableMap.of("a1", "v1", "a2", "v2", "a3", "v3"));

    assertThat(getWithIndex.getAttribute("a1")).hasValue("v1");
    assertThat(getWithIndex.getAttribute("a2")).hasValue("v2");
    assertThat(getWithIndex.getAttribute("a3")).hasValue("v3");
    assertThat(getWithIndex.getAttributes())
        .isEqualTo(ImmutableMap.of("a1", "v1", "a2", "v2", "a3", "v3"));
  }
}
