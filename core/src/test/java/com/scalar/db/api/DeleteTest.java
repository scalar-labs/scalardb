package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class DeleteTest {
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";

  private Delete prepareDelete() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Delete(partitionKey, clusteringKey);
  }

  @Test
  public void getPartitionKey_ProperKeyGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    Key expected = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    Delete del = new Delete(expected, clusteringKey);

    // Act
    Key actual = del.getPartitionKey();

    // Assert
    assertThat((Iterable<? extends Value<?>>) expected).isEqualTo(actual);
  }

  @Test
  public void getClusteringKey_ProperKeyGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key expected = new Key(ANY_NAME_1, ANY_TEXT_2);
    Delete del = new Delete(partitionKey, expected);

    // Act
    Optional<Key> actual = del.getClusteringKey();

    // Assert
    assertThat(actual).isEqualTo(Optional.of(expected));
  }

  @Test
  public void getClusteringKey_ClusteringKeyNotGivenInConstructor_ShouldReturnNull() {
    // Arrange
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Delete del = new Delete(partitionKey);

    // Act
    Optional<Key> actual = del.getClusteringKey();

    // Assert
    assertThat(actual.isPresent()).isFalse();
  }

  @Test
  public void setConsistency_ProperValueGiven_ShouldReturnWhatsSet() {
    // Arrange
    Delete del = prepareDelete();
    Consistency expected = Consistency.EVENTUAL;

    // Act
    del.withConsistency(expected);

    // Assert
    assertThat(expected).isEqualTo(del.getConsistency());
  }

  @Test
  public void constructor_NullGiven_ShouldThrowNullPointerException() {
    // Act Assert
    assertThatThrownBy(() -> new Delete((Key) null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void constructor_DeleteGiven_ShouldCopyProperly() {
    // Arrange
    Delete del =
        prepareDelete()
            .withCondition(new DeleteIfExists())
            .withConsistency(Consistency.EVENTUAL)
            .forNamespace("n1")
            .forTable("t1");

    // Act
    Delete actual = new Delete(del);

    // Assert
    assertThat(actual).isEqualTo(del);
  }

  @Test
  public void equals_SameInstanceGiven_ShouldReturnTrue() {
    // Arrange
    Delete put = prepareDelete();

    // Act
    @SuppressWarnings("SelfEquals")
    boolean ret = put.equals(put);

    // Assert
    assertThat(ret).isTrue();
  }

  @Test
  public void equals_SameDeleteGiven_ShouldReturnTrue() {
    // Arrange
    Delete delete = prepareDelete();
    Delete another = prepareDelete();

    // Act
    boolean ret = delete.equals(another);

    // Assert
    assertThat(ret).isTrue();
    assertThat(delete.hashCode()).isEqualTo(another.hashCode());
  }

  @Test
  public void equals_SameDeleteWithDeleteIfExistsGiven_ShouldReturnTrue() {
    // Arrange
    Delete delete = prepareDelete().withCondition(new DeleteIfExists());
    Delete another = prepareDelete().withCondition(new DeleteIfExists());

    // Act
    boolean ret = delete.equals(another);

    // Assert
    assertThat(ret).isTrue();
    assertThat(delete.hashCode()).isEqualTo(another.hashCode());
  }

  @Test
  public void getAttribute_ShouldReturnProperValues() {
    // Arrange
    Delete delete =
        Delete.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofText("pk", "pv"))
            .attribute("a1", "v1")
            .attributes(ImmutableMap.of("a2", "v2", "a3", "v3"))
            .build();

    // Act Assert
    assertThat(delete.getAttribute("a1")).hasValue("v1");
    assertThat(delete.getAttribute("a2")).hasValue("v2");
    assertThat(delete.getAttribute("a3")).hasValue("v3");
    assertThat(delete.getAttributes())
        .isEqualTo(ImmutableMap.of("a1", "v1", "a2", "v2", "a3", "v3"));
  }
}
