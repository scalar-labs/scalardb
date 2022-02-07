package com.scalar.db.util;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.io.Key;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.junit.Test;

@SuppressWarnings("ReferenceEquality")
public class ScalarDbUtilsTest {

  private static final Optional<String> NAMESPACE = Optional.of("ns");
  private static final Optional<String> TABLE = Optional.of("tbl");

  @Test
  public void setTargetToIfNot_GetGivenWithoutCopy_ShouldReturnSameInstance() {
    // Arrange
    Get get = new Get(new Key("c1", "v1"));

    // Act
    Get actual = ScalarDbUtils.setTargetToIfNot(get, NAMESPACE, TABLE, false);

    // Assert
    assertThat(actual == get).isTrue();
    assertThat(get.forNamespace()).isEqualTo(NAMESPACE);
    assertThat(get.forTable()).isEqualTo(TABLE);
    assertThat(actual.forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.forTable()).isEqualTo(TABLE);
  }

  @Test
  public void setTargetToIfNot_GetGivenWithCopy_ShouldReturnDifferentInstance() {
    // Arrange
    Get get = new Get(new Key("c1", "v1"));

    // Act
    Get actual = ScalarDbUtils.setTargetToIfNot(get, NAMESPACE, TABLE, true);

    // Assert
    assertThat(actual == get).isFalse();
    assertThat(get.forNamespace()).isNotPresent();
    assertThat(get.forTable()).isNotPresent();
    assertThat(actual.forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.forTable()).isEqualTo(TABLE);
  }

  @Test
  public void setTargetToIfNot_ScanGivenWithoutCopy_ShouldReturnSameInstance() {
    // Arrange
    Scan scan = new Scan(new Key("c1", "v1"));

    // Act
    Scan actual = ScalarDbUtils.setTargetToIfNot(scan, NAMESPACE, TABLE, false);

    // Assert
    assertThat(actual == scan).isTrue();
    assertThat(scan.forNamespace()).isEqualTo(NAMESPACE);
    assertThat(scan.forTable()).isEqualTo(TABLE);
    assertThat(actual.forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.forTable()).isEqualTo(TABLE);
  }

  @Test
  public void setTargetToIfNot_ScanGivenWithCopy_ShouldReturnDifferentInstance() {
    // Arrange
    Scan scan = new Scan(new Key("c1", "v1"));

    // Act
    Scan actual = ScalarDbUtils.setTargetToIfNot(scan, NAMESPACE, TABLE, true);

    // Assert
    assertThat(actual == scan).isFalse();
    assertThat(scan.forNamespace()).isNotPresent();
    assertThat(scan.forTable()).isNotPresent();
    assertThat(actual.forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.forTable()).isEqualTo(TABLE);
  }

  @Test
  public void setTargetToIfNot_PutGivenWithoutCopy_ShouldReturnSameInstance() {
    // Arrange
    Put put = new Put(new Key("c1", "v1"));

    // Act
    Put actual = ScalarDbUtils.setTargetToIfNot(put, NAMESPACE, TABLE, false);

    // Assert
    assertThat(actual == put).isTrue();
    assertThat(put.forNamespace()).isEqualTo(NAMESPACE);
    assertThat(put.forTable()).isEqualTo(TABLE);
    assertThat(actual.forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.forTable()).isEqualTo(TABLE);
  }

  @Test
  public void setTargetToIfNot_PutGivenWithCopy_ShouldReturnDifferentInstance() {
    // Arrange
    Put put = new Put(new Key("c1", "v1"));

    // Act
    Put actual = ScalarDbUtils.setTargetToIfNot(put, NAMESPACE, TABLE, true);

    // Assert
    assertThat(actual == put).isFalse();
    assertThat(put.forNamespace()).isNotPresent();
    assertThat(put.forTable()).isNotPresent();
    assertThat(actual.forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.forTable()).isEqualTo(TABLE);
  }

  @Test
  public void setTargetToIfNot_DeleteGivenWithoutCopy_ShouldReturnSameInstance() {
    // Arrange
    Delete delete = new Delete(new Key("c1", "v1"));

    // Act
    Delete actual = ScalarDbUtils.setTargetToIfNot(delete, NAMESPACE, TABLE, false);

    // Assert
    assertThat(actual == delete).isTrue();
    assertThat(delete.forNamespace()).isEqualTo(NAMESPACE);
    assertThat(delete.forTable()).isEqualTo(TABLE);
    assertThat(actual.forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.forTable()).isEqualTo(TABLE);
  }

  @Test
  public void setTargetToIfNot_DeleteGivenWithCopy_ShouldReturnDifferentInstance() {
    // Arrange
    Delete delete = new Delete(new Key("c1", "v1"));

    // Act
    Delete actual = ScalarDbUtils.setTargetToIfNot(delete, NAMESPACE, TABLE, true);

    // Assert
    assertThat(actual == delete).isFalse();
    assertThat(delete.forNamespace()).isNotPresent();
    assertThat(delete.forTable()).isNotPresent();
    assertThat(actual.forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.forTable()).isEqualTo(TABLE);
  }

  @Test
  public void setTargetToIfNot_MutationsGivenWithoutCopy_ShouldReturnSameInstance() {
    // Arrange
    Put put = new Put(new Key("c1", "v1"));
    Delete delete = new Delete(new Key("c1", "v1"));
    List<Mutation> mutations = Arrays.asList(put, delete);

    // Act
    List<Mutation> actual = ScalarDbUtils.setTargetToIfNot(mutations, NAMESPACE, TABLE, false);

    // Assert
    assertThat(actual == mutations).isTrue();
    assertThat(actual.get(0) == put).isTrue();
    assertThat(actual.get(1) == delete).isTrue();
    assertThat(put.forNamespace()).isEqualTo(NAMESPACE);
    assertThat(put.forTable()).isEqualTo(TABLE);
    assertThat(delete.forNamespace()).isEqualTo(NAMESPACE);
    assertThat(delete.forTable()).isEqualTo(TABLE);
    assertThat(actual.get(0).forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.get(0).forTable()).isEqualTo(TABLE);
    assertThat(actual.get(1).forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.get(1).forTable()).isEqualTo(TABLE);
  }

  @Test
  public void setTargetToIfNot_MutationsGivenWithCopy_ShouldReturnDifferentInstance() {
    // Arrange
    Put put = new Put(new Key("c1", "v1"));
    Delete delete = new Delete(new Key("c1", "v1"));
    List<Mutation> mutations = Arrays.asList(put, delete);

    // Act
    List<Mutation> actual = ScalarDbUtils.setTargetToIfNot(mutations, NAMESPACE, TABLE, true);

    // Assert
    assertThat(actual == mutations).isFalse();
    assertThat(actual.get(0) == put).isFalse();
    assertThat(actual.get(1) == delete).isFalse();
    assertThat(put.forNamespace()).isNotPresent();
    assertThat(put.forTable()).isNotPresent();
    assertThat(delete.forNamespace()).isNotPresent();
    assertThat(delete.forTable()).isNotPresent();
    assertThat(actual.get(0).forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.get(0).forTable()).isEqualTo(TABLE);
    assertThat(actual.get(1).forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.get(1).forTable()).isEqualTo(TABLE);
  }
}
