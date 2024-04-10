package com.scalar.db.util;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.LikeExpression;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.api.ScanAll;
import com.scalar.db.api.ScanWithIndex;
import com.scalar.db.io.Key;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

@SuppressWarnings("ReferenceEquality")
public class ScalarDbUtilsTest {

  private static final Optional<String> NAMESPACE = Optional.of("ns");
  private static final Optional<String> TABLE = Optional.of("tbl");

  @Test
  public void copyAndSetTargetToIfNot_GetGiven_ShouldReturnDifferentInstance() {
    // Arrange
    Get get = new Get(new Key("c1", "v1"));

    // Act
    Get actual = ScalarDbUtils.copyAndSetTargetToIfNot(get, NAMESPACE, TABLE);

    // Assert
    assertThat(actual == get).isFalse();
    assertThat(get.forNamespace()).isNotPresent();
    assertThat(get.forTable()).isNotPresent();
    assertThat(actual.forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.forTable()).isEqualTo(TABLE);
  }

  @Test
  public void copyAndSetTargetToIfNot_ScanGiven_ShouldReturnDifferentInstance() {
    // Arrange
    Scan scan = new Scan(new Key("c1", "v1"));

    // Act
    Scan actual = ScalarDbUtils.copyAndSetTargetToIfNot(scan, NAMESPACE, TABLE);

    // Assert
    assertThat(actual == scan).isFalse();
    assertThat(actual instanceof ScanWithIndex).isFalse();
    assertThat(actual instanceof ScanAll).isFalse();
    assertThat(scan.forNamespace()).isNotPresent();
    assertThat(scan.forTable()).isNotPresent();
    assertThat(actual.forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.forTable()).isEqualTo(TABLE);
  }

  @Test
  public void copyAndSetTargetToIfNot_ScanAllGiven_ShouldReturnDifferentInstance() {
    // Arrange
    Scan scanAll = new ScanAll();

    // Act
    Scan actual = ScalarDbUtils.copyAndSetTargetToIfNot(scanAll, NAMESPACE, TABLE);

    // Assert
    assertThat(actual == scanAll).isFalse();
    assertThat(actual instanceof ScanAll).isTrue();
    assertThat(scanAll.forNamespace()).isNotPresent();
    assertThat(scanAll.forTable()).isNotPresent();
    assertThat(actual.forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.forTable()).isEqualTo(TABLE);
  }

  @Test
  public void copyAndSetTargetToIfNot_ScanWithIndexGiven_ShouldReturnDifferentInstance() {
    // Arrange
    Scan scanWithIndex = new ScanWithIndex(new Key("c2", "v2"));

    // Act
    Scan actual = ScalarDbUtils.copyAndSetTargetToIfNot(scanWithIndex, NAMESPACE, TABLE);

    // Assert
    assertThat(actual == scanWithIndex).isFalse();
    assertThat(actual instanceof ScanWithIndex).isTrue();
    assertThat(scanWithIndex.forNamespace()).isNotPresent();
    assertThat(scanWithIndex.forTable()).isNotPresent();
    assertThat(actual.forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.forTable()).isEqualTo(TABLE);
  }

  @Test
  public void copyAndSetTargetToIfNot_PutGiven_ShouldReturnDifferentInstance() {
    // Arrange
    Put put = new Put(new Key("c1", "v1"));

    // Act
    Put actual = ScalarDbUtils.copyAndSetTargetToIfNot(put, NAMESPACE, TABLE);

    // Assert
    assertThat(actual == put).isFalse();
    assertThat(put.forNamespace()).isNotPresent();
    assertThat(put.forTable()).isNotPresent();
    assertThat(actual.forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.forTable()).isEqualTo(TABLE);
  }

  @Test
  public void copyAndSetTargetToIfNot_DeleteGiven_ShouldReturnDifferentInstance() {
    // Arrange
    Delete delete = new Delete(new Key("c1", "v1"));

    // Act
    Delete actual = ScalarDbUtils.copyAndSetTargetToIfNot(delete, NAMESPACE, TABLE);

    // Assert
    assertThat(actual == delete).isFalse();
    assertThat(delete.forNamespace()).isNotPresent();
    assertThat(delete.forTable()).isNotPresent();
    assertThat(actual.forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.forTable()).isEqualTo(TABLE);
  }

  @Test
  public void copyAndSetTargetToIfNot_MutationsGiven_ShouldReturnDifferentInstance() {
    // Arrange
    Put put = new Put(new Key("c1", "v1"));
    Delete delete = new Delete(new Key("c1", "v1"));
    List<Mutation> mutations = Arrays.asList(put, delete);

    // Act
    List<Mutation> actual = ScalarDbUtils.copyAndSetTargetToIfNot(mutations, NAMESPACE, TABLE);

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

  @Test
  public void isMatched_SomePatternsWithoutEscapeGiven_ShouldReturnBooleanProperly() {
    // Arrange

    // Act Assert
    // The following tests are added referring to the similar tests in Spark.
    // https://github.com/apache/spark/blob/master/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/RegexpExpressionsSuite.scala
    // simple patterns
    assertThat(ScalarDbUtils.isMatched(prepareLike("abdef"), "abdef")).isTrue();
    assertThat(ScalarDbUtils.isMatched(prepareLike("a\\__b"), "a_%b")).isTrue();
    assertThat(ScalarDbUtils.isMatched(prepareLike("a_%b"), "addb")).isTrue();
    assertThat(ScalarDbUtils.isMatched(prepareLike("a\\__b"), "addb")).isFalse();
    assertThat(ScalarDbUtils.isMatched(prepareLike("a%\\%b"), "addb")).isFalse();
    assertThat(ScalarDbUtils.isMatched(prepareLike("a%\\%b"), "a_%b")).isTrue();
    assertThat(ScalarDbUtils.isMatched(prepareLike("a%"), "addb")).isTrue();
    assertThat(ScalarDbUtils.isMatched(prepareLike("**"), "addb")).isFalse();
    assertThat(ScalarDbUtils.isMatched(prepareLike("a%"), "abc")).isTrue();
    assertThat(ScalarDbUtils.isMatched(prepareLike("b%"), "abc")).isFalse();
    assertThat(ScalarDbUtils.isMatched(prepareLike("bc%"), "abc")).isFalse();
    assertThat(ScalarDbUtils.isMatched(prepareLike("a_b"), "a\nb")).isTrue();
    assertThat(ScalarDbUtils.isMatched(prepareLike("a%b"), "ab")).isTrue();
    assertThat(ScalarDbUtils.isMatched(prepareLike("a%b"), "a\nb")).isTrue();

    // empty input
    assertThat(ScalarDbUtils.isMatched(prepareLike(""), "")).isTrue();
    assertThat(ScalarDbUtils.isMatched(prepareLike(""), "a")).isFalse();
    assertThat(ScalarDbUtils.isMatched(prepareLike("a"), "")).isFalse();

    // SI-17647 double-escaping backslash
    assertThat(ScalarDbUtils.isMatched(prepareLike("%\\\\%"), "\\\\\\\\")).isTrue();
    assertThat(ScalarDbUtils.isMatched(prepareLike("%%"), "%%")).isTrue();
    assertThat(ScalarDbUtils.isMatched(prepareLike("\\\\\\__"), "\\__")).isTrue();
    assertThat(ScalarDbUtils.isMatched(prepareLike("%\\\\%\\%"), "\\\\\\__")).isFalse();
    assertThat(ScalarDbUtils.isMatched(prepareLike("%\\\\"), "_\\\\\\%")).isFalse();

    // unicode
    assertThat(ScalarDbUtils.isMatched(prepareLike("_\u20AC_"), "a\u20ACa")).isTrue();
    assertThat(ScalarDbUtils.isMatched(prepareLike("_€_"), "a€a")).isTrue();
    assertThat(ScalarDbUtils.isMatched(prepareLike("_\u20AC_"), "a€a")).isTrue();
    assertThat(ScalarDbUtils.isMatched(prepareLike("_€_"), "a\u20ACa")).isTrue();

    // case
    assertThat(ScalarDbUtils.isMatched(prepareLike("a%"), "A")).isFalse();
    assertThat(ScalarDbUtils.isMatched(prepareLike("A%"), "a")).isFalse();
    assertThat(ScalarDbUtils.isMatched(prepareLike("_a_"), "AaA")).isTrue();

    // example
    assertThat(
            ScalarDbUtils.isMatched(
                prepareLike("\\%SystemDrive\\%\\\\Users%"), "%SystemDrive%\\Users\\John"))
        .isTrue();
  }

  @Test
  public void isMatched_SomePatternsWithEscapeGiven_ShouldReturnBooleanProperly() {
    // Arrange

    // Act Assert
    // The following tests are added referring to the similar tests in Spark.
    // https://github.com/apache/spark/blob/master/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/RegexpExpressionsSuite.scala
    ImmutableList.of("/", "#", "\"")
        .forEach(
            escape -> {
              // simple patterns
              assertThat(ScalarDbUtils.isMatched(prepareLike("abdef", escape), "abdef")).isTrue();
              assertThat(ScalarDbUtils.isMatched(prepareLike("a" + escape + "__b", escape), "a_%b"))
                  .isTrue();
              assertThat(ScalarDbUtils.isMatched(prepareLike("a_%b", escape), "addb")).isTrue();
              assertThat(ScalarDbUtils.isMatched(prepareLike("a" + escape + "__b", escape), "addb"))
                  .isFalse();
              assertThat(ScalarDbUtils.isMatched(prepareLike("a%" + escape + "%b", escape), "addb"))
                  .isFalse();
              assertThat(ScalarDbUtils.isMatched(prepareLike("a%" + escape + "%b", escape), "a_%b"))
                  .isTrue();
              assertThat(ScalarDbUtils.isMatched(prepareLike("a%", escape), "addb")).isTrue();
              assertThat(ScalarDbUtils.isMatched(prepareLike("**", escape), "addb")).isFalse();
              assertThat(ScalarDbUtils.isMatched(prepareLike("a%", escape), "abc")).isTrue();
              assertThat(ScalarDbUtils.isMatched(prepareLike("b%", escape), "abc")).isFalse();
              assertThat(ScalarDbUtils.isMatched(prepareLike("bc%", escape), "abc")).isFalse();
              assertThat(ScalarDbUtils.isMatched(prepareLike("a_b", escape), "a\nb")).isTrue();
              assertThat(ScalarDbUtils.isMatched(prepareLike("a%b", escape), "ab")).isTrue();
              assertThat(ScalarDbUtils.isMatched(prepareLike("a%b", escape), "a\nb")).isTrue();

              // empty input
              assertThat(ScalarDbUtils.isMatched(prepareLike("", escape), "")).isTrue();
              assertThat(ScalarDbUtils.isMatched(prepareLike("", escape), "a")).isFalse();
              assertThat(ScalarDbUtils.isMatched(prepareLike("a", escape), "")).isFalse();

              // SI-17647 double-escaping backslash
              assertThat(
                      ScalarDbUtils.isMatched(
                          prepareLike(String.format("%%%s%s%%", escape, escape), escape),
                          String.format("%s%s%s%s", escape, escape, escape, escape)))
                  .isTrue();
              assertThat(ScalarDbUtils.isMatched(prepareLike("%%", escape), "%%")).isTrue();
              assertThat(
                      ScalarDbUtils.isMatched(
                          prepareLike(String.format("%s%s%s__", escape, escape, escape), escape),
                          String.format("%s__", escape)))
                  .isTrue();
              assertThat(
                      ScalarDbUtils.isMatched(
                          prepareLike(
                              String.format("%%%s%s%%%s%%", escape, escape, escape), escape),
                          String.format("%s%s%s__", escape, escape, escape)))
                  .isFalse();
              assertThat(
                      ScalarDbUtils.isMatched(
                          prepareLike(String.format("%%%s%s", escape, escape), escape),
                          String.format("_%s%s%s%%", escape, escape, escape)))
                  .isFalse();

              // unicode
              assertThat(ScalarDbUtils.isMatched(prepareLike("_\u20AC_", escape), "a\u20ACa"))
                  .isTrue();
              assertThat(ScalarDbUtils.isMatched(prepareLike("_€_", escape), "a€a")).isTrue();
              assertThat(ScalarDbUtils.isMatched(prepareLike("_\u20AC_", escape), "a€a")).isTrue();
              assertThat(ScalarDbUtils.isMatched(prepareLike("_€_", escape), "a\u20ACa")).isTrue();

              // case
              assertThat(ScalarDbUtils.isMatched(prepareLike("a%", escape), "A")).isFalse();
              assertThat(ScalarDbUtils.isMatched(prepareLike("A%", escape), "a")).isFalse();
              assertThat(ScalarDbUtils.isMatched(prepareLike("_a_", escape), "AaA")).isTrue();

              // example
              assertThat(
                      ScalarDbUtils.isMatched(
                          prepareLike(
                              String.format(
                                  "%s%%SystemDrive%s%%%s%sUsers%%", escape, escape, escape, escape),
                              escape),
                          String.format("%%SystemDrive%%%sUsers%sJohn", escape, escape)))
                  .isTrue();
            });
  }

  @Test
  public void isMatched_IsNotLikeOperatorWithSomePatternsGiven_ShouldReturnBooleanProperly() {
    // Arrange

    // Act Assert
    assertThat(ScalarDbUtils.isMatched(prepareNotLike("abdef"), "abdef")).isFalse();
    assertThat(ScalarDbUtils.isMatched(prepareNotLike("a\\__b"), "a_%b")).isFalse();
    assertThat(ScalarDbUtils.isMatched(prepareNotLike("a_%b"), "addb")).isFalse();
    assertThat(ScalarDbUtils.isMatched(prepareNotLike("a\\__b"), "addb")).isTrue();
    ImmutableList.of("/", "#", "\"")
        .forEach(
            escape -> {
              assertThat(ScalarDbUtils.isMatched(prepareNotLike("abdef", escape), "abdef"))
                  .isFalse();
              assertThat(
                      ScalarDbUtils.isMatched(prepareNotLike("a" + escape + "__b", escape), "a_%b"))
                  .isFalse();
              assertThat(ScalarDbUtils.isMatched(prepareNotLike("a_%b", escape), "addb")).isFalse();
              assertThat(
                      ScalarDbUtils.isMatched(prepareNotLike("a" + escape + "__b", escape), "addb"))
                  .isTrue();
            });
  }

  private LikeExpression prepareLike(String pattern) {
    return ConditionBuilder.column("col1").isLikeText(pattern);
  }

  private LikeExpression prepareLike(String pattern, String escape) {
    return ConditionBuilder.column("col1").isLikeText(pattern, escape);
  }

  private LikeExpression prepareNotLike(String pattern) {
    return ConditionBuilder.column("col1").isNotLikeText(pattern);
  }

  private LikeExpression prepareNotLike(String pattern, String escape) {
    return ConditionBuilder.column("col1").isNotLikeText(pattern, escape);
  }
}
