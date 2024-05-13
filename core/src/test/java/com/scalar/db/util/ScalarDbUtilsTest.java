package com.scalar.db.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.api.ScanAll;
import com.scalar.db.api.ScanWithIndex;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
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
  public void copyAndSetTargetToIfNot_InsertGiven_ShouldReturnDifferentInstance() {
    // Arrange
    Insert insert =
        Insert.newBuilder().table(TABLE.get()).partitionKey(Key.ofText("c1", "v1")).build();

    // Act
    Insert actual = ScalarDbUtils.copyAndSetTargetToIfNot(insert, NAMESPACE, TABLE);

    // Assert
    assertThat(actual == insert).isFalse();
    assertThat(insert.forNamespace()).isNotPresent();
    assertThat(insert.forTable()).isEqualTo(TABLE);
    assertThat(actual.forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.forTable()).isEqualTo(TABLE);
  }

  @Test
  public void copyAndSetTargetToIfNot_UpsertGiven_ShouldReturnDifferentInstance() {
    // Arrange
    Upsert upsert =
        Upsert.newBuilder().table(TABLE.get()).partitionKey(Key.ofText("c1", "v1")).build();

    // Act
    Upsert actual = ScalarDbUtils.copyAndSetTargetToIfNot(upsert, NAMESPACE, TABLE);

    // Assert
    assertThat(actual == upsert).isFalse();
    assertThat(upsert.forNamespace()).isNotPresent();
    assertThat(upsert.forTable()).isEqualTo(TABLE);
    assertThat(actual.forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.forTable()).isEqualTo(TABLE);
  }

  @Test
  public void copyAndSetTargetToIfNot_UpdateGiven_ShouldReturnDifferentInstance() {
    // Arrange
    Update update =
        Update.newBuilder().table(TABLE.get()).partitionKey(Key.ofText("c1", "v1")).build();

    // Act
    Update actual = ScalarDbUtils.copyAndSetTargetToIfNot(update, NAMESPACE, TABLE);

    // Assert
    assertThat(actual == update).isFalse();
    assertThat(update.forNamespace()).isNotPresent();
    assertThat(update.forTable()).isEqualTo(TABLE);
    assertThat(actual.forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.forTable()).isEqualTo(TABLE);
  }

  @Test
  public void copyAndSetTargetToIfNot_MutationsGiven_ShouldReturnDifferentInstance() {
    // Arrange
    Put put = new Put(new Key("c1", "v1"));
    Delete delete = new Delete(new Key("c1", "v1"));
    Insert insert =
        Insert.newBuilder().table(TABLE.get()).partitionKey(Key.ofText("c1", "v1")).build();
    Upsert upsert =
        Upsert.newBuilder().table(TABLE.get()).partitionKey(Key.ofText("c1", "v1")).build();
    Update update =
        Update.newBuilder().table(TABLE.get()).partitionKey(Key.ofText("c1", "v1")).build();
    List<Mutation> mutations = Arrays.asList(put, delete, insert, upsert, update);

    // Act
    List<Mutation> actual = ScalarDbUtils.copyAndSetTargetToIfNot(mutations, NAMESPACE, TABLE);

    // Assert
    assertThat(actual == mutations).isFalse();
    assertThat(actual.get(0) == put).isFalse();
    assertThat(actual.get(1) == delete).isFalse();
    assertThat(actual.get(2) == insert).isFalse();
    assertThat(actual.get(3) == upsert).isFalse();
    assertThat(actual.get(4) == update).isFalse();
    assertThat(put.forNamespace()).isNotPresent();
    assertThat(put.forTable()).isNotPresent();
    assertThat(delete.forNamespace()).isNotPresent();
    assertThat(delete.forTable()).isNotPresent();
    assertThat(insert.forNamespace()).isNotPresent();
    assertThat(insert.forTable()).isEqualTo(TABLE);
    assertThat(upsert.forNamespace()).isNotPresent();
    assertThat(upsert.forTable()).isEqualTo(TABLE);
    assertThat(update.forNamespace()).isNotPresent();
    assertThat(update.forTable()).isEqualTo(TABLE);
    assertThat(actual.get(0).forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.get(0).forTable()).isEqualTo(TABLE);
    assertThat(actual.get(1).forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.get(1).forTable()).isEqualTo(TABLE);
    assertThat(actual.get(2).forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.get(2).forTable()).isEqualTo(TABLE);
    assertThat(actual.get(3).forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.get(3).forTable()).isEqualTo(TABLE);
    assertThat(actual.get(4).forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.get(4).forTable()).isEqualTo(TABLE);
  }

  @Test
  public void checkUpdate_ShouldBehaveProperly() {
    // Arrange
    Update updateWithValidCondition1 =
        Update.newBuilder()
            .namespace(NAMESPACE.get())
            .table(TABLE.get())
            .partitionKey(Key.ofText("c1", "v1"))
            .condition(
                ConditionBuilder.updateIf(ConditionBuilder.column("c2").isEqualToText("v2"))
                    .build())
            .build();
    Update updateWithValidCondition2 =
        Update.newBuilder()
            .namespace(NAMESPACE.get())
            .table(TABLE.get())
            .partitionKey(Key.ofText("c1", "v1"))
            .condition(ConditionBuilder.updateIfExists())
            .build();
    Update updateWithInvalidCondition =
        Update.newBuilder()
            .namespace(NAMESPACE.get())
            .table(TABLE.get())
            .partitionKey(Key.ofText("c1", "v1"))
            .condition(ConditionBuilder.putIfExists())
            .build();

    // Act
    assertThatCode(() -> ScalarDbUtils.checkUpdate(updateWithValidCondition1))
        .doesNotThrowAnyException();
    assertThatCode(() -> ScalarDbUtils.checkUpdate(updateWithValidCondition2))
        .doesNotThrowAnyException();
    assertThatThrownBy(() -> ScalarDbUtils.checkUpdate(updateWithInvalidCondition))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
