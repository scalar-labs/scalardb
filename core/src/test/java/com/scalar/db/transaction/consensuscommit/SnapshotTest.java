package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class SnapshotTest {
  private static final String ANY_NAMESPACE_NAME = "namespace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_ID = "id";
  private static final int ANY_VERSION = 1;
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_NAME_4 = "name4";
  private static final String ANY_NAME_5 = "name5";
  private static final String ANY_NAME_6 = "name6";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_TEXT_3 = "text3";
  private static final String ANY_TEXT_4 = "text4";
  private static final String ANY_TEXT_5 = "text5";
  private static final String ANY_TEXT_6 = "text6";
  private Snapshot snapshot;
  private Map<Snapshot.Key, Optional<TransactionResult>> readSet;
  private Map<Scan, List<Snapshot.Key>> scanSet;
  private Map<Snapshot.Key, Put> writeSet;
  private Map<Snapshot.Key, Delete> deleteSet;

  @Mock private ConsensusCommitConfig config;
  @Mock private PrepareMutationComposer prepareComposer;
  @Mock private CommitMutationComposer commitComposer;
  @Mock private RollbackMutationComposer rollbackComposer;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  private Snapshot prepareSnapshot(Isolation isolation) {
    return prepareSnapshot(isolation, SerializableStrategy.EXTRA_WRITE);
  }

  private Snapshot prepareSnapshot(Isolation isolation, SerializableStrategy strategy) {
    readSet = new HashMap<>();
    scanSet = new HashMap<>();
    writeSet = new HashMap<>();
    deleteSet = new HashMap<>();

    return spy(
        new Snapshot(
            ANY_ID,
            isolation,
            strategy,
            new ParallelExecutor(config),
            readSet,
            scanSet,
            writeSet,
            deleteSet));
  }

  private TransactionResult prepareResult(String txId) {
    TransactionResult result = mock(TransactionResult.class);
    when(result.getPartitionKey())
        .thenReturn(Optional.of(new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1))));
    when(result.getClusteringKey())
        .thenReturn(Optional.of(new Key(new TextValue(ANY_NAME_2, ANY_TEXT_2))));
    when(result.getValues())
        .thenReturn(
            ImmutableMap.<String, Value<?>>builder()
                .put(ANY_NAME_1, new TextValue(ANY_NAME_1, ANY_TEXT_1))
                .put(ANY_NAME_2, new TextValue(ANY_NAME_2, ANY_TEXT_2))
                .put(ANY_NAME_3, new TextValue(ANY_NAME_3, ANY_TEXT_3))
                .put(ANY_NAME_4, new TextValue(ANY_NAME_4, ANY_TEXT_4))
                .put(Attribute.ID, Attribute.toIdValue(txId))
                .put(Attribute.VERSION, Attribute.toVersionValue(ANY_VERSION))
                .build());
    return result;
  }

  private Get prepareGet() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Get(partitionKey, clusteringKey)
        .withConsistency(Consistency.LINEARIZABLE)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  private Get prepareAnotherGet() {
    Key partitionKey = new Key(ANY_NAME_5, ANY_TEXT_5);
    Key clusteringKey = new Key(ANY_NAME_6, ANY_TEXT_6);
    return new Get(partitionKey, clusteringKey)
        .withConsistency(Consistency.LINEARIZABLE)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  private Scan prepareScan() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Scan(partitionKey)
        .withStart(clusteringKey)
        .withConsistency(Consistency.LINEARIZABLE)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  private Put preparePut() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Put(partitionKey, clusteringKey)
        .withConsistency(Consistency.LINEARIZABLE)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME)
        .withValue(ANY_NAME_3, ANY_TEXT_3)
        .withValue(ANY_NAME_4, ANY_TEXT_4);
  }

  private Put prepareAnotherPut() {
    Key partitionKey = new Key(ANY_NAME_5, ANY_TEXT_5);
    Key clusteringKey = new Key(ANY_NAME_6, ANY_TEXT_6);
    return new Put(partitionKey, clusteringKey)
        .withConsistency(Consistency.LINEARIZABLE)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  private Put preparePutWithPartitionKeyOnly() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    return new Put(partitionKey)
        .withConsistency(Consistency.LINEARIZABLE)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME)
        .withValue(ANY_NAME_3, ANY_TEXT_3)
        .withValue(ANY_NAME_4, ANY_TEXT_4);
  }

  private Delete prepareDelete() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Delete(partitionKey, clusteringKey)
        .withConsistency(Consistency.LINEARIZABLE)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  private Delete prepareAnotherDelete() {
    Key partitionKey = new Key(ANY_NAME_5, ANY_TEXT_5);
    Key clusteringKey = new Key(ANY_NAME_6, ANY_TEXT_6);
    return new Delete(partitionKey, clusteringKey)
        .withConsistency(Consistency.LINEARIZABLE)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  private void configureBehavior() throws ExecutionException {
    doNothing().when(prepareComposer).add(any(Put.class), any(TransactionResult.class));
    doNothing().when(prepareComposer).add(any(Delete.class), any(TransactionResult.class));
    doNothing().when(commitComposer).add(any(Put.class), any(TransactionResult.class));
    doNothing().when(commitComposer).add(any(Delete.class), any(TransactionResult.class));
    doNothing().when(rollbackComposer).add(any(Put.class), any(TransactionResult.class));
    doNothing().when(rollbackComposer).add(any(Delete.class), any(TransactionResult.class));
  }

  @Test
  public void put_ResultGiven_ShouldHoldWhatsGivenInReadSet() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Snapshot.Key key = new Snapshot.Key(prepareGet());
    TransactionResult result = prepareResult(ANY_ID);

    // Act
    snapshot.put(key, Optional.of(result));

    // Assert
    assertThat(readSet.get(key)).isEqualTo(Optional.of(result));
  }

  @Test
  public void put_PutGiven_ShouldHoldWhatsGivenInWriteSet() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Put put = preparePut();
    Snapshot.Key key = new Snapshot.Key(put);

    // Act
    snapshot.put(key, put);

    // Assert
    assertThat(writeSet.get(key)).isEqualTo(put);
  }

  @Test
  public void put_PutGivenTwice_ShouldHoldMergedPut() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Put put1 = preparePut();
    Snapshot.Key key = new Snapshot.Key(put1);

    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    Put put2 =
        new Put(partitionKey, clusteringKey)
            .withConsistency(Consistency.LINEARIZABLE)
            .forNamespace(ANY_NAMESPACE_NAME)
            .forTable(ANY_TABLE_NAME)
            .withValue(ANY_NAME_3, ANY_TEXT_5);

    // Act
    snapshot.put(key, put1);
    snapshot.put(key, put2);

    // Assert
    assertThat(writeSet.get(key).getValues())
        .isEqualTo(
            ImmutableMap.of(
                ANY_NAME_3,
                new TextValue(ANY_NAME_3, ANY_TEXT_5),
                ANY_NAME_4,
                new TextValue(ANY_NAME_4, ANY_TEXT_4)));
  }

  @Test
  public void put_DeleteGiven_ShouldHoldWhatsGivenInDeleteSet() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Delete delete = prepareDelete();
    Snapshot.Key key = new Snapshot.Key(delete);

    // Act
    snapshot.put(key, delete);

    // Assert
    assertThat(deleteSet.get(key)).isEqualTo(delete);
  }

  @Test
  public void put_ScanGiven_ShouldHoldWhatsGivenInScanSet() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Scan scan = prepareScan();
    Snapshot.Key key = new Snapshot.Key(scan, prepareResult(ANY_ID));
    List<Snapshot.Key> expected = Collections.singletonList(key);

    // Act
    snapshot.put(scan, expected);

    // Assert
    assertThat(scanSet.get(scan)).isEqualTo(expected);
  }

  @Test
  public void get_KeyGivenContainedInWriteSetAndReadSet_ShouldReturnMergedResult() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    Put put =
        new Put(partitionKey, clusteringKey)
            .withConsistency(Consistency.LINEARIZABLE)
            .forNamespace(ANY_NAMESPACE_NAME)
            .forTable(ANY_TABLE_NAME)
            .withValue(ANY_NAME_3, ANY_TEXT_5);
    Snapshot.Key key = new Snapshot.Key(prepareGet());
    TransactionResult result = prepareResult(ANY_ID);
    snapshot.put(key, Optional.of(result));
    snapshot.put(key, put);

    // Act
    Optional<TransactionResult> actual = snapshot.get(key);

    // Assert
    assertThat(actual).isPresent();
    assertThat(actual.get().getValues())
        .isEqualTo(
            ImmutableMap.<String, Value<?>>builder()
                .put(ANY_NAME_1, new TextValue(ANY_NAME_1, ANY_TEXT_1))
                .put(ANY_NAME_2, new TextValue(ANY_NAME_2, ANY_TEXT_2))
                .put(ANY_NAME_3, new TextValue(ANY_NAME_3, ANY_TEXT_5))
                .put(ANY_NAME_4, new TextValue(ANY_NAME_4, ANY_TEXT_4))
                .put(Attribute.ID, Attribute.toIdValue(ANY_ID))
                .put(Attribute.VERSION, Attribute.toVersionValue(ANY_VERSION))
                .build());
    assertThat(actual.get().getValue(ANY_NAME_1).get().getAsString().get()).isEqualTo(ANY_TEXT_1);
    assertThat(actual.get().getValue(ANY_NAME_2).get().getAsString().get()).isEqualTo(ANY_TEXT_2);
    assertThat(actual.get().getValue(ANY_NAME_3).get().getAsString().get()).isEqualTo(ANY_TEXT_5);
    assertThat(actual.get().getValue(ANY_NAME_4).get().getAsString().get()).isEqualTo(ANY_TEXT_4);
    assertThat(actual.get().getValue(Attribute.ID).get().getAsString().get()).isEqualTo(ANY_ID);
    assertThat(actual.get().getValue(Attribute.VERSION).get().getAsInt()).isEqualTo(ANY_VERSION);
  }

  @Test
  public void
      get_KeyGivenContainedInWriteSetAndReadSet_AndEmptyResultInReadSet_ShouldReturnMergedResult() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    Put put =
        new Put(partitionKey, clusteringKey)
            .withConsistency(Consistency.LINEARIZABLE)
            .forNamespace(ANY_NAMESPACE_NAME)
            .forTable(ANY_TABLE_NAME)
            .withValue(ANY_NAME_3, ANY_TEXT_5);
    Snapshot.Key key = new Snapshot.Key(prepareGet());
    snapshot.put(key, Optional.empty());
    snapshot.put(key, put);

    // Act
    Optional<TransactionResult> actual = snapshot.get(key);

    // Assert
    assertThat(actual).isPresent();
    assertThat(actual.get().getValues())
        .isEqualTo(
            ImmutableMap.<String, Value<?>>builder()
                .put(ANY_NAME_1, new TextValue(ANY_NAME_1, ANY_TEXT_1))
                .put(ANY_NAME_2, new TextValue(ANY_NAME_2, ANY_TEXT_2))
                .put(ANY_NAME_3, new TextValue(ANY_NAME_3, ANY_TEXT_5))
                .build());
    assertThat(actual.get().getValue(ANY_NAME_1).get().getAsString().get()).isEqualTo(ANY_TEXT_1);
    assertThat(actual.get().getValue(ANY_NAME_2).get().getAsString().get()).isEqualTo(ANY_TEXT_2);
    assertThat(actual.get().getValue(ANY_NAME_3).get().getAsString().get()).isEqualTo(ANY_TEXT_5);
  }

  @Test
  public void get_KeyGivenContainedInReadSet_ShouldReturnFromReadSet() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Snapshot.Key key = new Snapshot.Key(prepareGet());
    TransactionResult result = prepareResult(ANY_ID);
    snapshot.put(key, Optional.of(result));

    // Act
    Optional<TransactionResult> actual = snapshot.get(key);

    // Assert
    assertThat(actual).isEqualTo(Optional.of(result));
  }

  @Test
  public void get_KeyGivenNotContainedInSnapshot_ShouldThrowIllegalArgumentException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Snapshot.Key key = new Snapshot.Key(prepareGet());

    // Act Assert
    assertThatThrownBy(() -> snapshot.get(key)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void get_KeyGivenContainedInWriteSet_ShouldThrowIllegalArgumentException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Put put = preparePut();
    Snapshot.Key key = new Snapshot.Key(put);
    snapshot.put(key, put);

    // Act Assert
    assertThatThrownBy(() -> snapshot.get(key)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void get_KeyGivenContainedInDeleteSet_ShouldReturnEmpty() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Delete delete = prepareDelete();
    Snapshot.Key key = new Snapshot.Key(delete);
    snapshot.put(key, delete);

    // Act
    Optional<TransactionResult> actual = snapshot.get(key);

    // Assert
    assertThat(actual).isNotPresent();
  }

  @Test
  public void get_KeyGivenContainedInReadSetAndDeleteSet_ShouldReturnEmpty() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Snapshot.Key key = new Snapshot.Key(prepareGet());
    TransactionResult result = prepareResult(ANY_ID);
    snapshot.put(key, Optional.of(result));

    Delete delete = prepareDelete();
    snapshot.put(key, delete);

    // Act
    Optional<TransactionResult> actual = snapshot.get(key);

    // Assert
    assertThat(actual).isNotPresent();
  }

  @Test
  public void get_ScanNotContainedInSnapshotGiven_ShouldReturnEmptyList() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Scan scan = prepareScan();

    // Act
    Optional<List<Snapshot.Key>> keys = snapshot.get(scan);

    // Assert
    assertThat(keys.isPresent()).isFalse();
  }

  @Test
  public void to_PrepareMutationComposerGivenAndSnapshotIsolationSet_ShouldCallComposerProperly()
      throws CommitConflictException, ExecutionException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Put put = preparePut();
    Delete delete = prepareAnotherDelete();
    TransactionResult result = prepareResult(ANY_ID);
    snapshot.put(new Snapshot.Key(prepareGet()), Optional.of(result));
    snapshot.put(new Snapshot.Key(prepareAnotherGet()), Optional.of(result));
    snapshot.put(new Snapshot.Key(put), put);
    snapshot.put(new Snapshot.Key(delete), delete);
    configureBehavior();

    // Act
    snapshot.to(prepareComposer);

    // Assert
    verify(prepareComposer).add(put, result);
    verify(prepareComposer).add(delete, result);
  }

  @Test
  public void
      to_PrepareMutationComposerGivenAndSerializableWithExtraWriteIsolationSet_ShouldCallComposerProperly()
          throws CommitConflictException, ExecutionException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    Put put = preparePut();
    TransactionResult result = prepareResult(ANY_ID);
    snapshot.put(new Snapshot.Key(prepareGet()), Optional.of(result));
    snapshot.put(new Snapshot.Key(prepareAnotherGet()), Optional.of(result));
    snapshot.put(new Snapshot.Key(put), put);
    configureBehavior();

    // Act
    snapshot.to(prepareComposer);

    // Assert
    Put putFromGet = prepareAnotherPut();
    verify(prepareComposer).add(put, result);
    verify(prepareComposer).add(putFromGet, result);
    verify(snapshot).toSerializableWithExtraWrite(prepareComposer);
  }

  @Test
  public void to_CommitMutationComposerGiven_ShouldCallComposerProperly()
      throws CommitConflictException, ExecutionException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Put put = preparePut();
    Delete delete = prepareAnotherDelete();
    TransactionResult result = prepareResult(ANY_ID);
    snapshot.put(new Snapshot.Key(prepareGet()), Optional.of(result));
    snapshot.put(new Snapshot.Key(prepareAnotherGet()), Optional.of(result));
    snapshot.put(new Snapshot.Key(put), put);
    snapshot.put(new Snapshot.Key(delete), delete);

    // Act
    snapshot.to(commitComposer);

    // Assert
    verify(commitComposer).add(put, result);
    verify(commitComposer).add(delete, result);
  }

  @Test
  public void
      to_CommitMutationComposerGivenAndSerializableWithExtraWriteIsolationSet_ShouldCallComposerProperly()
          throws CommitConflictException, ExecutionException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    Put put = preparePut();
    Delete delete = prepareAnotherDelete();
    TransactionResult result = prepareResult(ANY_ID);
    snapshot.put(new Snapshot.Key(prepareGet()), Optional.of(result));
    snapshot.put(new Snapshot.Key(prepareAnotherGet()), Optional.of(result));
    snapshot.put(new Snapshot.Key(put), put);
    snapshot.put(new Snapshot.Key(delete), delete);
    configureBehavior();

    // Act
    snapshot.to(commitComposer);

    // Assert
    // no effect on CommitMutationComposer
    verify(commitComposer).add(put, result);
    verify(commitComposer).add(delete, result);
    verify(snapshot).toSerializableWithExtraWrite(commitComposer);
  }

  @Test
  public void to_RollbackMutationComposerGiven_ShouldCallComposerProperly()
      throws CommitConflictException, ExecutionException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Put put = preparePut();
    Delete delete = prepareAnotherDelete();
    TransactionResult result = prepareResult(ANY_ID);
    snapshot.put(new Snapshot.Key(prepareGet()), Optional.of(result));
    snapshot.put(new Snapshot.Key(prepareAnotherGet()), Optional.of(result));
    snapshot.put(new Snapshot.Key(put), put);
    snapshot.put(new Snapshot.Key(delete), delete);
    configureBehavior();

    // Act
    snapshot.to(rollbackComposer);

    // Assert
    verify(rollbackComposer).add(put, result);
    verify(rollbackComposer).add(delete, result);
  }

  @Test
  public void
      to_RollbackMutationComposerGivenAndSerializableWithExtraWriteIsolationSet_ShouldCallComposerProperly()
          throws CommitConflictException, ExecutionException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    Put put = preparePut();
    Delete delete = prepareAnotherDelete();
    TransactionResult result = prepareResult(ANY_ID);
    snapshot.put(new Snapshot.Key(prepareGet()), Optional.of(result));
    snapshot.put(new Snapshot.Key(prepareAnotherGet()), Optional.of(result));
    snapshot.put(new Snapshot.Key(put), put);
    snapshot.put(new Snapshot.Key(delete), delete);
    configureBehavior();

    // Act
    snapshot.to(rollbackComposer);

    // Assert
    // no effect on RollbackMutationComposer
    verify(rollbackComposer).add(put, result);
    verify(rollbackComposer).add(delete, result);
    verify(snapshot).toSerializableWithExtraWrite(rollbackComposer);
  }

  @Test
  public void
      toSerializableWithExtraWrite_UnmutatedReadSetExists_ShouldConvertReadSetIntoWriteSet() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    Get get = prepareAnotherGet();
    Put put = preparePut();
    TransactionResult result = prepareResult(ANY_ID);
    TransactionResult txResult = new TransactionResult(result);
    snapshot.put(new Snapshot.Key(get), Optional.of(txResult));
    snapshot.put(new Snapshot.Key(put), put);

    // Act Assert
    assertThatCode(() -> snapshot.toSerializableWithExtraWrite(prepareComposer))
        .doesNotThrowAnyException();

    // Assert
    Put expected =
        new Put(get.getPartitionKey(), get.getClusteringKey().get())
            .withConsistency(Consistency.LINEARIZABLE)
            .forNamespace(get.forNamespace().get())
            .forTable(get.forTable().get());
    assertThat(writeSet).contains(entry(new Snapshot.Key(get), expected));
    assertThat(writeSet.size()).isEqualTo(2);
    verify(prepareComposer, never()).add(any(), any());
  }

  @Test
  public void
      toSerializableWithExtraWrite_UnmutatedReadSetForNonExistingExists_ShouldThrowCommitConflictException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    Get get = prepareAnotherGet();
    Put put = preparePut();
    snapshot.put(new Snapshot.Key(get), Optional.empty());
    snapshot.put(new Snapshot.Key(put), put);

    // Act Assert
    Throwable thrown = catchThrowable(() -> snapshot.toSerializableWithExtraWrite(prepareComposer));

    // Assert
    assertThat(thrown).doesNotThrowAnyException();
    Get expected =
        new Get(get.getPartitionKey(), get.getClusteringKey().get())
            .withConsistency(Consistency.LINEARIZABLE)
            .forNamespace(get.forNamespace().get())
            .forTable(get.forTable().get());
    verify(prepareComposer).add(expected, null);
  }

  @Test
  public void
      toSerializableWithExtraWrite_ScanSetAndWriteSetExist_ShouldThrowCommitConflictException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_WRITE);
    Scan scan = prepareScan();
    Snapshot.Key key = new Snapshot.Key(scan, prepareResult(ANY_ID));
    Put put = preparePut();
    snapshot.put(scan, Collections.singletonList(key));
    snapshot.put(new Snapshot.Key(put), put);

    // Act Assert
    Throwable thrown = catchThrowable(() -> snapshot.toSerializableWithExtraWrite(prepareComposer));

    // Assert
    assertThat(thrown).isInstanceOf(CommitConflictException.class);
  }

  @Test
  public void toSerializableWithExtraRead_ReadSetNotChanged_ShouldProcessWithoutExceptions()
      throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    Get get = prepareAnotherGet();
    Put put = preparePut();
    TransactionResult result = prepareResult(ANY_ID);
    TransactionResult txResult = new TransactionResult(result);
    snapshot.put(new Snapshot.Key(get), Optional.of(txResult));
    snapshot.put(new Snapshot.Key(put), put);
    DistributedStorage storage = mock(DistributedStorage.class);
    Get getWithProjections =
        prepareAnotherGet().withProjection(Attribute.ID).withProjection(Attribute.VERSION);
    when(storage.get(getWithProjections)).thenReturn(Optional.of(txResult));

    // Act Assert
    assertThatCode(() -> snapshot.toSerializableWithExtraRead(storage)).doesNotThrowAnyException();

    // Assert
    verify(storage).get(getWithProjections);
  }

  @Test
  public void toSerializableWithExtraRead_ReadSetUpdated_ShouldThrowCommitConflictException()
      throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    Get get = prepareAnotherGet();
    Put put = preparePut();
    TransactionResult txResult = prepareResult(ANY_ID);
    snapshot.put(new Snapshot.Key(get), Optional.of(txResult));
    snapshot.put(new Snapshot.Key(put), put);
    DistributedStorage storage = mock(DistributedStorage.class);
    TransactionResult changedTxResult = prepareResult(ANY_ID + "x");
    Get getWithProjections =
        prepareAnotherGet().withProjection(Attribute.ID).withProjection(Attribute.VERSION);
    when(storage.get(getWithProjections)).thenReturn(Optional.of(changedTxResult));

    // Act Assert
    assertThatThrownBy(() -> snapshot.toSerializableWithExtraRead(storage))
        .isInstanceOf(CommitConflictException.class);

    // Assert
    verify(storage).get(getWithProjections);
  }

  @Test
  public void toSerializableWithExtraRead_ReadSetExtended_ShouldThrowCommitConflictException()
      throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    Get get = prepareAnotherGet();
    Put put = preparePut();
    snapshot.put(new Snapshot.Key(get), Optional.empty());
    snapshot.put(new Snapshot.Key(put), put);
    DistributedStorage storage = mock(DistributedStorage.class);
    TransactionResult txResult = prepareResult(ANY_ID);
    Get getWithProjections =
        prepareAnotherGet().withProjection(Attribute.ID).withProjection(Attribute.VERSION);
    when(storage.get(getWithProjections)).thenReturn(Optional.of(txResult));

    // Act Assert
    assertThatThrownBy(() -> snapshot.toSerializableWithExtraRead(storage))
        .isInstanceOf(CommitConflictException.class);

    // Assert
    verify(storage).get(getWithProjections);
  }

  @Test
  public void toSerializableWithExtraRead_ScanSetNotChanged_ShouldProcessWithoutExceptions()
      throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    Scan scan = prepareScan();
    Put put = preparePut();
    TransactionResult txResult = prepareResult(ANY_ID);
    Snapshot.Key key = new Snapshot.Key(scan, txResult);
    snapshot.put(key, Optional.of(txResult));
    snapshot.put(scan, Collections.singletonList(key));
    snapshot.put(new Snapshot.Key(put), put);
    DistributedStorage storage = mock(DistributedStorage.class);
    Scanner scanner = mock(Scanner.class);
    when(scanner.iterator()).thenReturn(Collections.singletonList((Result) txResult).iterator());
    Scan scanWithProjections =
        prepareScan().withProjection(Attribute.ID).withProjection(Attribute.VERSION);
    when(storage.scan(scanWithProjections)).thenReturn(scanner);

    // Act Assert
    assertThatCode(() -> snapshot.toSerializableWithExtraRead(storage)).doesNotThrowAnyException();

    // Assert
    verify(storage).scan(scanWithProjections);
  }

  @Test
  public void toSerializableWithExtraRead_ScanSetUpdated_ShouldProcessWithoutExceptions()
      throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    Scan scan = prepareScan();
    Put put = preparePut();
    TransactionResult txResult = prepareResult(ANY_ID);
    Snapshot.Key key = new Snapshot.Key(scan, txResult);
    snapshot.put(key, Optional.of(txResult));
    snapshot.put(scan, Collections.singletonList(key));
    snapshot.put(new Snapshot.Key(put), put);
    DistributedStorage storage = mock(DistributedStorage.class);
    TransactionResult changedTxResult = prepareResult(ANY_ID + "x");
    Scanner scanner = mock(Scanner.class);
    when(scanner.iterator())
        .thenReturn(Collections.singletonList((Result) changedTxResult).iterator());
    Scan scanWithProjections =
        prepareScan().withProjection(Attribute.ID).withProjection(Attribute.VERSION);
    when(storage.scan(scanWithProjections)).thenReturn(scanner);

    // Act Assert
    assertThatThrownBy(() -> snapshot.toSerializableWithExtraRead(storage))
        .isInstanceOf(CommitConflictException.class);

    // Assert
    verify(storage).scan(scanWithProjections);
  }

  @Test
  public void toSerializableWithExtraRead_ScanSetExtended_ShouldProcessWithoutExceptions()
      throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);
    Scan scan = prepareScan();
    Put put = preparePut();
    TransactionResult result = prepareResult(ANY_ID + "x");
    snapshot.put(scan, Collections.emptyList());
    snapshot.put(new Snapshot.Key(put), put);
    DistributedStorage storage = mock(DistributedStorage.class);
    TransactionResult txResult = new TransactionResult(result);
    Scanner scanner = mock(Scanner.class);
    when(scanner.iterator()).thenReturn(Collections.singletonList((Result) txResult).iterator());
    Scan scanWithProjections =
        prepareScan().withProjection(Attribute.ID).withProjection(Attribute.VERSION);
    when(storage.scan(scanWithProjections)).thenReturn(scanner);

    // Act Assert
    assertThatThrownBy(() -> snapshot.toSerializableWithExtraRead(storage))
        .isInstanceOf(CommitConflictException.class);

    // Assert
    verify(storage).scan(scanWithProjections);
  }

  @Test
  public void
      toSerializableWithExtraRead_MultipleScansInScanSetExist_ShouldThrowCommitConflictException()
          throws ExecutionException {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SERIALIZABLE, SerializableStrategy.EXTRA_READ);

    Scan scan1 =
        new Scan(new Key(ANY_NAME_1, ANY_TEXT_1))
            .withStart(new Key(ANY_NAME_2, ANY_TEXT_2))
            .withConsistency(Consistency.LINEARIZABLE)
            .forNamespace(ANY_NAMESPACE_NAME)
            .forTable(ANY_TABLE_NAME);
    Scan scan2 =
        new Scan(new Key(ANY_NAME_1, ANY_TEXT_2))
            .withStart(new Key(ANY_NAME_2, ANY_TEXT_1))
            .withConsistency(Consistency.LINEARIZABLE)
            .forNamespace(ANY_NAMESPACE_NAME)
            .forTable(ANY_TABLE_NAME);

    Result result1 = mock(TransactionResult.class);
    when(result1.getPartitionKey())
        .thenReturn(Optional.of(new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1))));
    when(result1.getClusteringKey())
        .thenReturn(Optional.of(new Key(new TextValue(ANY_NAME_2, ANY_TEXT_2))));
    when(result1.getValues())
        .thenReturn(
            ImmutableMap.of(
                ANY_NAME_1,
                new TextValue(ANY_NAME_1, ANY_TEXT_1),
                ANY_NAME_2,
                new TextValue(ANY_NAME_2, ANY_TEXT_2),
                Attribute.ID,
                Attribute.toIdValue("id1"),
                Attribute.VERSION,
                Attribute.toVersionValue(ANY_VERSION)));

    Result result2 = mock(TransactionResult.class);
    when(result2.getPartitionKey())
        .thenReturn(Optional.of(new Key(new TextValue(ANY_NAME_1, ANY_TEXT_2))));
    when(result2.getClusteringKey())
        .thenReturn(Optional.of(new Key(new TextValue(ANY_NAME_2, ANY_TEXT_1))));
    when(result2.getValues())
        .thenReturn(
            ImmutableMap.of(
                ANY_NAME_1,
                new TextValue(ANY_NAME_1, ANY_TEXT_2),
                ANY_NAME_2,
                new TextValue(ANY_NAME_2, ANY_TEXT_1),
                Attribute.ID,
                Attribute.toIdValue("id2"),
                Attribute.VERSION,
                Attribute.toVersionValue(ANY_VERSION)));

    Snapshot.Key key1 = new Snapshot.Key(scan1, result1);
    Snapshot.Key key2 = new Snapshot.Key(scan2, result2);

    snapshot.put(scan1, Collections.singletonList(key1));
    snapshot.put(scan2, Collections.singletonList(key2));
    snapshot.put(key1, Optional.of(new TransactionResult(result1)));
    snapshot.put(key2, Optional.of(new TransactionResult(result2)));

    DistributedStorage storage = mock(DistributedStorage.class);

    Scanner scanner1 = mock(Scanner.class);
    when(scanner1.iterator()).thenReturn(Collections.singletonList(result1).iterator());
    Scan scan1WithProjections =
        new Scan(new Key(ANY_NAME_1, ANY_TEXT_1))
            .withStart(new Key(ANY_NAME_2, ANY_TEXT_2))
            .withConsistency(Consistency.LINEARIZABLE)
            .forNamespace(ANY_NAMESPACE_NAME)
            .forTable(ANY_TABLE_NAME)
            .withProjection(Attribute.ID)
            .withProjection(Attribute.VERSION);
    when(storage.scan(scan1WithProjections)).thenReturn(scanner1);

    Scanner scanner2 = mock(Scanner.class);
    when(scanner2.iterator()).thenReturn(Collections.singletonList(result2).iterator());
    Scan scan2WithProjections =
        new Scan(new Key(ANY_NAME_1, ANY_TEXT_2))
            .withStart(new Key(ANY_NAME_2, ANY_TEXT_1))
            .withConsistency(Consistency.LINEARIZABLE)
            .forNamespace(ANY_NAMESPACE_NAME)
            .forTable(ANY_TABLE_NAME)
            .withProjection(Attribute.ID)
            .withProjection(Attribute.VERSION);
    when(storage.scan(scan2WithProjections)).thenReturn(scanner2);

    // Act Assert
    assertThatCode(() -> snapshot.toSerializableWithExtraRead(storage)).doesNotThrowAnyException();
  }

  @Test
  public void put_DeleteGivenAfterPut_PutSupercedesDelete() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(preparePut());
    snapshot.put(putKey, put);

    Delete delete = prepareDelete();
    Snapshot.Key deleteKey = new Snapshot.Key(prepareDelete());

    // Act
    snapshot.put(deleteKey, delete);

    // Assert
    assertThat(writeSet.size()).isEqualTo(0);
    assertThat(deleteSet.size()).isEqualTo(1);
    assertThat(deleteSet.get(deleteKey)).isEqualTo(delete);
  }

  @Test
  public void put_PutGivenAfterDelete_ShouldThrowIllegalArgumentException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Delete delete = prepareDelete();
    Snapshot.Key deleteKey = new Snapshot.Key(prepareDelete());
    snapshot.put(deleteKey, delete);

    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(preparePut());

    // Act Assert
    assertThatThrownBy(() -> snapshot.put(putKey, put))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void get_ScanGivenAndPutInWriteSetNotOverlappedWithScan_ShouldNotThrowException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    // "text2"
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(put);
    snapshot.put(putKey, put);
    Scan scan =
        new Scan(new Key(ANY_NAME_1, ANY_TEXT_1))
            // ["text3", "text4"]
            .withStart(new Key(ANY_NAME_2, ANY_TEXT_3), true)
            .withEnd(new Key(ANY_NAME_2, ANY_TEXT_4), true)
            .withConsistency(Consistency.LINEARIZABLE)
            .forNamespace(ANY_NAMESPACE_NAME)
            .forTable(ANY_TABLE_NAME);

    // Act Assert
    Throwable thrown = catchThrowable(() -> snapshot.get(scan));

    // Assert
    assertThat(thrown).doesNotThrowAnyException();
  }

  @Test
  public void
      get_ScanGivenAndPutWithSamePartitionKeyWithoutClusteringKeyInWriteSet_ShouldThrowIllegalArgumentException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    Put put = preparePutWithPartitionKeyOnly();
    Snapshot.Key putKey = new Snapshot.Key(put);
    snapshot.put(putKey, put);
    Scan scan = prepareScan();

    // Act Assert
    Throwable thrown = catchThrowable(() -> snapshot.get(scan));

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      get_ScanWithNoRangeGivenAndPutInWriteSetOverlappedWithScan_ShouldThrowIllegalArgumentException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    // "text2"
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(put);
    snapshot.put(putKey, put);
    Scan scan =
        new Scan(new Key(ANY_NAME_1, ANY_TEXT_1))
            // (-infinite, infinite)
            .withConsistency(Consistency.LINEARIZABLE)
            .forNamespace(ANY_NAMESPACE_NAME)
            .forTable(ANY_TABLE_NAME);

    // Act Assert
    Throwable thrown = catchThrowable(() -> snapshot.get(scan));

    // Assert
    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      get_ScanWithRangeGivenAndPutInWriteSetOverlappedWithScan_ShouldThrowIllegalArgumentException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    // "text2"
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(put);
    snapshot.put(putKey, put);
    Scan scan1 =
        prepareScan()
            // ["text1", "text3"]
            .withStart(new Key(ANY_NAME_2, ANY_TEXT_1), true)
            .withEnd(new Key(ANY_NAME_2, ANY_TEXT_3), true);
    Scan scan2 =
        prepareScan()
            // ["text2", "text3"]
            .withStart(new Key(ANY_NAME_2, ANY_TEXT_2), true)
            .withEnd(new Key(ANY_NAME_2, ANY_TEXT_3), true);
    Scan scan3 =
        prepareScan()
            // ["text1", "text2"]
            .withStart(new Key(ANY_NAME_2, ANY_TEXT_1), true)
            .withEnd(new Key(ANY_NAME_2, ANY_TEXT_2), true);
    Scan scan4 =
        prepareScan()
            // ("text2", "text3"]
            .withStart(new Key(ANY_NAME_2, ANY_TEXT_2), false)
            .withEnd(new Key(ANY_NAME_2, ANY_TEXT_3), true);
    Scan scan5 =
        prepareScan()
            // ["text1", "text2")
            .withStart(new Key(ANY_NAME_2, ANY_TEXT_1), true)
            .withEnd(new Key(ANY_NAME_2, ANY_TEXT_2), false);

    // Act Assert
    Throwable thrown1 = catchThrowable(() -> snapshot.get(scan1));
    Throwable thrown2 = catchThrowable(() -> snapshot.get(scan2));
    Throwable thrown3 = catchThrowable(() -> snapshot.get(scan3));
    Throwable thrown4 = catchThrowable(() -> snapshot.get(scan4));
    Throwable thrown5 = catchThrowable(() -> snapshot.get(scan5));

    // Assert
    assertThat(thrown1).isInstanceOf(IllegalArgumentException.class);
    assertThat(thrown2).isInstanceOf(IllegalArgumentException.class);
    assertThat(thrown3).isInstanceOf(IllegalArgumentException.class);
    assertThat(thrown4).doesNotThrowAnyException();
    assertThat(thrown5).doesNotThrowAnyException();
  }

  @Test
  public void
      get_ScanWithEndSideInfiniteRangeGivenAndPutInWriteSetOverlappedWithScan_ShouldThrowIllegalArgumentException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    // "text2"
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(put);
    snapshot.put(putKey, put);
    Scan scan1 =
        new Scan(new Key(ANY_NAME_1, ANY_TEXT_1))
            // (-infinite, "text3"]
            .withEnd(new Key(ANY_NAME_2, ANY_TEXT_3), true)
            .withConsistency(Consistency.LINEARIZABLE)
            .forNamespace(ANY_NAMESPACE_NAME)
            .forTable(ANY_TABLE_NAME);
    Scan scan2 =
        new Scan(new Key(ANY_NAME_1, ANY_TEXT_1))
            // (-infinite, "text2"]
            .withEnd(new Key(ANY_NAME_2, ANY_TEXT_2), true)
            .withConsistency(Consistency.LINEARIZABLE)
            .forNamespace(ANY_NAMESPACE_NAME)
            .forTable(ANY_TABLE_NAME);
    Scan scan3 =
        new Scan(new Key(ANY_NAME_1, ANY_TEXT_1))
            // (-infinite, "text2")
            .withEnd(new Key(ANY_NAME_2, ANY_TEXT_2), false)
            .withConsistency(Consistency.LINEARIZABLE)
            .forNamespace(ANY_NAMESPACE_NAME)
            .forTable(ANY_TABLE_NAME);

    // Act Assert
    Throwable thrown1 = catchThrowable(() -> snapshot.get(scan1));
    Throwable thrown2 = catchThrowable(() -> snapshot.get(scan2));
    Throwable thrown3 = catchThrowable(() -> snapshot.get(scan3));

    // Assert
    assertThat(thrown1).isInstanceOf(IllegalArgumentException.class);
    assertThat(thrown2).isInstanceOf(IllegalArgumentException.class);
    assertThat(thrown3).doesNotThrowAnyException();
  }

  @Test
  public void
      get_ScanWithStartSideInfiniteRangeGivenAndPutInWriteSetOverlappedWithScan_ShouldThrowIllegalArgumentException() {
    // Arrange
    snapshot = prepareSnapshot(Isolation.SNAPSHOT);
    // "text2"
    Put put = preparePut();
    Snapshot.Key putKey = new Snapshot.Key(put);
    snapshot.put(putKey, put);
    Scan scan1 =
        new Scan(new Key(ANY_NAME_1, ANY_TEXT_1))
            // ["text1", infinite)
            .withStart(new Key(ANY_NAME_2, ANY_TEXT_1), true)
            .withConsistency(Consistency.LINEARIZABLE)
            .forNamespace(ANY_NAMESPACE_NAME)
            .forTable(ANY_TABLE_NAME);
    Scan scan2 =
        new Scan(new Key(ANY_NAME_1, ANY_TEXT_1))
            // ["text2", infinite)
            .withStart(new Key(ANY_NAME_2, ANY_TEXT_2), true)
            .withConsistency(Consistency.LINEARIZABLE)
            .forNamespace(ANY_NAMESPACE_NAME)
            .forTable(ANY_TABLE_NAME);
    Scan scan3 =
        new Scan(new Key(ANY_NAME_1, ANY_TEXT_1))
            // ("text2", infinite)
            .withStart(new Key(ANY_NAME_2, ANY_TEXT_2), false)
            .withConsistency(Consistency.LINEARIZABLE)
            .forNamespace(ANY_NAMESPACE_NAME)
            .forTable(ANY_TABLE_NAME);

    // Act Assert
    Throwable thrown1 = catchThrowable(() -> snapshot.get(scan1));
    Throwable thrown2 = catchThrowable(() -> snapshot.get(scan2));
    Throwable thrown3 = catchThrowable(() -> snapshot.get(scan3));

    // Assert
    assertThat(thrown1).isInstanceOf(IllegalArgumentException.class);
    assertThat(thrown2).isInstanceOf(IllegalArgumentException.class);
    assertThat(thrown3).doesNotThrowAnyException();
  }
}
