package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.io.Key;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ConsensusCommitTest {
  private static final String ANY_NAMESPACE = "namespace";
  private static final String ANY_TABLE_NAME = "table";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_TEXT_3 = "text3";

  @Mock private Snapshot snapshot;
  @Mock private CrudHandler crud;
  @Mock private CommitHandler commit;
  @Mock private RecoveryHandler recovery;
  @InjectMocks private ConsensusCommit consensus;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  private Get prepareGet() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Get(partitionKey, clusteringKey)
        .forNamespace(ANY_NAMESPACE)
        .forTable(ANY_TABLE_NAME);
  }

  private Scan prepareScan() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    return new Scan(partitionKey).forNamespace(ANY_NAMESPACE).forTable(ANY_TABLE_NAME);
  }

  private Put preparePut() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Put(partitionKey, clusteringKey)
        .withValue(ANY_NAME_3, ANY_TEXT_3)
        .forNamespace(ANY_NAMESPACE)
        .forTable(ANY_TABLE_NAME);
  }

  private Delete prepareDelete() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Delete(partitionKey, clusteringKey)
        .forNamespace(ANY_NAMESPACE)
        .forTable(ANY_TABLE_NAME);
  }

  @Test
  public void get_GetGiven_ShouldCallCrudHandlerGet() throws CrudException {
    // Arrange
    Get get = prepareGet();
    TransactionResult result = mock(TransactionResult.class);
    when(crud.get(get)).thenReturn(Optional.of(result));

    // Act Assert
    Optional<Result> actual = consensus.get(get);

    // Assert
    assertThat(actual).isPresent();
    verify(recovery, never()).recover(get, result);
    verify(crud).get(get);
  }

  @Test
  public void get_GetForUncommittedRecordGiven_ShouldRecoverRecord() throws CrudException {
    // Arrange
    Get get = prepareGet();
    TransactionResult result = mock(TransactionResult.class);
    UncommittedRecordException toThrow = mock(UncommittedRecordException.class);
    when(crud.get(get)).thenThrow(toThrow);
    when(toThrow.getResults()).thenReturn(Collections.singletonList(result));

    // Act Assert
    assertThatThrownBy(() -> consensus.get(get)).isInstanceOf(UncommittedRecordException.class);

    // Assert
    verify(recovery).recover(get, result);
  }

  @Test
  public void scan_ScanGiven_ShouldCallCrudHandlerScan() throws CrudException {
    // Arrange
    Scan scan = prepareScan();
    TransactionResult result = mock(TransactionResult.class);
    List<Result> results = Collections.singletonList(result);
    when(crud.scan(scan)).thenReturn(results);

    // Act Assert
    List<Result> actual = consensus.scan(scan);

    // Assert
    assertThat(actual.size()).isEqualTo(1);
    verify(crud).scan(scan);
  }

  @Test
  public void put_PutGiven_ShouldCallCrudHandlerPut() {
    // Arrange
    Put put = preparePut();
    doNothing().when(crud).put(put);

    // Act Assert
    consensus.put(put);

    // Assert
    verify(crud).put(put);
  }

  @Test
  public void put_TwoPutsGiven_ShouldCallCrudHandlerPutTwice() {
    // Arrange
    Put put = preparePut();
    doNothing().when(crud).put(put);

    // Act Assert
    consensus.put(Arrays.asList(put, put));

    // Assert
    verify(crud, times(2)).put(put);
  }

  @Test
  public void delete_DeleteGiven_ShouldCallCrudHandlerDelete() {
    // Arrange
    Delete delete = prepareDelete();
    doNothing().when(crud).delete(delete);

    // Act Assert
    consensus.delete(delete);

    // Assert
    verify(crud).delete(delete);
  }

  @Test
  public void delete_TwoDeletesGiven_ShouldCallCrudHandlerDeleteTwice() {
    // Arrange
    Delete delete = prepareDelete();
    doNothing().when(crud).delete(delete);

    // Act Assert
    consensus.delete(Arrays.asList(delete, delete));

    // Assert
    verify(crud, times(2)).delete(delete);
  }

  @Test
  public void mutate_PutAndDeleteGiven_ShouldCallCrudHandlerPutAndDelete() {
    // Arrange
    Put put = preparePut();
    Delete delete = prepareDelete();
    doNothing().when(crud).put(put);
    doNothing().when(crud).delete(delete);

    // Act Assert
    consensus.mutate(Arrays.asList(put, delete));

    // Assert
    verify(crud).put(put);
    verify(crud).delete(delete);
  }

  @Test
  public void commit_ProcessedCrudGiven_ShouldCommitWithSnapshot()
      throws CommitException, UnknownTransactionStatusException {
    // Arrange
    doNothing().when(commit).commit(any(Snapshot.class));
    when(crud.getSnapshot()).thenReturn(snapshot);

    // Act Assert
    consensus.commit();

    // Assert
    verify(commit).commit(snapshot);
  }
}
