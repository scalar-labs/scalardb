package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
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
  @Mock private Get get;
  @Mock private Scan scan;
  @Mock private Put put;
  @Mock private Delete delete;
  @Mock private Snapshot snapshot;
  @Mock private CrudHandler crud;
  @Mock private CommitHandler commit;
  @InjectMocks private ConsensusCommit consensus;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    when(get.forNamespace()).thenReturn(Optional.of(ANY_NAMESPACE));
    when(get.forTable()).thenReturn(Optional.of(ANY_TABLE_NAME));
    when(scan.forNamespace()).thenReturn(Optional.of(ANY_NAMESPACE));
    when(scan.forTable()).thenReturn(Optional.of(ANY_TABLE_NAME));
    when(put.forNamespace()).thenReturn(Optional.of(ANY_NAMESPACE));
    when(put.forTable()).thenReturn(Optional.of(ANY_TABLE_NAME));
    when(delete.forNamespace()).thenReturn(Optional.of(ANY_NAMESPACE));
    when(delete.forTable()).thenReturn(Optional.of(ANY_TABLE_NAME));
  }

  @Test
  public void get_GetGiven_ShouldCallCrudHandlerGet() throws CrudException {
    // Arrange
    TransactionResult result = mock(TransactionResult.class);
    when(crud.get(get)).thenReturn(Optional.of(result));

    // Act Assert
    Optional<Result> actual = consensus.get(get);

    // Assert
    assertThat(actual).isPresent();
    verify(crud).get(get);
  }

  @Test
  public void scan_ScanGiven_ShouldCallCrudHandlerScan() throws CrudException {
    // Arrange
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
    doNothing().when(crud).put(put);

    // Act Assert
    consensus.put(put);

    // Assert
    verify(crud).put(put);
  }

  @Test
  public void put_TwoPutsGiven_ShouldCallCrudHandlerPutTwice() {
    // Arrange
    doNothing().when(crud).put(put);

    // Act Assert
    consensus.put(Arrays.asList(put, put));

    // Assert
    verify(crud, times(2)).put(put);
  }

  @Test
  public void delete_DeleteGiven_ShouldCallCrudHandlerDelete() {
    // Arrange
    doNothing().when(crud).delete(delete);

    // Act Assert
    consensus.delete(delete);

    // Assert
    verify(crud).delete(delete);
  }

  @Test
  public void delete_TwoDeletesGiven_ShouldCallCrudHandlerDeleteTwice() {
    // Arrange
    doNothing().when(crud).delete(delete);

    // Act Assert
    consensus.delete(Arrays.asList(delete, delete));

    // Assert
    verify(crud, times(2)).delete(delete);
  }

  @Test
  public void mutate_PutAndDeleteGiven_ShouldCallCrudHandlerPutAndDelete() {
    // Arrange
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
