package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.transaction.consensuscommit.ParallelExecutor.ParallelExecutorTask;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ParallelExecutorTest {

  private static final String TX_ID = "id";

  @Mock private ConsensusCommitConfig config;
  @Mock private ParallelExecutorTask task;

  private ExecutorService parallelExecutorService;
  private ParallelExecutor parallelExecutor;
  private List<ParallelExecutorTask> tasks;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    tasks = Arrays.asList(task, task, task);
    parallelExecutorService = spy(Executors.newFixedThreadPool(tasks.size()));
    parallelExecutor = new ParallelExecutor(config, parallelExecutorService);
  }

  @AfterEach
  protected void afterEach() throws Exception {
    parallelExecutorService.shutdown();
    if (!parallelExecutorService.awaitTermination(10, TimeUnit.SECONDS)) {
      parallelExecutorService.shutdownNow();
    }
  }

  @Test
  public void prepare_ParallelPreparationNotEnabled_ShouldExecuteTasksSerially()
      throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelPreparationEnabled()).thenReturn(false);

    // Act
    parallelExecutor.prepare(tasks, TX_ID);

    // Assert
    verify(task, times(tasks.size())).run();
    verify(parallelExecutorService, never()).execute(any());
  }

  @Test
  public void
      prepare_ParallelPreparationNotEnabled_ExecutionExceptionThrownByTask_ShouldStopRunningTasks()
          throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelPreparationEnabled()).thenReturn(false);
    doThrow(ExecutionException.class).when(task).run();

    // Act Assert
    assertThatThrownBy(() -> parallelExecutor.prepare(tasks, TX_ID))
        .isInstanceOf(ExecutionException.class);

    verify(task, only()).run();
    verify(parallelExecutorService, never()).execute(any());
  }

  @Test
  public void prepare_ParallelPreparationEnabled_ShouldExecuteTasksInParallel()
      throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelPreparationEnabled()).thenReturn(true);

    // Act
    parallelExecutor.prepare(tasks, TX_ID);

    // Assert
    verify(task, times(tasks.size())).run();
    verify(parallelExecutorService, times(tasks.size())).execute(any());
  }

  @Test
  public void
      prepare_ParallelPreparationEnabled_ExecutionExceptionThrownByTask_ShouldNotStopRunningTasks()
          throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelPreparationEnabled()).thenReturn(true);
    doThrow(ExecutionException.class).when(task).run();

    // Act Assert
    assertThatThrownBy(() -> parallelExecutor.prepare(tasks, TX_ID))
        .isInstanceOf(ExecutionException.class);

    verify(task, times(tasks.size())).run();
    verify(parallelExecutorService, times(tasks.size())).execute(any());
  }

  @Test
  public void validate_ParallelValidationNotEnabled_ShouldExecuteTasksSerially()
      throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelValidationEnabled()).thenReturn(false);

    // Act
    parallelExecutor.validate(tasks, TX_ID);

    // Assert
    verify(task, times(tasks.size())).run();
    verify(parallelExecutorService, never()).execute(any());
  }

  @Test
  public void
      validate_ParallelValidationNotEnabled_ExecutionExceptionThrownByTask_ShouldStopRunningTasks()
          throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelValidationEnabled()).thenReturn(false);
    doThrow(ExecutionException.class).when(task).run();

    // Act Assert
    assertThatThrownBy(() -> parallelExecutor.validate(tasks, TX_ID))
        .isInstanceOf(ExecutionException.class);

    verify(task, only()).run();
    verify(parallelExecutorService, never()).execute(any());
  }

  @Test
  public void
      validate_ParallelValidationNotEnabled_ValidationConflictExceptionThrownByTask_ShouldStopRunningTasks()
          throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelValidationEnabled()).thenReturn(false);
    doThrow(ValidationConflictException.class).when(task).run();

    // Act Assert
    assertThatThrownBy(() -> parallelExecutor.validate(tasks, TX_ID))
        .isInstanceOf(ValidationConflictException.class);

    verify(task, only()).run();
    verify(parallelExecutorService, never()).execute(any());
  }

  @Test
  public void validate_ParallelValidationEnabled_ShouldExecuteTasksInParallel()
      throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelValidationEnabled()).thenReturn(true);

    // Act
    parallelExecutor.validate(tasks, TX_ID);

    // Assert
    verify(task, times(tasks.size())).run();
    verify(parallelExecutorService, times(tasks.size())).execute(any());
  }

  @Test
  public void
      validate_ParallelValidationEnabled_ExecutionExceptionThrownByTask_ShouldStopRunningTasks()
          throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelValidationEnabled()).thenReturn(true);
    doThrow(ExecutionException.class).when(task).run();

    // Act Assert
    assertThatThrownBy(() -> parallelExecutor.validate(tasks, TX_ID))
        .isInstanceOf(ExecutionException.class);

    verify(task, atMost(tasks.size())).run();
    verify(parallelExecutorService, times(tasks.size())).execute(any());
  }

  @Test
  public void
      validate_ParallelValidationEnabled_ValidationConflictExceptionThrownByTask_ShouldStopRunningTasks()
          throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelValidationEnabled()).thenReturn(true);
    doThrow(ValidationConflictException.class).when(task).run();

    // Act Assert
    assertThatThrownBy(() -> parallelExecutor.validate(tasks, TX_ID))
        .isInstanceOf(ValidationConflictException.class);

    verify(task, atMost(tasks.size())).run();
    verify(parallelExecutorService, times(tasks.size())).execute(any());
  }

  @Test
  public void commitRecords_ParallelCommitNotEnabled_ShouldExecuteTasksSerially()
      throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelCommitEnabled()).thenReturn(false);

    // Act
    parallelExecutor.commitRecords(tasks, TX_ID);

    // Assert
    verify(task, times(tasks.size())).run();
    verify(parallelExecutorService, never()).execute(any());
  }

  @Test
  public void
      commitRecords_ParallelCommitNotEnabled_ExecutionExceptionThrownByTask_ShouldNotStopRunningTasks()
          throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelCommitEnabled()).thenReturn(false);
    doThrow(ExecutionException.class).when(task).run();

    // Act Assert
    assertThatThrownBy(() -> parallelExecutor.commitRecords(tasks, TX_ID))
        .isInstanceOf(ExecutionException.class);

    verify(task, times(tasks.size())).run();
    verify(parallelExecutorService, never()).execute(any());
  }

  @Test
  public void
      commitRecords_ParallelCommitEnabledAndAsyncCommitNotEnabled_ShouldExecuteTasksInParallel()
          throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelCommitEnabled()).thenReturn(true);
    when(config.isAsyncCommitEnabled()).thenReturn(false);

    // Act
    parallelExecutor.commitRecords(tasks, TX_ID);

    // Assert
    verify(task, times(tasks.size())).run();
    verify(parallelExecutorService, times(tasks.size())).execute(any());
  }

  @Test
  public void
      commitRecords_ParallelCommitEnabledAndAsyncCommitNotEnabled_ExecutionExceptionThrownByTask_ShouldNotStopRunningTasks()
          throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelCommitEnabled()).thenReturn(true);
    when(config.isAsyncCommitEnabled()).thenReturn(false);
    doThrow(ExecutionException.class).when(task).run();

    // Act Assert
    assertThatThrownBy(() -> parallelExecutor.commitRecords(tasks, TX_ID))
        .isInstanceOf(ExecutionException.class);

    verify(task, times(tasks.size())).run();
    verify(parallelExecutorService, times(tasks.size())).execute(any());
  }

  @Test
  public void
      commitRecords_ParallelCommitEnabledAndAsyncCommitEnabled_ShouldExecuteTasksInParallelAndAsynchronously()
          throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelCommitEnabled()).thenReturn(true);
    when(config.isAsyncCommitEnabled()).thenReturn(true);

    // Act
    parallelExecutor.commitRecords(tasks, TX_ID);

    // Assert
    verify(task, atMost(tasks.size())).run();
    verify(parallelExecutorService, times(tasks.size())).execute(any());
  }

  @Test
  public void
      commitRecords_ParallelCommitEnabledAndAsyncCommitEnabled_ExecutionExceptionThrownByTask_ShouldNotStopRunningTasks()
          throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelCommitEnabled()).thenReturn(true);
    when(config.isAsyncCommitEnabled()).thenReturn(true);
    doThrow(ExecutionException.class).when(task).run();

    // Act Assert
    assertThatCode(() -> parallelExecutor.commitRecords(tasks, TX_ID)).doesNotThrowAnyException();

    verify(task, atMost(tasks.size())).run();
    verify(parallelExecutorService, times(tasks.size())).execute(any());
  }

  @Test
  public void rollbackRecords_ParallelRollbackNotEnabled_ShouldExecuteTasksSerially()
      throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelRollbackEnabled()).thenReturn(false);

    // Act
    parallelExecutor.rollbackRecords(tasks, TX_ID);

    // Assert
    verify(task, times(tasks.size())).run();
    verify(parallelExecutorService, never()).execute(any());
  }

  @Test
  public void
      rollbackRecords_ParallelRollbackNotEnabled_ExecutionExceptionThrownByTask_ShouldNotStopRunningTasks()
          throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelRollbackEnabled()).thenReturn(false);
    doThrow(ExecutionException.class).when(task).run();

    // Act Assert
    assertThatThrownBy(() -> parallelExecutor.rollbackRecords(tasks, TX_ID))
        .isInstanceOf(ExecutionException.class);

    verify(task, times(tasks.size())).run();
    verify(parallelExecutorService, never()).execute(any());
  }

  @Test
  public void
      rollbackRecords_ParallelRollbackEnabledAndAsyncRollbackNotEnabled_ShouldExecuteTasksInParallel()
          throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelRollbackEnabled()).thenReturn(true);
    when(config.isAsyncRollbackEnabled()).thenReturn(false);

    // Act
    parallelExecutor.rollbackRecords(tasks, TX_ID);

    // Assert
    verify(task, times(tasks.size())).run();
    verify(parallelExecutorService, times(tasks.size())).execute(any());
  }

  @Test
  public void
      rollbackRecords_ParallelRollbackEnabledAndAsyncRollbackNotEnabled_ExecutionExceptionThrownByTask_ShouldNotStopRunningTasks()
          throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelRollbackEnabled()).thenReturn(true);
    when(config.isAsyncRollbackEnabled()).thenReturn(false);
    doThrow(ExecutionException.class).when(task).run();

    // Act Assert
    assertThatThrownBy(() -> parallelExecutor.rollbackRecords(tasks, TX_ID))
        .isInstanceOf(ExecutionException.class);

    verify(task, times(tasks.size())).run();
    verify(parallelExecutorService, times(tasks.size())).execute(any());
  }

  @Test
  public void
      rollbackRecords_ParallelRollbackEnabledAndAsyncRollbackEnabled_ShouldExecuteTasksInParallelAndAsynchronously()
          throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelRollbackEnabled()).thenReturn(true);
    when(config.isAsyncRollbackEnabled()).thenReturn(true);

    // Act
    parallelExecutor.rollbackRecords(tasks, TX_ID);

    // Assert
    verify(task, atMost(tasks.size())).run();
    verify(parallelExecutorService, times(tasks.size())).execute(any());
  }

  @Test
  public void
      rollbackRecords_ParallelRollbackEnabledAndAsyncRollbackEnabled_ExecutionExceptionThrownByTask_ShouldNotStopRunningTasks()
          throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelRollbackEnabled()).thenReturn(true);
    when(config.isAsyncRollbackEnabled()).thenReturn(true);
    doThrow(ExecutionException.class).when(task).run();

    // Act Assert
    assertThatCode(() -> parallelExecutor.rollbackRecords(tasks, TX_ID)).doesNotThrowAnyException();

    verify(task, atMost(tasks.size())).run();
    verify(parallelExecutorService, times(tasks.size())).execute(any());
  }

  @Test
  public void
      fillReadSetForRecordsFromWriteAndDeleteSetsIfUnread_ParallelRollbackParallelFillReadSetNotEnabled_ShouldExecuteTasksSerially()
          throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelFillReadSetEnabled()).thenReturn(false);

    // Act
    parallelExecutor.fillReadSetForRecordsFromWriteAndDeleteSetsIfUnread(tasks, TX_ID);

    // Assert
    verify(task, times(tasks.size())).run();
    verify(parallelExecutorService, never()).execute(any());
  }

  @Test
  public void
      fillReadSetForRecordsFromWriteAndDeleteSetsIfUnread_ParallelRollbackParallelFillReadSetNotEnabled_CrudExceptionThrownByTask_ShouldThrowCrudException()
          throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelFillReadSetEnabled()).thenReturn(false);
    doThrow(CrudException.class).when(task).run();

    // Act Assert
    assertThatThrownBy(
            () ->
                parallelExecutor.fillReadSetForRecordsFromWriteAndDeleteSetsIfUnread(tasks, TX_ID))
        .isInstanceOf(CrudException.class);
  }

  @Test
  public void
      fillReadSetForRecordsFromWriteAndDeleteSetsIfUnread_ParallelRollbackParallelFillReadSetEnabled_ShouldExecuteTasksInParallel()
          throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelFillReadSetEnabled()).thenReturn(true);

    // Act
    parallelExecutor.fillReadSetForRecordsFromWriteAndDeleteSetsIfUnread(tasks, TX_ID);

    // Assert
    verify(task, times(tasks.size())).run();
    verify(parallelExecutorService, times(tasks.size())).execute(any());
  }

  @Test
  public void
      fillReadSetForRecordsFromWriteAndDeleteSetsIfUnread_ParallelRollbackParallelFillReadSetEnabled_CrudExceptionThrownByTask_ShouldThrowCrudException()
          throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelFillReadSetEnabled()).thenReturn(true);
    doThrow(CrudException.class).when(task).run();

    // Act Assert
    assertThatThrownBy(
            () ->
                parallelExecutor.fillReadSetForRecordsFromWriteAndDeleteSetsIfUnread(tasks, TX_ID))
        .isInstanceOf(CrudException.class);
  }
}
