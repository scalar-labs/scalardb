package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
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
import java.util.Collections;
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
  public void prepareRecords_ParallelPreparationNotEnabled_ShouldExecuteTasksSerially()
      throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelPreparationEnabled()).thenReturn(false);

    // Act
    parallelExecutor.prepareRecords(tasks, TX_ID);

    // Assert
    verify(task, times(tasks.size())).run();
    verify(parallelExecutorService, never()).execute(any());
  }

  @Test
  public void
      prepareRecords_ParallelPreparationNotEnabled_ExecutionExceptionThrownByTask_ShouldStopRunningTasks()
          throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelPreparationEnabled()).thenReturn(false);
    doThrow(ExecutionException.class).when(task).run();

    // Act Assert
    assertThatThrownBy(() -> parallelExecutor.prepareRecords(tasks, TX_ID))
        .isInstanceOf(ExecutionException.class);

    verify(task, only()).run();
    verify(parallelExecutorService, never()).execute(any());
  }

  @Test
  public void prepareRecords_ParallelPreparationEnabled_ShouldExecuteTasksInParallel()
      throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelPreparationEnabled()).thenReturn(true);

    // Act
    parallelExecutor.prepareRecords(tasks, TX_ID);

    // Assert
    verify(task, times(tasks.size())).run();
    verify(parallelExecutorService, times(tasks.size())).execute(any());
  }

  @Test
  public void prepareRecords_ParallelPreparationEnabled_SingleTaskGiven_ShouldExecuteTasksSerially()
      throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelPreparationEnabled()).thenReturn(true);

    // A single task
    tasks = Collections.singletonList(task);

    // Act
    parallelExecutor.prepareRecords(tasks, TX_ID);

    // Assert
    verify(task, times(tasks.size())).run();
    verify(parallelExecutorService, never()).execute(any());
  }

  @Test
  public void
      prepareRecords_ParallelPreparationEnabled_ExecutionExceptionThrownByTask_ShouldNotStopRunningTasks()
          throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelPreparationEnabled()).thenReturn(true);
    doThrow(ExecutionException.class).when(task).run();

    // Act Assert
    assertThatThrownBy(() -> parallelExecutor.prepareRecords(tasks, TX_ID))
        .isInstanceOf(ExecutionException.class);

    verify(task, times(tasks.size())).run();
    verify(parallelExecutorService, times(tasks.size())).execute(any());
  }

  @Test
  public void validateRecords_ParallelValidationNotEnabled_ShouldExecuteTasksSerially()
      throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelValidationEnabled()).thenReturn(false);

    // Act
    parallelExecutor.validateRecords(tasks, TX_ID);

    // Assert
    verify(task, times(tasks.size())).run();
    verify(parallelExecutorService, never()).execute(any());
  }

  @Test
  public void
      validateRecords_ParallelValidationNotEnabled_ExecutionExceptionThrownByTask_ShouldStopRunningTasks()
          throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelValidationEnabled()).thenReturn(false);
    doThrow(ExecutionException.class).when(task).run();

    // Act Assert
    assertThatThrownBy(() -> parallelExecutor.validateRecords(tasks, TX_ID))
        .isInstanceOf(ExecutionException.class);

    verify(task, only()).run();
    verify(parallelExecutorService, never()).execute(any());
  }

  @Test
  public void
      validateRecords_ParallelValidationNotEnabled_ValidationConflictExceptionThrownByTask_ShouldStopRunningTasks()
          throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelValidationEnabled()).thenReturn(false);
    doThrow(ValidationConflictException.class).when(task).run();

    // Act Assert
    assertThatThrownBy(() -> parallelExecutor.validateRecords(tasks, TX_ID))
        .isInstanceOf(ValidationConflictException.class);

    verify(task, only()).run();
    verify(parallelExecutorService, never()).execute(any());
  }

  @Test
  public void validateRecords_ParallelValidationEnabled_ShouldExecuteTasksInParallel()
      throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelValidationEnabled()).thenReturn(true);

    // Act
    parallelExecutor.validateRecords(tasks, TX_ID);

    // Assert
    verify(task, times(tasks.size())).run();
    verify(parallelExecutorService, times(tasks.size())).execute(any());
  }

  @Test
  public void validateRecords_ParallelValidationEnabled_SingleTaskGiven_ShouldExecuteTasksSerially()
      throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelValidationEnabled()).thenReturn(true);

    // A single task
    tasks = Collections.singletonList(task);

    // Act
    parallelExecutor.validateRecords(tasks, TX_ID);

    // Assert
    verify(task, times(tasks.size())).run();
    verify(parallelExecutorService, never()).execute(any());
  }

  @Test
  public void
      validateRecords_ParallelValidationEnabled_ExecutionExceptionThrownByTask_ShouldStopRunningTasks()
          throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelValidationEnabled()).thenReturn(true);
    doThrow(ExecutionException.class).when(task).run();

    // Act Assert
    assertThatThrownBy(() -> parallelExecutor.validateRecords(tasks, TX_ID))
        .isInstanceOf(ExecutionException.class);

    verify(task, atMost(tasks.size())).run();
    verify(parallelExecutorService, times(tasks.size())).execute(any());
  }

  @Test
  public void
      validateRecords_ParallelValidationEnabled_ValidationConflictExceptionThrownByTask_ShouldStopRunningTasks()
          throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelValidationEnabled()).thenReturn(true);
    doThrow(ValidationConflictException.class).when(task).run();

    // Act Assert
    assertThatThrownBy(() -> parallelExecutor.validateRecords(tasks, TX_ID))
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
      commitRecords_ParallelCommitEnabledAndAsyncCommitNotEnabled_SingleTaskGiven_ShouldExecuteTasksSerially()
          throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelCommitEnabled()).thenReturn(true);
    when(config.isAsyncCommitEnabled()).thenReturn(false);

    // A single task
    tasks = Collections.singletonList(task);

    // Act
    parallelExecutor.commitRecords(tasks, TX_ID);

    // Assert
    verify(task, times(tasks.size())).run();
    verify(parallelExecutorService, never()).execute(any());
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
      commitRecords_ParallelCommitEnabledAndAsyncCommitEnabled_SingleTaskGiven_ShouldExecuteTasksInParallelAndAsynchronously()
          throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelCommitEnabled()).thenReturn(true);
    when(config.isAsyncCommitEnabled()).thenReturn(true);

    // A single task
    tasks = Collections.singletonList(task);

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
      rollbackRecords_ParallelRollbackEnabledAndAsyncRollbackNotEnabled_SingleTaskGiven_ShouldExecuteTasksSerially()
          throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelRollbackEnabled()).thenReturn(true);
    when(config.isAsyncRollbackEnabled()).thenReturn(false);

    // A single task
    tasks = Collections.singletonList(task);

    // Act
    parallelExecutor.rollbackRecords(tasks, TX_ID);

    // Assert
    verify(task, times(tasks.size())).run();
    verify(parallelExecutorService, never()).execute(any());
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
      rollbackRecords_ParallelRollbackEnabledAndAsyncRollbackEnabled_SingleTaskGiven_ShouldExecuteTasksInParallelAndAsynchronously()
          throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelRollbackEnabled()).thenReturn(true);
    when(config.isAsyncRollbackEnabled()).thenReturn(true);

    // A single task
    tasks = Collections.singletonList(task);

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
  public void executeImplicitPreRead_ParallelImplicitPreReadNotEnabled_ShouldExecuteTasksSerially()
      throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelImplicitPreReadEnabled()).thenReturn(false);

    // Act
    parallelExecutor.executeImplicitPreRead(tasks, TX_ID);

    // Assert
    verify(task, times(tasks.size())).run();
    verify(parallelExecutorService, never()).execute(any());
  }

  @Test
  public void
      executeImplicitPreRead_ParallelImplicitPreReadNotEnabled_CrudExceptionThrownByTask_ShouldThrowCrudException()
          throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelImplicitPreReadEnabled()).thenReturn(false);
    doThrow(CrudException.class).when(task).run();

    // Act Assert
    assertThatThrownBy(() -> parallelExecutor.executeImplicitPreRead(tasks, TX_ID))
        .isInstanceOf(CrudException.class);
  }

  @Test
  public void executeImplicitPreRead_ParallelImplicitPreReadEnabled_ShouldExecuteTasksInParallel()
      throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelImplicitPreReadEnabled()).thenReturn(true);

    // Act
    parallelExecutor.executeImplicitPreRead(tasks, TX_ID);

    // Assert
    verify(task, times(tasks.size())).run();
    verify(parallelExecutorService, times(tasks.size())).execute(any());
  }

  @Test
  public void
      executeImplicitPreRead_ParallelImplicitPreReadEnabled_CrudExceptionThrownByTask_ShouldThrowCrudException()
          throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelImplicitPreReadEnabled()).thenReturn(true);
    doThrow(CrudException.class).when(task).run();

    // Act Assert
    assertThatThrownBy(() -> parallelExecutor.executeImplicitPreRead(tasks, TX_ID))
        .isInstanceOf(CrudException.class);
  }

  @Test
  public void executeTasks_SingleTaskAndNoWaitFalse_ShouldExecuteDirectly()
      throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    List<ParallelExecutorTask> tasks = Collections.singletonList(task);
    boolean parallel = true; // Should be ignored
    boolean noWait = false;
    boolean stopOnError = true;

    // Act
    parallelExecutor.executeTasks(tasks, parallel, noWait, stopOnError, "test", TX_ID);

    // Assert
    verify(task).run();
    verify(parallelExecutorService, never()).execute(any());
  }

  @Test
  public void executeTasks_SingleTaskAndNoWaitTrue_ShouldUseParallelExecution()
      throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    when(config.isParallelPreparationEnabled()).thenReturn(true);

    List<ParallelExecutorTask> tasks = Collections.singletonList(task);
    boolean parallel = true;
    boolean noWait = true;
    boolean stopOnError = false;

    // Act
    parallelExecutor.executeTasks(tasks, parallel, noWait, stopOnError, "test", TX_ID);

    // Assert
    verify(parallelExecutorService).execute(any());
  }

  @Test
  public void executeTasks_ParallelTrue_ShouldExecuteTasksInParallel()
      throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    boolean parallel = true;
    boolean noWait = false;
    boolean stopOnError = false;

    // Act
    parallelExecutor.executeTasks(tasks, parallel, noWait, stopOnError, "test", TX_ID);

    // Assert
    verify(parallelExecutorService, times(tasks.size())).execute(any());
  }

  @Test
  public void executeTasks_ParallelFalse_ShouldExecuteTasksSerially()
      throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    boolean parallel = false;
    boolean noWait = false;
    boolean stopOnError = false;

    // Act
    parallelExecutor.executeTasks(tasks, parallel, noWait, stopOnError, "test", TX_ID);

    // Assert
    verify(task, times(tasks.size())).run();
    verify(parallelExecutorService, never()).execute(any());
  }

  @Test
  public void executeTasks_ParallelTrueAndStopOnErrorTrue_ExceptionThrown_ShouldStopExecution()
      throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    boolean parallel = true;
    boolean noWait = false;
    boolean stopOnError = true;

    doThrow(new ExecutionException("Test exception")).when(task).run();

    // Act Assert
    assertThatThrownBy(
            () ->
                parallelExecutor.executeTasks(tasks, parallel, noWait, stopOnError, "test", TX_ID))
        .isInstanceOf(ExecutionException.class)
        .hasMessage("Test exception");

    verify(parallelExecutorService, times(tasks.size())).execute(any());
  }

  @Test
  public void
      executeTasks_ParallelTrueAndStopOnErrorFalse_ExceptionThrown_ShouldContinueOtherTasks()
          throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    boolean parallel = true;
    boolean noWait = false;
    boolean stopOnError = false;

    ParallelExecutorTask failingTask = mock(ParallelExecutorTask.class);
    doThrow(new ExecutionException("Test exception")).when(failingTask).run();

    List<ParallelExecutorTask> mixedTasks = Arrays.asList(failingTask, task);

    // Act Assert
    assertThatThrownBy(
            () ->
                parallelExecutor.executeTasks(
                    mixedTasks, parallel, noWait, stopOnError, "test", TX_ID))
        .isInstanceOf(ExecutionException.class);

    verify(parallelExecutorService, times(mixedTasks.size())).execute(any());
  }

  @Test
  public void
      executeTasks_ParallelTrueAndStopOnErrorFalse_ExceptionThrownByMultipleTasks_ShouldContinueOtherTasks()
          throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    boolean parallel = true;
    boolean noWait = false;
    boolean stopOnError = false;

    ExecutionException executionException1 = new ExecutionException("Test exception1");
    ParallelExecutorTask failingTask1 = mock(ParallelExecutorTask.class);
    doThrow(executionException1).when(failingTask1).run();

    ExecutionException executionException2 = new ExecutionException("Test exception2");
    ParallelExecutorTask failingTask2 = mock(ParallelExecutorTask.class);
    doThrow(executionException2).when(failingTask2).run();

    List<ParallelExecutorTask> mixedTasks = Arrays.asList(failingTask1, failingTask2, task);

    // Act
    Exception exception =
        catchException(
            () ->
                parallelExecutor.executeTasks(
                    mixedTasks, parallel, noWait, stopOnError, "test", TX_ID));

    // Assert
    assertThat(exception)
        .satisfiesAnyOf(
            e ->
                assertThat(e)
                    .isEqualTo(executionException1)
                    .hasSuppressedException(executionException2),
            e ->
                assertThat(e)
                    .isEqualTo(executionException2)
                    .hasSuppressedException(executionException1));

    verify(parallelExecutorService, times(mixedTasks.size())).execute(any());
  }

  @Test
  public void
      executeTasks_ParallelFalseAndStopOnErrorFalse_ExceptionThrown_ShouldContinueOtherTasks()
          throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    boolean parallel = false;
    boolean noWait = false;
    boolean stopOnError = false;

    ParallelExecutorTask failingTask = mock(ParallelExecutorTask.class);
    doThrow(new ExecutionException("Test exception")).when(failingTask).run();

    List<ParallelExecutorTask> mixedTasks = Arrays.asList(failingTask, task);

    // Act Assert
    assertThatThrownBy(
            () ->
                parallelExecutor.executeTasks(
                    mixedTasks, parallel, noWait, stopOnError, "test", TX_ID))
        .isInstanceOf(ExecutionException.class);

    verify(failingTask, only()).run();
    verify(task, only()).run();
    verify(parallelExecutorService, never()).execute(any());
  }

  @Test
  public void
      executeTasks_ParallelFalseAndStopOnErrorFalse_ExceptionThrownByMultipleTasks_ShouldContinueOtherTasks()
          throws ExecutionException, ValidationConflictException, CrudException {
    // Arrange
    boolean parallel = false;
    boolean noWait = false;
    boolean stopOnError = false;

    ExecutionException executionException1 = new ExecutionException("Test exception1");
    ParallelExecutorTask failingTask1 = mock(ParallelExecutorTask.class);
    doThrow(executionException1).when(failingTask1).run();

    ExecutionException executionException2 = new ExecutionException("Test exception2");
    ParallelExecutorTask failingTask2 = mock(ParallelExecutorTask.class);
    doThrow(executionException2).when(failingTask2).run();

    List<ParallelExecutorTask> mixedTasks = Arrays.asList(failingTask1, failingTask2, task);

    // Act Assert
    assertThatThrownBy(
            () ->
                parallelExecutor.executeTasks(
                    mixedTasks, parallel, noWait, stopOnError, "test", TX_ID))
        .isEqualTo(executionException1)
        .hasSuppressedException(executionException2);

    verify(failingTask1, only()).run();
    verify(failingTask2, only()).run();
    verify(task, only()).run();
    verify(parallelExecutorService, never()).execute(any());
  }
}
