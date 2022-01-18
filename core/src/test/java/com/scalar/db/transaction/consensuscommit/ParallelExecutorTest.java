package com.scalar.db.transaction.consensuscommit;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.transaction.consensuscommit.ParallelExecutor.ParallelExecutorTask;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
public class ParallelExecutorTest {

  @Mock private ConsensusCommitConfig config;
  @Mock private ExecutorService parallelExecutorService;
  @Mock private Future<Void> future;
  @Mock private ParallelExecutorTask task;

  private ParallelExecutor parallelExecutor;
  private List<ParallelExecutorTask> tasks;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    when(parallelExecutorService.submit(ArgumentMatchers.<Callable<Void>>any())).thenReturn(future);
    parallelExecutor = new ParallelExecutor(config, parallelExecutorService);
    tasks = Arrays.asList(task, task, task);
  }

  @Test
  public void prepare_ParallelPreparationNotEnabled_ShouldExecuteTasksSerially()
      throws ExecutionException, java.util.concurrent.ExecutionException, InterruptedException,
          CommitConflictException {
    // Arrange
    when(config.isParallelPreparationEnabled()).thenReturn(false);

    // Act
    parallelExecutor.prepare(tasks);

    // Assert
    verify(task, times(tasks.size())).run();
    verify(parallelExecutorService, never()).submit(ArgumentMatchers.<Callable<Void>>any());
    verify(future, never()).get();
  }

  @Test
  public void prepare_ParallelPreparationEnabled_ShouldExecuteTasksInParallel()
      throws ExecutionException, java.util.concurrent.ExecutionException, InterruptedException,
          CommitConflictException {
    // Arrange
    when(config.isParallelPreparationEnabled()).thenReturn(true);

    // Act
    parallelExecutor.prepare(tasks);

    // Assert
    verify(task, never()).run();
    verify(parallelExecutorService, times(tasks.size()))
        .submit(ArgumentMatchers.<Callable<Void>>any());
    verify(future, times(tasks.size())).get();
  }

  @Test
  public void validate_ParallelValidationNotEnabled_ShouldExecuteTasksSerially()
      throws ExecutionException, java.util.concurrent.ExecutionException, InterruptedException,
          CommitConflictException {
    // Arrange
    when(config.isParallelValidationEnabled()).thenReturn(false);

    // Act
    parallelExecutor.validate(tasks);

    // Assert
    verify(task, times(tasks.size())).run();
    verify(parallelExecutorService, never()).submit(ArgumentMatchers.<Callable<Void>>any());
    verify(future, never()).get();
  }

  @Test
  public void validate_ParallelValidationEnabled_ShouldExecuteTasksInParallel()
      throws ExecutionException, java.util.concurrent.ExecutionException, InterruptedException,
          CommitConflictException {
    // Arrange
    when(config.isParallelValidationEnabled()).thenReturn(true);

    // Act
    parallelExecutor.validate(tasks);

    // Assert
    verify(task, never()).run();
    verify(parallelExecutorService, times(tasks.size()))
        .submit(ArgumentMatchers.<Callable<Void>>any());
    verify(future, times(tasks.size())).get();
  }

  @Test
  public void commit_ParallelCommitNotEnabled_ShouldExecuteTasksSerially()
      throws ExecutionException, java.util.concurrent.ExecutionException, InterruptedException,
          CommitConflictException {
    // Arrange
    when(config.isParallelCommitEnabled()).thenReturn(false);

    // Act
    parallelExecutor.commit(tasks);

    // Assert
    verify(task, times(tasks.size())).run();
    verify(parallelExecutorService, never()).submit(ArgumentMatchers.<Callable<Void>>any());
    verify(future, never()).get();
  }

  @Test
  public void commit_ParallelCommitEnabled_ShouldExecuteTasksInParallel()
      throws ExecutionException, java.util.concurrent.ExecutionException, InterruptedException,
          CommitConflictException {
    // Arrange
    when(config.isParallelCommitEnabled()).thenReturn(true);

    // Act
    parallelExecutor.commit(tasks);

    // Assert
    verify(task, never()).run();
    verify(parallelExecutorService, times(tasks.size()))
        .submit(ArgumentMatchers.<Callable<Void>>any());
    verify(future, times(tasks.size())).get();
  }

  @Test
  public void
      commit_ParallelCommitEnabledAndAsyncCommitEnabled_ShouldExecuteTasksInParallelAndAsynchronously()
          throws ExecutionException, java.util.concurrent.ExecutionException, InterruptedException,
              CommitConflictException {
    // Arrange
    when(config.isParallelCommitEnabled()).thenReturn(true);
    when(config.isAsyncCommitEnabled()).thenReturn(true);

    // Act
    parallelExecutor.commit(tasks);

    // Assert
    verify(task, never()).run();
    verify(parallelExecutorService, times(tasks.size()))
        .submit(ArgumentMatchers.<Callable<Void>>any());
    verify(future, never()).get();
  }

  @Test
  public void rollback_ParallelRollbackNotEnabled_ShouldExecuteTasksSerially()
      throws ExecutionException, java.util.concurrent.ExecutionException, InterruptedException,
          CommitConflictException {
    // Arrange
    when(config.isParallelRollbackEnabled()).thenReturn(false);

    // Act
    parallelExecutor.rollback(tasks);

    // Assert
    verify(task, times(tasks.size())).run();
    verify(parallelExecutorService, never()).submit(ArgumentMatchers.<Callable<Void>>any());
    verify(future, never()).get();
  }

  @Test
  public void rollback_ParallelRollbackEnabled_ShouldExecuteTasksInParallel()
      throws ExecutionException, java.util.concurrent.ExecutionException, InterruptedException,
          CommitConflictException {
    // Arrange
    when(config.isParallelRollbackEnabled()).thenReturn(true);

    // Act
    parallelExecutor.rollback(tasks);

    // Assert
    verify(task, never()).run();
    verify(parallelExecutorService, times(tasks.size()))
        .submit(ArgumentMatchers.<Callable<Void>>any());
    verify(future, times(tasks.size())).get();
  }

  @Test
  public void
      rollback_ParallelRollbackEnabledAndAsyncRollbackEnabled_ShouldExecuteTasksInParallelAndAsynchronously()
          throws ExecutionException, java.util.concurrent.ExecutionException, InterruptedException,
              CommitConflictException {
    // Arrange
    when(config.isParallelRollbackEnabled()).thenReturn(true);
    when(config.isAsyncRollbackEnabled()).thenReturn(true);

    // Act
    parallelExecutor.rollback(tasks);

    // Assert
    verify(task, never()).run();
    verify(parallelExecutorService, times(tasks.size()))
        .submit(ArgumentMatchers.<Callable<Void>>any());
    verify(future, never()).get();
  }
}
