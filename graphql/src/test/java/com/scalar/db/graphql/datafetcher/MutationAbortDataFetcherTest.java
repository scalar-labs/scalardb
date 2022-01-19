package com.scalar.db.graphql.datafetcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import com.scalar.db.exception.transaction.AbortException;
import graphql.execution.AbortExecutionException;
import graphql.execution.DataFetcherResult;
import org.junit.Test;

public class MutationAbortDataFetcherTest extends DataFetcherTestBase {
  private MutationAbortDataFetcher dataFetcher;

  @Override
  protected void doSetUp() {
    // Arrange
    dataFetcher = new MutationAbortDataFetcher();
  }

  @Test
  public void get_WhenTransactionIsPresent_ShouldAbortTransactionAndReturnTrue() throws Exception {
    // Arrange
    setTransactionStarted();

    // Act
    DataFetcherResult<Boolean> result = dataFetcher.get(environment);

    // Assert
    verify(transaction).abort();
    assertThat(result.getData()).isTrue();
    assertThat(result.getErrors()).isEmpty();
  }

  @Test
  public void get_WhenTransactionIsNotPresent_ShouldReturnFalseWithErrors() throws Exception {
    // Act
    DataFetcherResult<Boolean> result = dataFetcher.get(environment);

    // Assert
    assertThat(result.getData()).isFalse();
    assertThat(result.getErrors())
        .hasSize(1)
        .element(0)
        .isInstanceOf(AbortExecutionException.class);
  }

  @Test
  public void get_WhenAbortExceptionThrown_ShouldReturnFalseWithErrors() throws Exception {
    // Arrange
    setTransactionStarted();
    AbortException exception = new AbortException("error");
    doThrow(exception).when(transaction).abort();

    // Act
    DataFetcherResult<Boolean> result = dataFetcher.get(environment);

    // Assert
    assertThat(result.getData()).isFalse();
    assertThat(result.getErrors())
        .hasSize(1)
        .element(0)
        .isInstanceOf(AbortExecutionException.class);
    assertThat(((AbortExecutionException) result.getErrors().get(0)).getCause())
        .isSameAs(exception);
  }
}
