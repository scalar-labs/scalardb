package com.scalar.db.sql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.sql.exception.SqlException;
import com.scalar.db.sql.exception.TransactionConflictException;
import com.scalar.db.sql.exception.UnknownTransactionStatusException;
import com.scalar.db.sql.statement.CreateTableStatement;
import com.scalar.db.sql.statement.SelectStatement;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TransactionSessionTest {

  @Mock private DistributedTransactionAdmin admin;
  @Mock private DistributedTransactionManager manager;
  @Mock private StatementValidator statementValidator;
  @Mock private DmlStatementExecutor dmlStatementExecutor;
  @Mock private DdlStatementExecutor ddlStatementExecutor;
  @Mock private DistributedTransaction transaction;

  private TransactionSession transactionSession;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    transactionSession =
        new TransactionSession(
            admin, manager, statementValidator, dmlStatementExecutor, ddlStatementExecutor);
  }

  @Test
  public void begin_CalledOnce_ShouldNotThrowAnyException() throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);

    // Act Assert
    assertThatCode(() -> transactionSession.begin()).doesNotThrowAnyException();
    verify(manager).start();
  }

  @Test
  public void begin_CalledTwice_ShouldThrowIllegalStateException() throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);

    // Act Assert
    transactionSession.begin();
    assertThatThrownBy(() -> transactionSession.begin()).isInstanceOf(IllegalStateException.class);
    verify(manager).start();
  }

  @Test
  public void begin_WhenManagerThrowsTransactionException_ShouldThrowSqlException()
      throws TransactionException {
    // Arrange
    when(manager.start()).thenThrow(TransactionException.class);

    // Act Assert
    assertThatThrownBy(() -> transactionSession.begin()).isInstanceOf(SqlException.class);
    verify(manager).start();
  }

  @Test
  public void join_ShouldThrowUnsupportedOperationException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> transactionSession.join("txId"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void resume_ShouldThrowUnsupportedOperationException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> transactionSession.resume("txId"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void execute_DdlStatementGiven_ShouldNotThrowAnyException() {
    // Arrange
    CreateTableStatement ddlStatement = mock(CreateTableStatement.class);

    // Act Assert
    ResultSet actual = transactionSession.execute(ddlStatement);

    verify(statementValidator).validate(ddlStatement);
    verify(ddlStatementExecutor).execute(ddlStatement);
    assertThat(actual).isEqualTo(EmptyResultSet.INSTANCE);
  }

  @Test
  public void execute_DdlStatementGivenAfterBeginningTransaction_ShouldThrowIllegalStateException()
      throws TransactionException {
    // Arrange
    CreateTableStatement ddlStatement = mock(CreateTableStatement.class);

    when(manager.start()).thenReturn(transaction);

    // Act Assert
    transactionSession.begin();
    assertThatThrownBy(() -> transactionSession.execute(ddlStatement))
        .isInstanceOf(IllegalStateException.class);

    verify(statementValidator, never()).validate(ddlStatement);
    verify(ddlStatementExecutor, never()).execute(ddlStatement);
  }

  @Test
  public void execute_DmlStatementGiven_ShouldNotThrowAnyException() throws TransactionException {
    // Arrange
    SelectStatement dmlStatement = mock(SelectStatement.class);

    when(manager.start()).thenReturn(transaction);

    ResultSet resultSet = mock(ResultSet.class);
    when(dmlStatementExecutor.execute(transaction, dmlStatement)).thenReturn(resultSet);

    // Act Assert
    transactionSession.begin();
    ResultSet actual = transactionSession.execute(dmlStatement);

    verify(statementValidator).validate(dmlStatement);
    verify(dmlStatementExecutor).execute(transaction, dmlStatement);
    assertThat(actual).isEqualTo(resultSet);
  }

  @Test
  public void
      execute_DmlStatementGivenBeforeBeginningTransaction_ShouldThrowIllegalStateException() {
    // Arrange
    SelectStatement dmlStatement = mock(SelectStatement.class);

    ResultSet resultSet = mock(ResultSet.class);
    when(dmlStatementExecutor.execute(transaction, dmlStatement)).thenReturn(resultSet);

    // Act Assert
    assertThatThrownBy(() -> transactionSession.execute(dmlStatement))
        .isInstanceOf(IllegalStateException.class);

    verify(statementValidator, never()).validate(dmlStatement);
    verify(dmlStatementExecutor, never()).execute(transaction, dmlStatement);
  }

  @Test
  public void prepare_ShouldThrowUnsupportedOperationException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> transactionSession.prepare())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void validate_ShouldThrowUnsupportedOperationException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> transactionSession.validate())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void commit_ShouldNotThrowAnyException() throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);

    // Act Assert
    transactionSession.begin();
    assertThatCode(() -> transactionSession.commit()).doesNotThrowAnyException();

    verify(transaction).commit();
  }

  @Test
  public void commit_CalledBeforeBeginningTransaction_ShouldThrowIllegalStateException()
      throws TransactionException {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> transactionSession.commit()).isInstanceOf(IllegalStateException.class);

    verify(transaction, never()).commit();
  }

  @Test
  public void
      commit_WhenTransactionThrowsCommitConflictException_ShouldThrowTransactionConflictException()
          throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);
    doThrow(CommitConflictException.class).when(transaction).commit();

    // Act Assert
    transactionSession.begin();
    assertThatThrownBy(() -> transactionSession.commit())
        .isInstanceOf(TransactionConflictException.class);

    verify(transaction).commit();
  }

  @Test
  public void commit_WhenTransactionThrowsCommitException_ShouldThrowSqlException()
      throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);
    doThrow(CommitException.class).when(transaction).commit();

    // Act Assert
    transactionSession.begin();
    assertThatThrownBy(() -> transactionSession.commit()).isInstanceOf(SqlException.class);

    verify(transaction).commit();
  }

  @Test
  public void
      commit_WhenTransactionThrowsUnknownTransactionStatusException_ShouldThrowUnknownTransactionStatusException()
          throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);
    doThrow(com.scalar.db.exception.transaction.UnknownTransactionStatusException.class)
        .when(transaction)
        .commit();

    // Act Assert
    transactionSession.begin();
    assertThatThrownBy(() -> transactionSession.commit())
        .isInstanceOf(UnknownTransactionStatusException.class);

    verify(transaction).commit();
  }

  @Test
  public void rollback_ShouldNotThrowAnyException() throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);

    // Act Assert
    transactionSession.begin();
    assertThatCode(() -> transactionSession.rollback()).doesNotThrowAnyException();

    verify(transaction).abort();
  }

  @Test
  public void rollback_CalledBeforeBeginningTransaction_ShouldThrowIllegalStateException()
      throws TransactionException {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> transactionSession.rollback())
        .isInstanceOf(IllegalStateException.class);

    verify(transaction, never()).abort();
  }

  @Test
  public void rollback_WhenTransactionThrowsAbortException_ShouldThrowSqlException()
      throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);
    doThrow(AbortException.class).when(transaction).abort();

    // Act Assert
    transactionSession.begin();
    assertThatThrownBy(() -> transactionSession.rollback()).isInstanceOf(SqlException.class);

    verify(transaction).abort();
  }

  @Test
  public void getTransactionId_ShouldNotThrowAnyException() throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);
    when(transaction.getId()).thenReturn("txId");

    // Act
    transactionSession.begin();
    String transactionId = transactionSession.getTransactionId();

    // Assert
    assertThat(transactionId).isEqualTo("txId");
  }

  @Test
  public void getTransactionId_CalledBeforeBeginningTransaction_ShouldThrowIllegalStateException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> transactionSession.getTransactionId())
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void getMetadata_ShouldNotThrowAnyException() throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);

    // Act Assert
    assertThatCode(() -> transactionSession.getMetadata()).doesNotThrowAnyException();
  }

  @Test
  public void getMetadata_CalledAfterBeginningTransaction_ShouldThrowIllegalStateException()
      throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);

    // Act Assert
    transactionSession.begin();
    assertThatThrownBy(() -> transactionSession.getMetadata())
        .isInstanceOf(IllegalStateException.class);
  }
}
