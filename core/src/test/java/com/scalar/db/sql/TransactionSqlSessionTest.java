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

public class TransactionSqlSessionTest {

  @Mock private DistributedTransactionAdmin admin;
  @Mock private DistributedTransactionManager manager;
  @Mock private StatementValidator statementValidator;
  @Mock private DmlStatementExecutor dmlStatementExecutor;
  @Mock private DdlStatementExecutor ddlStatementExecutor;
  @Mock private DistributedTransaction transaction;

  private TransactionSqlSession transactionSqlSession;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    transactionSqlSession =
        new TransactionSqlSession(
            admin, manager, statementValidator, dmlStatementExecutor, ddlStatementExecutor);
  }

  @Test
  public void beginTransaction_CalledOnce_ShouldNotThrowAnyException() throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);

    // Act Assert
    assertThatCode(() -> transactionSqlSession.beginTransaction()).doesNotThrowAnyException();
    verify(manager).start();
  }

  @Test
  public void beginTransaction_CalledTwice_ShouldThrowIllegalStateException()
      throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);

    // Act Assert
    transactionSqlSession.beginTransaction();
    assertThatThrownBy(() -> transactionSqlSession.beginTransaction())
        .isInstanceOf(IllegalStateException.class);
    verify(manager).start();
  }

  @Test
  public void beginTransaction_WhenManagerThrowsTransactionException_ShouldThrowSqlException()
      throws TransactionException {
    // Arrange
    when(manager.start()).thenThrow(TransactionException.class);

    // Act Assert
    assertThatThrownBy(() -> transactionSqlSession.beginTransaction())
        .isInstanceOf(SqlException.class);
    verify(manager).start();
  }

  @Test
  public void joinTransaction_ShouldThrowUnsupportedOperationException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> transactionSqlSession.joinTransaction("txId"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void execute_DdlStatementGiven_ShouldNotThrowAnyException() {
    // Arrange
    CreateTableStatement ddlStatement = mock(CreateTableStatement.class);

    // Act Assert
    assertThatCode(() -> transactionSqlSession.execute(ddlStatement)).doesNotThrowAnyException();

    verify(statementValidator).validate(ddlStatement);
    verify(ddlStatementExecutor).execute(ddlStatement);
    assertThat(transactionSqlSession.getResultSet()).isEqualTo(EmptyResultSet.INSTANCE);
  }

  @Test
  public void execute_DdlStatementGivenAfterBeginningTransaction_ShouldThrowIllegalStateException()
      throws TransactionException {
    // Arrange
    CreateTableStatement ddlStatement = mock(CreateTableStatement.class);

    when(manager.start()).thenReturn(transaction);

    // Act Assert
    transactionSqlSession.beginTransaction();
    assertThatThrownBy(() -> transactionSqlSession.execute(ddlStatement))
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
    transactionSqlSession.beginTransaction();
    assertThatCode(() -> transactionSqlSession.execute(dmlStatement)).doesNotThrowAnyException();

    verify(statementValidator).validate(dmlStatement);
    verify(dmlStatementExecutor).execute(transaction, dmlStatement);
    assertThat(transactionSqlSession.getResultSet()).isEqualTo(resultSet);
  }

  @Test
  public void
      execute_DmlStatementGivenBeforeBeginningTransaction_ShouldThrowIllegalStateException() {
    // Arrange
    SelectStatement dmlStatement = mock(SelectStatement.class);

    ResultSet resultSet = mock(ResultSet.class);
    when(dmlStatementExecutor.execute(transaction, dmlStatement)).thenReturn(resultSet);

    // Act Assert
    assertThatThrownBy(() -> transactionSqlSession.execute(dmlStatement))
        .isInstanceOf(IllegalStateException.class);

    verify(statementValidator, never()).validate(dmlStatement);
    verify(dmlStatementExecutor, never()).execute(transaction, dmlStatement);
  }

  @Test
  public void getResultSet_BeforeExecutingStatement_ShouldThrowIllegalStateException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> transactionSqlSession.getResultSet())
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void executeQuery_SelectStatementGiven_ShouldNotThrowAnyException()
      throws TransactionException {
    // Arrange
    SelectStatement selectStatement = mock(SelectStatement.class);

    when(manager.start()).thenReturn(transaction);

    ResultSet resultSet = mock(ResultSet.class);
    when(dmlStatementExecutor.execute(transaction, selectStatement)).thenReturn(resultSet);

    // Act Assert
    transactionSqlSession.beginTransaction();
    ResultSet actual = transactionSqlSession.executeQuery(selectStatement);

    verify(statementValidator).validate(selectStatement);
    verify(dmlStatementExecutor).execute(transaction, selectStatement);
    assertThat(actual).isEqualTo(resultSet);
    assertThat(transactionSqlSession.getResultSet()).isEqualTo(resultSet);
  }

  @Test
  public void
      executeQuery_SelectStatementGivenBeforeBeginningTransaction_ShouldThrowIllegalStateException() {
    // Arrange
    SelectStatement selectStatement = mock(SelectStatement.class);

    ResultSet resultSet = mock(ResultSet.class);
    when(dmlStatementExecutor.execute(transaction, selectStatement)).thenReturn(resultSet);

    // Act Assert
    assertThatThrownBy(() -> transactionSqlSession.executeQuery(selectStatement))
        .isInstanceOf(IllegalStateException.class);

    verify(statementValidator, never()).validate(selectStatement);
    verify(dmlStatementExecutor, never()).execute(transaction, selectStatement);
  }

  @Test
  public void prepare_ShouldThrowUnsupportedOperationException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> transactionSqlSession.prepare())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void validate_ShouldThrowUnsupportedOperationException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> transactionSqlSession.validate())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void commit_ShouldNotThrowAnyException() throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);

    // Act Assert
    transactionSqlSession.beginTransaction();
    assertThatCode(() -> transactionSqlSession.commit()).doesNotThrowAnyException();

    verify(transaction).commit();
  }

  @Test
  public void commit_CalledBeforeBeginningTransaction_ShouldThrowIllegalStateException()
      throws TransactionException {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> transactionSqlSession.commit())
        .isInstanceOf(IllegalStateException.class);

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
    transactionSqlSession.beginTransaction();
    assertThatThrownBy(() -> transactionSqlSession.commit())
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
    transactionSqlSession.beginTransaction();
    assertThatThrownBy(() -> transactionSqlSession.commit()).isInstanceOf(SqlException.class);

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
    transactionSqlSession.beginTransaction();
    assertThatThrownBy(() -> transactionSqlSession.commit())
        .isInstanceOf(UnknownTransactionStatusException.class);

    verify(transaction).commit();
  }

  @Test
  public void rollback_ShouldNotThrowAnyException() throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);

    // Act Assert
    transactionSqlSession.beginTransaction();
    assertThatCode(() -> transactionSqlSession.rollback()).doesNotThrowAnyException();

    verify(transaction).abort();
  }

  @Test
  public void rollback_CalledBeforeBeginningTransaction_ShouldThrowIllegalStateException()
      throws TransactionException {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> transactionSqlSession.rollback())
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
    transactionSqlSession.beginTransaction();
    assertThatThrownBy(() -> transactionSqlSession.rollback()).isInstanceOf(SqlException.class);

    verify(transaction).abort();
  }

  @Test
  public void getTransactionId_ShouldNotThrowAnyException() throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);
    when(transaction.getId()).thenReturn("txId");

    // Act
    transactionSqlSession.beginTransaction();
    String transactionId = transactionSqlSession.getTransactionId();

    // Assert
    assertThat(transactionId).isEqualTo("txId");
  }

  @Test
  public void getTransactionId_CalledBeforeBeginningTransaction_ShouldThrowIllegalStateException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> transactionSqlSession.getTransactionId())
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void getMetadata_ShouldNotThrowAnyException() throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);

    // Act Assert
    assertThatCode(() -> transactionSqlSession.getMetadata()).doesNotThrowAnyException();
  }

  @Test
  public void getMetadata_CalledAfterBeginningTransaction_ShouldThrowIllegalStateException()
      throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);

    // Act Assert
    transactionSqlSession.beginTransaction();
    assertThatThrownBy(() -> transactionSqlSession.getMetadata())
        .isInstanceOf(IllegalStateException.class);
  }
}
