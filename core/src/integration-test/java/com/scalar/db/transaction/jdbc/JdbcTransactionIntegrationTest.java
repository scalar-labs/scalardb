package com.scalar.db.transaction.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedTransactionIntegrationTestBase;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.storage.jdbc.JdbcEnv;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class JdbcTransactionIntegrationTest extends DistributedTransactionIntegrationTestBase {

  @Override
  protected String getTestName() {
    return "tx_jdbc";
  }

  @Override
  protected Properties getProperties(String testName) {
    Properties properties = new Properties();
    properties.putAll(JdbcEnv.getProperties(testName));
    properties.setProperty(DatabaseConfig.TRANSACTION_MANAGER, "jdbc");
    return properties;
  }

  @Disabled("JDBC transaction doesn't support getState()")
  @Override
  public void getState_forSuccessfulTransaction_ShouldReturnCommittedState() {}

  @Disabled("JDBC transaction doesn't support getState()")
  @Override
  public void getState_forFailedTransaction_ShouldReturnAbortedState() {}

  @Disabled("JDBC transaction doesn't support abort()")
  @Override
  public void abort_forOngoingTransaction_ShouldAbortCorrectly() {}

  @Disabled("JDBC transaction doesn't support rollback()")
  @Override
  public void rollback_forOngoingTransaction_ShouldRollbackCorrectly() {}

  @Test
  public void put_withPutIfWhenRecordDoesNotExist_shouldThrowCrudException()
      throws TransactionException {

    // Arrange
    Put put =
        Put.newBuilder(preparePut(0, 0))
            .condition(ConditionBuilder.putIf(ConditionBuilder.column(BALANCE).isNullInt()).build())
            .build();

    // Act Assert
    assertThatThrownBy(() -> put(put)).isInstanceOf(CrudException.class);

    Optional<Result> result = get(prepareGet(0, 0));
    assertThat(result).isNotPresent();
  }

  @Test
  public void put_withPutIfExistsWhenRecordDoesNotExist_shouldThrowCrudException()
      throws TransactionException {

    // Arrange
    Put put = Put.newBuilder(preparePut(0, 0)).condition(ConditionBuilder.putIfExists()).build();

    // Act Assert
    assertThatThrownBy(() -> getThenPut(put)).isInstanceOf(CrudException.class);

    Optional<Result> result = get(prepareGet(0, 0));
    assertThat(result).isNotPresent();
  }

  @Test
  public void put_withPutIfNotExistsWhenRecordExists_shouldThrowCrudException()
      throws TransactionException {
    // Arrange
    Put put = preparePut(0, 0);
    put(put);
    Put putIfNotExists =
        Put.newBuilder(put)
            .intValue(BALANCE, INITIAL_BALANCE)
            .condition(ConditionBuilder.putIfNotExists())
            .build();

    // Act Assert
    assertThatThrownBy(() -> getThenPut(putIfNotExists)).isInstanceOf(CrudException.class);

    Optional<Result> optResult = get(prepareGet(0, 0));
    assertThat(optResult.isPresent()).isTrue();
    Result result = optResult.get();
    assertThat(result.getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result.isNull(BALANCE)).isTrue();
    assertThat(result.isNull(SOME_COLUMN)).isTrue();
  }

  @Test
  public void delete_withDeleteIfExistsWhenRecordDoesNotExist_shouldThrowCrudException() {
    // Arrange
    Delete deleteIf =
        Delete.newBuilder(prepareDelete(0, 0)).condition(ConditionBuilder.deleteIfExists()).build();

    // Act Assert
    assertThatThrownBy(() -> getThenDelete(deleteIf)).isInstanceOf(CrudException.class);
  }

  @Test
  public void delete_withDeleteIfWithNonVerifiedCondition_shouldThrowCrudException()
      throws TransactionException {
    // Arrange
    Put initialData = Put.newBuilder(preparePut(0, 0)).build();
    put(initialData);

    Delete deleteIf =
        Delete.newBuilder(prepareDelete(0, 0))
            .condition(
                ConditionBuilder.deleteIf(
                        ConditionBuilder.column(BALANCE).isEqualToInt(INITIAL_BALANCE))
                    .and(ConditionBuilder.column(SOME_COLUMN).isNotNullInt())
                    .build())
            .build();

    // Act Assert
    assertThatThrownBy(() -> getThenDelete(deleteIf)).isInstanceOf(CrudException.class);

    Optional<Result> optResult = get(prepareGet(0, 0));
    assertThat(optResult.isPresent()).isTrue();
    Result result = optResult.get();
    assertThat(result.getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result.isNull(BALANCE)).isTrue();
    assertThat(result.isNull(SOME_COLUMN)).isTrue();
  }

  @Test
  public void put_withPutIfWithNonVerifiedCondition_shouldThrowCrudException()
      throws TransactionException {
    // Arrange
    Put initialData = Put.newBuilder(preparePut(0, 0)).intValue(BALANCE, INITIAL_BALANCE).build();
    put(initialData);

    Put putIf =
        Put.newBuilder(initialData)
            .intValue(BALANCE, 2)
            .condition(
                ConditionBuilder.putIf(
                        ConditionBuilder.column(BALANCE).isEqualToInt(INITIAL_BALANCE))
                    .and(ConditionBuilder.column(SOME_COLUMN).isNotNullInt())
                    .build())
            .build();

    // Act Assert
    assertThatThrownBy(() -> getThenPut(putIf)).isInstanceOf(CrudException.class);

    Optional<Result> optResult = get(prepareGet(0, 0));
    assertThat(optResult.isPresent()).isTrue();
    Result result = optResult.get();
    assertThat(result.getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result.getInt(BALANCE)).isEqualTo(INITIAL_BALANCE);
    assertThat(result.isNull(SOME_COLUMN)).isTrue();
  }
}
