package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionIntegrationTestBase;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Result;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.Key;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public abstract class ConsensusCommitIntegrationTestBase
    extends DistributedTransactionIntegrationTestBase {

  @Override
  protected String getTestName() {
    return "tx_cc";
  }

  @Override
  protected final Properties getProperties(String testName) {
    Properties properties = new Properties();
    properties.putAll(getProps(testName));

    // Add testName as a coordinator namespace suffix
    ConsensusCommitIntegrationTestUtils.addSuffixToCoordinatorNamespace(properties, testName);

    return properties;
  }

  protected abstract Properties getProps(String testName);

  @Test
  public void insertAndInsert_forSameRecord_shouldThrowIllegalArgumentExceptionOnSecondInsert()
      throws TransactionException {
    // Arrange
    Key partitionKey = Key.ofInt(ACCOUNT_ID, 0);
    Key clusteringKey = Key.ofInt(ACCOUNT_TYPE, 0);

    DistributedTransaction transaction = manager.start();

    // Act Assert
    transaction.insert(
        Insert.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());
    assertThatThrownBy(
            () ->
                transaction.insert(
                    Insert.newBuilder()
                        .namespace(namespace)
                        .table(TABLE)
                        .partitionKey(partitionKey)
                        .clusteringKey(clusteringKey)
                        .intValue(BALANCE, INITIAL_BALANCE)
                        .build()))
        .isInstanceOf(IllegalArgumentException.class);

    transaction.rollback();
  }

  @Test
  public void insertAndUpsert_forSameRecord_shouldWorkCorrectly() throws TransactionException {
    // Arrange
    Key partitionKey = Key.ofInt(ACCOUNT_ID, 0);
    Key clusteringKey = Key.ofInt(ACCOUNT_TYPE, 0);
    int expectedBalance = 100;
    int expectedSomeColumn = 200;

    DistributedTransaction transaction = manager.start();

    // Act
    transaction.insert(
        Insert.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());
    transaction.upsert(
        Upsert.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .intValue(BALANCE, expectedBalance)
            .intValue(SOME_COLUMN, expectedSomeColumn)
            .build());

    transaction.commit();

    // Assert
    Optional<Result> optResult = get(prepareGet(0, 0));
    assertThat(optResult).isPresent();
    Result result = optResult.get();
    assertThat(result.getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result.getInt(BALANCE)).isEqualTo(expectedBalance);
    assertThat(result.getInt(SOME_COLUMN)).isEqualTo(expectedSomeColumn);
  }

  @Test
  public void insertAndUpdate_forSameRecord_shouldWorkCorrectly() throws TransactionException {
    // Arrange
    Key partitionKey = Key.ofInt(ACCOUNT_ID, 0);
    Key clusteringKey = Key.ofInt(ACCOUNT_TYPE, 0);
    int expectedBalance = 100;
    int expectedSomeColumn = 200;

    DistributedTransaction transaction = manager.start();

    // Act
    transaction.insert(
        Insert.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());
    transaction.update(
        Update.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .intValue(BALANCE, expectedBalance)
            .intValue(SOME_COLUMN, expectedSomeColumn)
            .build());

    transaction.commit();

    // Assert
    Optional<Result> optResult = get(prepareGet(0, 0));
    assertThat(optResult).isPresent();
    Result result = optResult.get();
    assertThat(result.getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result.getInt(BALANCE)).isEqualTo(expectedBalance);
    assertThat(result.getInt(SOME_COLUMN)).isEqualTo(expectedSomeColumn);
  }

  @Test
  public void insertAndDelete_forSameRecord_shouldWorkCorrectly() throws TransactionException {
    // Arrange
    Key partitionKey = Key.ofInt(ACCOUNT_ID, 0);
    Key clusteringKey = Key.ofInt(ACCOUNT_TYPE, 0);

    DistributedTransaction transaction = manager.start();

    // Act
    transaction.insert(
        Insert.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());
    transaction.delete(
        Delete.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .build());

    transaction.commit();

    // Assert
    Optional<Result> optResult = get(prepareGet(0, 0));
    assertThat(optResult).isNotPresent();
  }

  @Test
  public void upsertAndInsert_forSameRecord_shouldThrowIllegalArgumentExceptionOnInsert()
      throws TransactionException {
    // Arrange
    Key partitionKey = Key.ofInt(ACCOUNT_ID, 0);
    Key clusteringKey = Key.ofInt(ACCOUNT_TYPE, 0);

    DistributedTransaction transaction = manager.start();

    // Act Assert
    transaction.upsert(
        Upsert.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());
    assertThatThrownBy(
            () ->
                transaction.insert(
                    Insert.newBuilder()
                        .namespace(namespace)
                        .table(TABLE)
                        .partitionKey(partitionKey)
                        .clusteringKey(clusteringKey)
                        .intValue(BALANCE, INITIAL_BALANCE)
                        .build()))
        .isInstanceOf(IllegalArgumentException.class);

    transaction.rollback();
  }

  @Test
  public void upsertAndUpsert_forSameRecord_shouldWorkCorrectly() throws TransactionException {
    // Arrange
    Key partitionKey = Key.ofInt(ACCOUNT_ID, 0);
    Key clusteringKey = Key.ofInt(ACCOUNT_TYPE, 0);
    int expectedBalance = 100;
    int expectedSomeColumn = 200;

    DistributedTransaction transaction = manager.start();

    // Act
    transaction.upsert(
        Upsert.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());
    transaction.upsert(
        Upsert.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .intValue(BALANCE, expectedBalance)
            .intValue(SOME_COLUMN, expectedSomeColumn)
            .build());

    transaction.commit();

    // Assert
    Optional<Result> optResult = get(prepareGet(0, 0));
    assertThat(optResult).isPresent();
    Result result = optResult.get();
    assertThat(result.getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result.getInt(BALANCE)).isEqualTo(expectedBalance);
    assertThat(result.getInt(SOME_COLUMN)).isEqualTo(expectedSomeColumn);
  }

  @Test
  public void upsertAndUpdate_forSameRecord_shouldWorkCorrectly() throws TransactionException {
    // Arrange
    Key partitionKey = Key.ofInt(ACCOUNT_ID, 0);
    Key clusteringKey = Key.ofInt(ACCOUNT_TYPE, 0);
    int expectedBalance = 100;
    int expectedSomeColumn = 200;

    DistributedTransaction transaction = manager.start();

    // Act
    transaction.upsert(
        Upsert.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());
    transaction.update(
        Update.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .intValue(BALANCE, expectedBalance)
            .intValue(SOME_COLUMN, expectedSomeColumn)
            .build());

    transaction.commit();

    // Assert
    Optional<Result> optResult = get(prepareGet(0, 0));
    assertThat(optResult).isPresent();
    Result result = optResult.get();
    assertThat(result.getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result.getInt(BALANCE)).isEqualTo(expectedBalance);
    assertThat(result.getInt(SOME_COLUMN)).isEqualTo(expectedSomeColumn);
  }

  @Test
  public void upsertAndDelete_forSameRecord_shouldWorkCorrectly() throws TransactionException {
    // Arrange
    Key partitionKey = Key.ofInt(ACCOUNT_ID, 0);
    Key clusteringKey = Key.ofInt(ACCOUNT_TYPE, 0);

    DistributedTransaction transaction = manager.start();

    // Act
    transaction.upsert(
        Upsert.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());
    transaction.delete(
        Delete.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .build());

    transaction.commit();

    // Assert
    Optional<Result> optResult = get(prepareGet(0, 0));
    assertThat(optResult).isNotPresent();
  }

  @Test
  public void updateAndInsert_forSameRecord_whenRecordNotExists_shouldWorkCorrectly()
      throws TransactionException {
    // Arrange
    Key partitionKey = Key.ofInt(ACCOUNT_ID, 0);
    Key clusteringKey = Key.ofInt(ACCOUNT_TYPE, 0);
    int expectedBalance = 100;
    int expectedSomeColumn = 200;

    DistributedTransaction transaction = manager.start();

    // Act
    transaction.update(
        Update.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());
    transaction.insert(
        Insert.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .intValue(BALANCE, expectedBalance)
            .intValue(SOME_COLUMN, expectedSomeColumn)
            .build());

    transaction.commit();

    // Assert
    Optional<Result> optResult = get(prepareGet(0, 0));
    assertThat(optResult).isPresent();
    Result result = optResult.get();
    assertThat(result.getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result.getInt(BALANCE)).isEqualTo(expectedBalance);
    assertThat(result.getInt(SOME_COLUMN)).isEqualTo(expectedSomeColumn);
  }

  @Test
  public void
      updateAndInsert_forSameRecord_whenRecordExists_shouldThrowIllegalArgumentExceptionOnInsert()
          throws TransactionException {
    // Arrange
    put(preparePut(0, 0));

    Key partitionKey = Key.ofInt(ACCOUNT_ID, 0);
    Key clusteringKey = Key.ofInt(ACCOUNT_TYPE, 0);
    int expectedBalance = 100;
    int expectedSomeColumn = 200;

    DistributedTransaction transaction = manager.start();

    // Act Assert
    transaction.update(
        Update.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());
    assertThatThrownBy(
            () ->
                transaction.insert(
                    Insert.newBuilder()
                        .namespace(namespace)
                        .table(TABLE)
                        .partitionKey(partitionKey)
                        .clusteringKey(clusteringKey)
                        .intValue(BALANCE, expectedBalance)
                        .intValue(SOME_COLUMN, expectedSomeColumn)
                        .build()))
        .isInstanceOf(IllegalArgumentException.class);

    transaction.rollback();
  }

  @Test
  public void updateAndUpsert_forSameRecord_whenRecordNotExists_shouldWorkCorrectly()
      throws TransactionException {
    // Arrange
    Key partitionKey = Key.ofInt(ACCOUNT_ID, 0);
    Key clusteringKey = Key.ofInt(ACCOUNT_TYPE, 0);
    int expectedBalance = 100;
    int expectedSomeColumn = 200;

    DistributedTransaction transaction = manager.start();

    // Act
    transaction.update(
        Update.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());
    transaction.upsert(
        Upsert.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .intValue(BALANCE, expectedBalance)
            .intValue(SOME_COLUMN, expectedSomeColumn)
            .build());

    transaction.commit();

    // Assert
    Optional<Result> optResult = get(prepareGet(0, 0));
    assertThat(optResult).isPresent();
    Result result = optResult.get();
    assertThat(result.getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result.getInt(BALANCE)).isEqualTo(expectedBalance);
    assertThat(result.getInt(SOME_COLUMN)).isEqualTo(expectedSomeColumn);
  }

  @Test
  public void updateAndUpsert_forSameRecord_whenRecordExists_shouldWorkCorrectly()
      throws TransactionException {
    // Arrange
    put(preparePut(0, 0));

    Key partitionKey = Key.ofInt(ACCOUNT_ID, 0);
    Key clusteringKey = Key.ofInt(ACCOUNT_TYPE, 0);
    int expectedBalance = 100;
    int expectedSomeColumn = 200;

    DistributedTransaction transaction = manager.start();

    // Act
    transaction.update(
        Update.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());
    transaction.upsert(
        Upsert.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .intValue(BALANCE, expectedBalance)
            .intValue(SOME_COLUMN, expectedSomeColumn)
            .build());

    transaction.commit();

    // Assert
    Optional<Result> optResult = get(prepareGet(0, 0));
    assertThat(optResult).isPresent();
    Result result = optResult.get();
    assertThat(result.getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result.getInt(BALANCE)).isEqualTo(expectedBalance);
    assertThat(result.getInt(SOME_COLUMN)).isEqualTo(expectedSomeColumn);
  }

  @Test
  public void updateAndUpdate_forSameRecord_whenRecordNotExists_shouldWorkCorrectly()
      throws TransactionException {
    // Arrange
    Key partitionKey = Key.ofInt(ACCOUNT_ID, 0);
    Key clusteringKey = Key.ofInt(ACCOUNT_TYPE, 0);

    DistributedTransaction transaction = manager.start();

    // Act
    transaction.update(
        Update.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());
    transaction.update(
        Update.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());

    transaction.commit();

    // Assert
    Optional<Result> optResult = get(prepareGet(0, 0));
    assertThat(optResult).isNotPresent();
  }

  @Test
  public void updateAndUpdate_forSameRecord_whenRecordExists_shouldWorkCorrectly()
      throws TransactionException {
    // Arrange
    put(preparePut(0, 0));

    Key partitionKey = Key.ofInt(ACCOUNT_ID, 0);
    Key clusteringKey = Key.ofInt(ACCOUNT_TYPE, 0);
    int expectedBalance = 100;
    int expectedSomeColumn = 200;

    DistributedTransaction transaction = manager.start();

    // Act
    transaction.update(
        Update.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());
    transaction.update(
        Update.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .intValue(BALANCE, expectedBalance)
            .intValue(SOME_COLUMN, expectedSomeColumn)
            .build());

    transaction.commit();

    // Assert
    Optional<Result> optResult = get(prepareGet(0, 0));
    assertThat(optResult).isPresent();
    Result result = optResult.get();
    assertThat(result.getInt(ACCOUNT_ID)).isEqualTo(0);
    assertThat(result.getInt(ACCOUNT_TYPE)).isEqualTo(0);
    assertThat(result.getInt(BALANCE)).isEqualTo(expectedBalance);
    assertThat(result.getInt(SOME_COLUMN)).isEqualTo(expectedSomeColumn);
  }

  @Test
  public void updateAndDelete_forSameRecord_whenRecordNotExists_shouldWorkCorrectly()
      throws TransactionException {
    // Arrange
    Key partitionKey = Key.ofInt(ACCOUNT_ID, 0);
    Key clusteringKey = Key.ofInt(ACCOUNT_TYPE, 0);

    DistributedTransaction transaction = manager.start();

    // Act
    transaction.update(
        Update.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());
    transaction.delete(
        Delete.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .build());

    transaction.commit();

    // Assert
    Optional<Result> optResult = get(prepareGet(0, 0));
    assertThat(optResult).isNotPresent();
  }

  @Test
  public void updateAndDelete_forSameRecord_whenRecordExists_shouldWorkCorrectly()
      throws TransactionException {
    // Arrange
    put(preparePut(0, 0));

    Key partitionKey = Key.ofInt(ACCOUNT_ID, 0);
    Key clusteringKey = Key.ofInt(ACCOUNT_TYPE, 0);

    DistributedTransaction transaction = manager.start();

    // Act
    transaction.update(
        Update.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());
    transaction.delete(
        Delete.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .build());

    transaction.commit();

    // Assert
    Optional<Result> optResult = get(prepareGet(0, 0));
    assertThat(optResult).isNotPresent();
  }

  @Test
  public void
      deleteAndInsert_forSameRecord_whenRecordExists_shouldThrowIllegalArgumentExceptionOnInsert()
          throws TransactionException {
    // Arrange
    put(preparePut(0, 0));

    Key partitionKey = Key.ofInt(ACCOUNT_ID, 0);
    Key clusteringKey = Key.ofInt(ACCOUNT_TYPE, 0);

    DistributedTransaction transaction = manager.start();

    // Act Assert
    transaction.delete(
        Delete.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .build());
    assertThatThrownBy(
            () ->
                transaction.insert(
                    Insert.newBuilder()
                        .namespace(namespace)
                        .table(TABLE)
                        .partitionKey(partitionKey)
                        .clusteringKey(clusteringKey)
                        .intValue(BALANCE, INITIAL_BALANCE)
                        .build()))
        .isInstanceOf(IllegalArgumentException.class);

    transaction.rollback();
  }

  @Test
  public void
      deleteAndUpsert_forSameRecord_whenRecordExists_shouldThrowIllegalArgumentExceptionOnUpsert()
          throws TransactionException {
    // Arrange
    put(preparePut(0, 0));

    Key partitionKey = Key.ofInt(ACCOUNT_ID, 0);
    Key clusteringKey = Key.ofInt(ACCOUNT_TYPE, 0);

    DistributedTransaction transaction = manager.start();

    // Act Assert
    transaction.delete(
        Delete.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .build());
    assertThatThrownBy(
            () ->
                transaction.upsert(
                    Upsert.newBuilder()
                        .namespace(namespace)
                        .table(TABLE)
                        .partitionKey(partitionKey)
                        .clusteringKey(clusteringKey)
                        .intValue(BALANCE, INITIAL_BALANCE)
                        .build()))
        .isInstanceOf(IllegalArgumentException.class);

    transaction.rollback();
  }

  @Test
  public void deleteAndUpdate_forSameRecord_whenRecordExists_shouldDoNothing()
      throws TransactionException {
    // Arrange
    put(preparePut(0, 0));

    Key partitionKey = Key.ofInt(ACCOUNT_ID, 0);
    Key clusteringKey = Key.ofInt(ACCOUNT_TYPE, 0);

    DistributedTransaction transaction = manager.start();

    // Act
    transaction.delete(
        Delete.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .build());
    transaction.update(
        Update.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .intValue(BALANCE, INITIAL_BALANCE)
            .build());

    transaction.commit();

    // Assert
    Optional<Result> optResult = get(prepareGet(0, 0));
    assertThat(optResult).isNotPresent();
  }

  @Test
  public void deleteAndDelete_forSameRecord_shouldWorkCorrectly() throws TransactionException {
    // Arrange
    put(preparePut(0, 0));

    Key partitionKey = Key.ofInt(ACCOUNT_ID, 0);
    Key clusteringKey = Key.ofInt(ACCOUNT_TYPE, 0);

    DistributedTransaction transaction = manager.start();

    // Act
    transaction.delete(
        Delete.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .build());
    transaction.delete(
        Delete.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey)
            .build());

    transaction.commit();

    // Assert
    Optional<Result> optResult = get(prepareGet(0, 0));
    assertThat(optResult).isNotPresent();
  }
}
