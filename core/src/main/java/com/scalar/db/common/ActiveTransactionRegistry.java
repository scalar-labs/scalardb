package com.scalar.db.common;

import com.scalar.db.util.ActiveExpiringMap;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A registry for managing active transactions with expiration support.
 *
 * @param <T> the type of transaction to manage
 */
@ThreadSafe
public class ActiveTransactionRegistry<T> {

  private static final long TRANSACTION_EXPIRATION_INTERVAL_MILLIS = 1000;

  private static final Logger logger = LoggerFactory.getLogger(ActiveTransactionRegistry.class);

  private final ActiveExpiringMap<String, T> activeTransactions;

  /**
   * Constructs an ActiveTransactionRegistry.
   *
   * @param expirationTimeMillis the expiration time in milliseconds
   * @param expirationHandler the handler invoked when a transaction expires
   */
  public ActiveTransactionRegistry(
      long expirationTimeMillis, ExpirationHandler<T> expirationHandler) {
    this.activeTransactions =
        new ActiveExpiringMap<>(
            expirationTimeMillis,
            TRANSACTION_EXPIRATION_INTERVAL_MILLIS,
            (id, t) -> {
              logger.warn("The transaction is expired. Transaction ID: {}", id);
              try {
                expirationHandler.onExpired(t);
              } catch (Exception e) {
                logger.warn("Failed to handle the expired transaction. Transaction ID: {}", id, e);
              }
            });
  }

  /**
   * Adds a transaction to the registry.
   *
   * @param id the transaction ID
   * @param transaction the transaction to add
   * @return true if the transaction was added, false if a transaction with the same ID already
   *     exists
   */
  public boolean add(String id, T transaction) {
    return !activeTransactions.putIfAbsent(id, transaction).isPresent();
  }

  /**
   * Removes a transaction from the registry.
   *
   * @param id the transaction ID
   */
  public void remove(String id) {
    activeTransactions.remove(id);
  }

  /**
   * Gets a transaction from the registry.
   *
   * @param id the transaction ID
   * @return an Optional containing the transaction if found
   */
  public Optional<T> get(String id) {
    return activeTransactions.get(id);
  }

  /**
   * Refreshes the expiration timer of a transaction, treating it as recently active. Does nothing
   * if no transaction with the given ID is registered.
   *
   * @param id the transaction ID
   */
  public void touch(String id) {
    activeTransactions.updateExpirationTime(id);
  }

  /**
   * Functional interface invoked when a transaction expires, to dispose of its resources (e.g.,
   * roll it back, or release its context without a storage rollback).
   *
   * @param <T> the type of transaction
   */
  @FunctionalInterface
  public interface ExpirationHandler<T> {
    void onExpired(T transaction) throws Exception;
  }
}
