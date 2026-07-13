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

  private static final Logger logger = LoggerFactory.getLogger(ActiveTransactionRegistry.class);

  private final ActiveExpiringMap<String, T> activeTransactions;

  /**
   * Constructs an ActiveTransactionRegistry.
   *
   * @param expirationTimeMillis the expiration time in milliseconds
   * @param maxActiveTransactions the maximum number of active transactions to keep; when exceeded,
   *     a transaction is evicted (disposed via {@code disposalHandler}) per the cache's recency-
   *     and frequency-aware policy to make room. Non-positive means unbounded.
   * @param disposalHandler the handler invoked to dispose a transaction when it expires or is
   *     evicted
   */
  public ActiveTransactionRegistry(
      long expirationTimeMillis, int maxActiveTransactions, DisposalHandler<T> disposalHandler) {
    this.activeTransactions =
        new ActiveExpiringMap<>(
            expirationTimeMillis,
            maxActiveTransactions,
            (id, t) -> {
              logger.warn("The transaction is expired. Transaction ID: {}", id);
              dispose(id, t, disposalHandler, "expired");
            },
            (id, t) -> {
              logger.warn(
                  "The transaction is evicted because the maximum number of active transactions "
                      + "({}) was exceeded. Transaction ID: {}",
                  maxActiveTransactions,
                  id);
              dispose(id, t, disposalHandler, "evicted");
            });
  }

  private void dispose(String id, T transaction, DisposalHandler<T> disposalHandler, String cause) {
    try {
      disposalHandler.onDisposed(transaction);
    } catch (Exception e) {
      logger.warn("Failed to handle the {} transaction. Transaction ID: {}", cause, id, e);
    }
  }

  /**
   * Adds a transaction to the registry.
   *
   * <p>A {@code true} return means the transaction was inserted, not that it will remain
   * registered: at the cap, a just-added transaction can be evicted (and disposed) moments later.
   * Callers must not assume a successful add keeps the transaction alive.
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
   * Functional interface invoked to dispose of a transaction's resources (e.g., roll it back, or
   * release its context without a storage rollback) when the transaction either expires (idle
   * timeout) or is evicted because the maximum number of active transactions was exceeded.
   *
   * @param <T> the type of transaction
   */
  @FunctionalInterface
  public interface DisposalHandler<T> {
    void onDisposed(T transaction) throws Exception;
  }
}
