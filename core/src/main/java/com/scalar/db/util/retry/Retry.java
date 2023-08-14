package com.scalar.db.util.retry;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.util.ThrowableSupplier;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Retry {
  private static final Logger logger = LoggerFactory.getLogger(Retry.class);

  private static final int RETRY_INITIAL_INTERVAL_MILLIS = 100;
  private static final int RETRY_MAX_INTERVAL_MILLIS = 60000;
  private static final int RETRY_MULTIPLIER = 2;
  private static final int RETRY_MAX_RETRIES = 10;

  @FunctionalInterface
  public interface ExceptionFactory<E extends Throwable> {
    E create(String message, @Nullable Throwable cause);
  }

  private Retry() {}

  public static <T, E extends Throwable> T executeWithRetries(
      ThrowableSupplier<T, E> supplier, ExceptionFactory<E> exceptionFactory) throws E {
    int interval = RETRY_INITIAL_INTERVAL_MILLIS;
    for (int i = 0; i < RETRY_MAX_RETRIES; i++) {
      try {
        return supplier.get();
      } catch (ServiceTemporaryUnavailableException e) {
        logger.warn(
            "Received UNAVAILABLE state (the message: \"{}\"). retrying after {} milliseconds..."
                + " the current attempt count: {}",
            e.getMessage(),
            interval,
            i + 1);
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw exceptionFactory.create(e.getMessage(), e);
      }

      Uninterruptibles.sleepUninterruptibly(interval, TimeUnit.MILLISECONDS);

      interval *= RETRY_MULTIPLIER;
      if (interval > RETRY_MAX_INTERVAL_MILLIS) {
        interval = RETRY_MAX_INTERVAL_MILLIS;
      }
    }

    throw exceptionFactory.create("retry exhausted. retried " + RETRY_MAX_RETRIES + " times", null);
  }
}
