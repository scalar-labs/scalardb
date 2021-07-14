package com.scalar.db.server;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.util.ThrowableRunnable;
import com.scalar.db.util.ThrowableSupplier;
import java.util.Objects;

public class Metrics {
  private final MetricRegistry metricRegistry;

  private final Counter totalSucceeded;
  private final Counter totalFailed;

  public Metrics(MetricRegistry metricRegistry) {
    this.metricRegistry = Objects.requireNonNull(metricRegistry);

    totalSucceeded = metricRegistry.counter(name(Metrics.class, "totalSucceeded"));
    totalFailed = metricRegistry.counter(name(Metrics.class, "totalFailed"));
  }

  @VisibleForTesting
  public Metrics() {
    metricRegistry = null;
    totalSucceeded = null;
    totalFailed = null;
  }

  public void measure(Class<?> klass, String method, ThrowableRunnable<Throwable> runnable)
      throws Throwable {
    // For test
    if (metricRegistry == null) {
      runnable.run();
    }

    Timer timer = metricRegistry.timer(name(klass, method));
    try (Context unused = timer.time()) {
      runnable.run();
      totalSucceeded.inc();
    } catch (Throwable t) {
      totalFailed.inc();
      throw t;
    }
  }

  public <T> T measure(Class<?> klass, String method, ThrowableSupplier<T, Throwable> supplier)
      throws Throwable {
    // For test
    if (metricRegistry == null) {
      return supplier.get();
    }

    Timer timer = metricRegistry.timer(name(klass, method));
    try (Context unused = timer.time()) {
      T ret = supplier.get();
      totalSucceeded.inc();
      return ret;
    } catch (Throwable t) {
      totalFailed.inc();
      throw t;
    }
  }
}
