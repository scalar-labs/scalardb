package com.scalar.db.util;

import com.google.common.util.concurrent.Uninterruptibles;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class ActiveExpiringMap<K, V> {
  private final ConcurrentMap<K, ValueHolder<V>> map;
  private final long valueLifetimeMillis;
  private final long valueExpirationThreadIntervalMillis;
  private final Consumer<V> valueExpirationHandler;

  public ActiveExpiringMap(
      long valueLifetimeMillis,
      long valueExpirationThreadIntervalMillis,
      Consumer<V> valueExpirationHandler) {
    map = new ConcurrentHashMap<>();
    this.valueLifetimeMillis = valueLifetimeMillis;
    this.valueExpirationThreadIntervalMillis = valueExpirationThreadIntervalMillis;
    this.valueExpirationHandler = valueExpirationHandler;
    startValueExpirationThread();
  }

  private void startValueExpirationThread() {
    Thread expirationThread =
        new Thread(
            () -> {
              while (true) {
                map.entrySet().stream()
                    .filter(e -> e.getValue().isExpired())
                    .map(Entry::getKey)
                    .forEach(
                        key -> {
                          ValueHolder<V> value = map.remove(key);
                          if (value != null) {
                            valueExpirationHandler.accept(value.get());
                          }
                        });
                Uninterruptibles.sleepUninterruptibly(
                    valueExpirationThreadIntervalMillis, TimeUnit.MILLISECONDS);
              }
            });
    expirationThread.setDaemon(true);
    expirationThread.setName("value expiration thread");
    expirationThread.start();
  }

  public Optional<V> get(K key) {
    if (!map.containsKey(key)) {
      return Optional.empty();
    }
    ValueHolder<V> value = map.get(key);
    value.updateExpirationTime();
    return Optional.of(value.get());
  }

  public V putIfAbsent(K key, V value) {
    ValueHolder<V> prev = map.putIfAbsent(key, new ValueHolder<>(value, valueLifetimeMillis));
    if (prev == null) {
      return null;
    }
    return prev.get();
  }

  public V put(K key, V value) {
    ValueHolder<V> prev = map.put(key, new ValueHolder<>(value, valueLifetimeMillis));
    if (prev == null) {
      return null;
    }
    return prev.get();
  }

  public boolean contains(K key) {
    return map.containsKey(key);
  }

  public void remove(K key) {
    map.remove(key);
  }

  public void updateExpirationTime(K key) {
    ValueHolder<V> value = map.get(key);
    if (value != null) {
      value.updateExpirationTime();
    }
  }

  private static class ValueHolder<V> {
    private final V value;
    private final long lifetimeMillis;
    private final AtomicLong lastUpdateTime = new AtomicLong();

    public ValueHolder(V value, long lifetimeMillis) {
      this.value = value;
      this.lifetimeMillis = lifetimeMillis;
      updateExpirationTime();
    }

    public void updateExpirationTime() {
      lastUpdateTime.set(System.currentTimeMillis());
    }

    public boolean isExpired() {
      return System.currentTimeMillis() - lastUpdateTime.get() >= lifetimeMillis;
    }

    public V get() {
      return value;
    }
  }
}
