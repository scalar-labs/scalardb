package com.scalar.db.util;

import com.google.common.util.concurrent.Uninterruptibles;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class ObjectHolder<T> {
  private final ConcurrentMap<String, Holder<T>> objects;
  private final long objectLifetimeMillis;
  private final long objectExpirationThreadIntervalMillis;
  private final Consumer<T> objectExpirationHandler;

  public ObjectHolder(
      long objectLifetimeMillis,
      long objectExpirationThreadIntervalMillis,
      Consumer<T> objectExpirationHandler) {
    objects = new ConcurrentHashMap<>();
    this.objectLifetimeMillis = objectLifetimeMillis;
    this.objectExpirationThreadIntervalMillis = objectExpirationThreadIntervalMillis;
    this.objectExpirationHandler = objectExpirationHandler;
    startObjectExpirationThread();
  }

  private void startObjectExpirationThread() {
    Thread expirationThread =
        new Thread(
            () -> {
              while (true) {
                objects.entrySet().stream()
                    .filter(e -> e.getValue().isExpired())
                    .map(Entry::getKey)
                    .forEach(
                        id -> {
                          Holder<T> object = objects.remove(id);
                          if (object != null) {
                            objectExpirationHandler.accept(object.get());
                          }
                        });
                Uninterruptibles.sleepUninterruptibly(
                    objectExpirationThreadIntervalMillis, TimeUnit.MILLISECONDS);
              }
            });
    expirationThread.setDaemon(true);
    expirationThread.setName("object expiration thread");
    expirationThread.start();
  }

  public Optional<T> get(String id) {
    if (!objects.containsKey(id)) {
      return Optional.empty();
    }
    Holder<T> object = objects.get(id);
    object.updateExpirationTime();
    return Optional.of(object.get());
  }

  public T putIfAbsent(String id, T object) {
    Holder<T> prev = objects.putIfAbsent(id, new Holder<>(object, objectLifetimeMillis));
    if (prev == null) {
      return null;
    }
    return prev.get();
  }

  public T put(String id, T object) {
    Holder<T> prev = objects.put(id, new Holder<>(object, objectLifetimeMillis));
    if (prev == null) {
      return null;
    }
    return prev.get();
  }

  public boolean contains(String id) {
    return objects.containsKey(id);
  }

  public void remove(String id) {
    objects.remove(id);
  }

  public void updateExpirationTime(String id) {
    Holder<T> object = objects.get(id);
    if (object != null) {
      object.updateExpirationTime();
    }
  }

  private static class Holder<T> {
    private final T object;
    private final long lifetimeMillis;
    private final AtomicLong expirationTime = new AtomicLong();

    public Holder(T object, long lifetimeMillis) {
      this.object = object;
      this.lifetimeMillis = lifetimeMillis;
      updateExpirationTime();
    }

    public void updateExpirationTime() {
      expirationTime.set(System.currentTimeMillis() + lifetimeMillis);
    }

    public boolean isExpired() {
      return System.currentTimeMillis() >= expirationTime.get();
    }

    public T get() {
      return object;
    }
  }
}
