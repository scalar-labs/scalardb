package com.scalar.db.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import javax.annotation.Nullable;
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
    if (valueLifetimeMillis > 0) {
      startValueExpirationThread();
    }
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

  public boolean containsValue(V value) {
    return map.values().stream().map(ValueHolder::get).anyMatch(value::equals);
  }

  public V remove(K key) {
    ValueHolder<V> prev = map.remove(key);
    if (prev == null) {
      return null;
    }
    return prev.get();
  }

  public Set<K> keySet() {
    return map.keySet();
  }

  public Collection<V> values() {
    Collection<ValueHolder<V>> values = map.values();
    return new AbstractCollection<V>() {
      @Override
      public Iterator<V> iterator() {
        return new Iterator<V>() {
          private final Iterator<ValueHolder<V>> valueIterator = values.iterator();

          @Override
          public boolean hasNext() {
            return valueIterator.hasNext();
          }

          @Override
          public V next() {
            ValueHolder<V> next = valueIterator.next();
            return next.get();
          }

          @Override
          public void remove() {
            valueIterator.remove();
          }
        };
      }

      @Override
      public int size() {
        return values.size();
      }
    };
  }

  public Set<Map.Entry<K, V>> entrySet() {
    return new AbstractSet<Entry<K, V>>() {
      private final Set<Entry<K, ValueHolder<V>>> entries = map.entrySet();

      @Override
      public Iterator<Entry<K, V>> iterator() {
        return new Iterator<Entry<K, V>>() {
          private final Iterator<Entry<K, ValueHolder<V>>> entryIterator = entries.iterator();

          @Override
          public boolean hasNext() {
            return entryIterator.hasNext();
          }

          @SuppressFBWarnings("SE_BAD_FIELD")
          @Override
          public Entry<K, V> next() {
            Entry<K, ValueHolder<V>> next = entryIterator.next();
            return new AbstractMap.SimpleEntry<K, V>(next.getKey(), next.getValue().get()) {
              @Override
              public V setValue(V value) {
                throw new UnsupportedOperationException();
              }
            };
          }

          @Override
          public void remove() {
            entryIterator.remove();
          }
        };
      }

      @Override
      public int size() {
        return entries.size();
      }
    };
  }

  public void updateExpirationTime(K key) {
    ValueHolder<V> value = map.get(key);
    if (value != null) {
      value.updateExpirationTime();
    }
  }

  @VisibleForTesting
  @Nullable
  ValueHolder<V> getValueHolder(K key) {
    return map.get(key);
  }

  @VisibleForTesting
  static class ValueHolder<V> {
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

    public long getLastUpdateTime() {
      return lastUpdateTime.get();
    }
  }
}
