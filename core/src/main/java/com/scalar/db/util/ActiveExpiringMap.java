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
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class ActiveExpiringMap<K, V> {
  private final ConcurrentMap<K, ValueHolder> map;
  private final long valueLifetimeMillis;
  private final long valueExpirationThreadIntervalMillis;
  private final BiConsumer<K, V> valueExpirationHandler;

  public ActiveExpiringMap(
      long valueLifetimeMillis,
      long valueExpirationThreadIntervalMillis,
      BiConsumer<K, V> valueExpirationHandler) {
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
                          ValueHolder value = map.remove(key);
                          if (value != null) {
                            valueExpirationHandler.accept(key, value.get());
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
    ValueHolder value = map.get(key);
    if (value == null) {
      return Optional.empty();
    }
    value.updateExpirationTime();
    return Optional.of(value.get());
  }

  public Optional<V> putIfAbsent(K key, V value) {
    ValueHolder prev = map.putIfAbsent(key, new ValueHolder(value));
    return prev == null ? Optional.empty() : Optional.of(prev.get());
  }

  public Optional<V> put(K key, V value) {
    ValueHolder prev = map.put(key, new ValueHolder(value));
    return prev == null ? Optional.empty() : Optional.of(prev.get());
  }

  public Optional<V> computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
    ValueHolder prev = map.computeIfAbsent(key, mappingFunction.andThen(ValueHolder::new));
    return prev == null ? Optional.empty() : Optional.of(prev.get());
  }

  public Optional<V> computeIfPresent(
      K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    ValueHolder prev =
        map.computeIfPresent(
            key,
            (k, oldValue) -> {
              V newValue = remappingFunction.apply(k, oldValue.get());
              return new ValueHolder(newValue);
            });
    return prev == null ? Optional.empty() : Optional.of(prev.get());
  }

  public boolean containsKey(K key) {
    return map.containsKey(key);
  }

  public boolean containsValue(V value) {
    return map.values().stream().map(ValueHolder::get).anyMatch(value::equals);
  }

  public Optional<V> remove(K key) {
    ValueHolder prev = map.remove(key);
    return prev == null ? Optional.empty() : Optional.of(prev.get());
  }

  public Set<K> keySet() {
    return map.keySet();
  }

  public Collection<V> values() {
    Collection<ValueHolder> values = map.values();
    return new AbstractCollection<V>() {
      @Override
      public Iterator<V> iterator() {
        return new Iterator<V>() {
          private final Iterator<ValueHolder> valueIterator = values.iterator();

          @Override
          public boolean hasNext() {
            return valueIterator.hasNext();
          }

          @Override
          public V next() {
            ValueHolder next = valueIterator.next();
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
      private final Set<Entry<K, ValueHolder>> entries = map.entrySet();

      @Override
      public Iterator<Entry<K, V>> iterator() {
        return new Iterator<Entry<K, V>>() {
          private final Iterator<Entry<K, ValueHolder>> entryIterator = entries.iterator();

          @Override
          public boolean hasNext() {
            return entryIterator.hasNext();
          }

          @SuppressFBWarnings("SE_BAD_FIELD")
          @Override
          public Entry<K, V> next() {
            Entry<K, ValueHolder> next = entryIterator.next();
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
    ValueHolder value = map.get(key);
    if (value != null) {
      value.updateExpirationTime();
    }
  }

  @VisibleForTesting
  @Nullable
  ValueHolder getValueHolder(K key) {
    return map.get(key);
  }

  @VisibleForTesting
  class ValueHolder {
    private final V value;
    private final AtomicLong lastUpdateTime = new AtomicLong();

    public ValueHolder(V value) {
      this.value = value;
      updateExpirationTime();
    }

    public void updateExpirationTime() {
      lastUpdateTime.set(System.currentTimeMillis());
    }

    public boolean isExpired() {
      return System.currentTimeMillis() - lastUpdateTime.get() >= valueLifetimeMillis;
    }

    public V get() {
      return value;
    }

    public long getLastUpdateTime() {
      return lastUpdateTime.get();
    }
  }
}
