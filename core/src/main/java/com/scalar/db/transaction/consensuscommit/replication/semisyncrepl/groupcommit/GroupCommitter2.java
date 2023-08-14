package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.groupcommit;

public class GroupCommitter2<K, V> {
  /*
  private static final Logger logger = LoggerFactory.getLogger(GroupCommitter2.class);
  private final BlockingQueue<BufferedValues<K, V>> queueOfBufferedValues =
      new LinkedBlockingQueue<>();
  private final long retentionTimeInMillis;
  private final int numberOfRetentionValues;
  private final long expirationCheckIntervalInMillis;
  private final ExecutorService emitExecutorService;
  private final ExecutorService expirationCheckExecutorService;
  private final Emittable<V> emitter;
  @Nullable private BufferedValues<K, V> bufferedValues;

  private static class ValueSlot<K, V> {
    private final BufferedValues<K, V> parentBuffer;
    private final CompletableFuture<Void> completableFuture = new CompletableFuture<>();
    @Nullable private volatile V value;

    public ValueSlot(BufferedValues<K, V> parentBuffer) {
      this.parentBuffer = parentBuffer;
    }

    public K getKey() {
      return parentBuffer.key;
    }

    public synchronized void setValue(V value) throws GroupCommitException {
      if (value == null) {
        throw new AssertionError("'value' is null. key=" + getKey());
      }
      if (parentBuffer.isDone()) {
        throw new GroupCommitAlreadyClosedException(
            String.format(
                "The parent buffer is already closed. parentBuffer:%s, value:%s",
                parentBuffer, value));
      }
      this.value = value;
      parentBuffer.notifyOfReadyValue(this);
    }

    public void waitUntilEmit() throws GroupCommitException, GroupCommitCascadeException {
      try {
        completableFuture.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new GroupCommitException("Group commit was interrupted", e);
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof GroupCommitCascadeException) {
          throw (GroupCommitCascadeException) cause;
        } else if (cause instanceof GroupCommitException) {
          throw (GroupCommitException) cause;
        }
        throw new GroupCommitException("Group commit failed", e);
      }
    }

    public void abort(Throwable e) {
      parentBuffer.abortAll(e);
    }

    public void remove(K keyCandidate) {
      parentBuffer.removeValueSlot(this, keyCandidate);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("value", value).toString();
    }
  }

  private static class BufferedValues<K, V> {
    private final ExecutorService executorService;
    private final Emittable<V> emitter;
    private final int capacity;
    private final AtomicReference<Integer> size = new AtomicReference<>();
    private final K key;
    public final Instant expiredAt;
    private final AtomicBoolean done = new AtomicBoolean();
    private final List<ValueSlot<K, V>> valueSlots;
    private final Set<ValueSlot<K, V>> readyValueSlots;

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("key", key)
          .add("hashCode", hashCode())
          .add("expiredAt", expiredAt)
          .add("done", isDone())
          .add("ready", isReady())
          .add("sizeFixed", isSizeFixed())
          .add("valueSlots.size", valueSlots.size())
          .add("readyValueSlots.size", readyValueSlots.size())
          .toString();
    }

    BufferedValues(
        ExecutorService executorService,
        Emittable<V> emitter,
        long retentionTimeInMillis,
        K key,
        int capacity) {
      this.executorService = executorService;
      this.emitter = emitter;
      this.capacity = capacity;
      this.expiredAt = Instant.now().plusMillis(retentionTimeInMillis);
      if (key == null) {
        throw new IllegalArgumentException("`key` can't be null");
      }
      this.key = key;
      this.valueSlots = new ArrayList<>(capacity);
      this.readyValueSlots = new HashSet<>(capacity);
    }

    public synchronized boolean noMoreSlot() {
      return valueSlots.size() >= capacity;
    }

    public synchronized ValueSlot<K, V> getValueSlot() throws GroupCommitAlreadyClosedException {
      if (isSizeFixed()) {
        throw new GroupCommitAlreadyClosedException(
            "The size of 'valueSlot' is already fixed. buffer:" + this);
      }
      ValueSlot<K, V> valueSlot = new ValueSlot<>(this);
      valueSlots.add(valueSlot);
      if (noMoreSlot()) {
        fixSize();
      }
      return valueSlot;
    }

    public synchronized void fixSize() {
      ////// FIXME: DEBUG
      logger.info("GC FIX-SIZE: key={}", key);
      ////// FIXME: DEBUG
      // Current ValueSlot that `index` is pointing is not used yet.
      size.set(valueSlots.size());
      emitIfReady();
    }

    public synchronized boolean isSizeFixed() {
      return size.get() != null;
    }

    public synchronized boolean isReady() {
      return isSizeFixed() && readyValueSlots.size() >= size.get();
    }

    public synchronized boolean isDone() {
      return done.get();
    }

    public synchronized void removeValueSlot(ValueSlot<K, V> valueSlot, K keyCandidate) {
      //      if (!isSizeFixed()) {
      valueSlots.remove(valueSlot);
      if (size.get() != null) {
        size.set(size.get() - 1);
      }
      readyValueSlots.remove(valueSlot);
      //      }
      ////// FIXME: DEBUG
      logger.info(
          "REMOVE VS: key={}, keyCandidate={}, valueSlot={}, valueSlotsSize={}, size={}, readyValueSlotsSize={}",
          key,
          keyCandidate,
          valueSlot,
          valueSlots.size(),
          size.get(),
          readyValueSlots.size());
      ////// FIXME: DEBUG
      emitIfReady();
    }

    public synchronized void emitIfReady() {
      if (isDone()) {
        return;
      }

      if (isReady()) {
        if (valueSlots.isEmpty()) {
          logger.warn("'valueSlots' is empty. Nothing to do. bufferedValue:{}", this);
          done.set(true);
          return;
        }
        executorService.execute(
            () -> {
              try {
                ////// FIXME: DEBUG
                logger.info("EMIT: buffer={}", this);
                ////// FIXME: DEBUG
                long startEmit = System.currentTimeMillis();
                emitter.execute(
                    valueSlots.stream().map(vs -> vs.value).collect(Collectors.toList()));
                logger.info(
                    "Emitted (thread_id:{}, num_of_values:{}): {} ms",
                    Thread.currentThread().getId(),
                    size.get(),
                    System.currentTimeMillis() - startEmit);

                long startNotify = System.currentTimeMillis();
                valueSlots.forEach(vf -> vf.completableFuture.complete(null));
                logger.info(
                    "Notified (thread_id:{}, num_of_values:{}): {} ms",
                    Thread.currentThread().getId(),
                    size.get(),
                    System.currentTimeMillis() - startNotify);
              } catch (Throwable e) {
                logger.error("Group commit failed", e);
                valueSlots.forEach(
                    vf ->
                        vf.completableFuture.completeExceptionally(
                            new GroupCommitException(
                                "Group commit failed. Aborting all the values", e)));
              }
            });
        done.set(true);
      }
    }

    public synchronized void notifyOfReadyValue(ValueSlot<K, V> valueSlot) {
      readyValueSlots.add(valueSlot);
      emitIfReady();
    }

    public synchronized void abortAll(Throwable cause) {
      for (ValueSlot<K, V> kv : valueSlots) {
        kv.completableFuture.completeExceptionally(
            new GroupCommitCascadeException(
                "One of the fetched items failed in group commit. The other items have the same key associated with the failure. All the items will fail",
                cause));
        done.set(true);
      }
    }
  }

  public GroupCommitter2(
      String label,
      long retentionTimeInMillis,
      int numberOfRetentionValues,
      long expirationCheckIntervalInMillis,
      int numberOfThreads,
      Emittable<V> emitter) {
    this.retentionTimeInMillis = retentionTimeInMillis;
    this.numberOfRetentionValues = numberOfRetentionValues;
    this.expirationCheckIntervalInMillis = expirationCheckIntervalInMillis;
    this.emitter = emitter;

    this.emitExecutorService =
        Executors.newFixedThreadPool(
            numberOfThreads,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(label + "-group-commit-emit-%d")
                .build());

    this.expirationCheckExecutorService =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(label + "-group-commit-expire-%d")
                .build());

    startExpirationCheckExecutorService();
  }

  ////////// FIXME: DEBUG LOG
  private volatile long lastDebugPrint = 0;
  ////////// FIXME: DEBUG LOG
  private boolean dequeueAndHandleBufferedValues() {
    BufferedValues<K, V> bufferedValues = queueOfBufferedValues.peek();
    Long retryWaitInMillis = null;

    ////////// FIXME: DEBUG LOG
    if (lastDebugPrint + 1000 < System.currentTimeMillis()) {
      logger.info("QUEUE STATUS: size={}", queueOfBufferedValues.size());
      lastDebugPrint = System.currentTimeMillis();
    }
    ////////// FIXME: DEBUG LOG

    if (bufferedValues == null) {
      retryWaitInMillis = expirationCheckIntervalInMillis;
    } else if (bufferedValues.isSizeFixed()) {
      // Already expired. Nothing to do
      ////////// FIXME: DEBUG LOG
      if (bufferedValues.expiredAt.isBefore(Instant.now().minusMillis(5000))) {
        logger.info(
            "TOO OLD BUFFER: buffer.key={}, buffer.values={}",
            bufferedValues.key,
            bufferedValues.valueSlots);
      }
      ////////// FIXME: DEBUG LOG
    } else {
      Instant now = Instant.now();
      if (now.isAfter(bufferedValues.expiredAt)) {
        // Expired
        bufferedValues.fixSize();
      } else {
        // Not expired. Retry
        retryWaitInMillis =
            Math.max(
                bufferedValues.expiredAt.minusMillis(now.toEpochMilli()).toEpochMilli(),
                expirationCheckIntervalInMillis);
      }
    }

    if (retryWaitInMillis != null) {
      ////////// FIXME: DEBUG LOG
      // logger.info("FETCHED BUFFER(RETRY): buffer={}, wait={}", bufferedValues,
      // retryWaitInMillis);
      ////////// FIXME: DEBUG LOG
      try {
        TimeUnit.MILLISECONDS.sleep(retryWaitInMillis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Interrupted", e);
        return false;
      }
    } else {
      ////////// FIXME: DEBUG LOG
      logger.info("FETCHED BUFFER(REMOVE): buffer={}", bufferedValues);
      ////////// FIXME: DEBUG LOG
      BufferedValues<K, V> removed = queueOfBufferedValues.poll();
      if (removed == null || !removed.equals(bufferedValues)) {
        logger.warn(
            "The queue returned an inconsistent return value. expected:{}, actual:{}",
            bufferedValues,
            removed);
      }
    }
    return true;
  }

  private void startExpirationCheckExecutorService() {
    expirationCheckExecutorService.execute(
        () -> {
          while (!expirationCheckExecutorService.isShutdown()) {
            if (!dequeueAndHandleBufferedValues()) {
              break;
            }
          }
        });
  }

  private synchronized ValueSlot<K, V> getValueSlot(K keyCandidate)
      throws GroupCommitAlreadyClosedException {
    if (bufferedValues == null || bufferedValues.noMoreSlot() || bufferedValues.isDone()) {
      bufferedValues =
          new BufferedValues<>(
              emitExecutorService,
              emitter,
              retentionTimeInMillis,
              keyCandidate,
              numberOfRetentionValues);
      queueOfBufferedValues.add(bufferedValues);
    }
    return bufferedValues.getValueSlot();
  }

  private ValueSlot<K, V> getValueSlotContainingValue(
      K keyCandidate, ValueGenerator<K, V> valueGeneratorFromUniqueKey)
      throws GroupCommitException {
    ValueSlot<K, V> valueSlot = null;
    try {
      valueSlot = getValueSlot(keyCandidate);

      long start = System.currentTimeMillis();
      V value = valueGeneratorFromUniqueKey.execute(valueSlot.getKey());
      logger.info(
          "Created value(thread_id:{}): {} ms, bufferKey={}, key={}",
          Thread.currentThread().getId(),
          System.currentTimeMillis() - start,
          valueSlot.parentBuffer.key,
          keyCandidate);
      valueSlot.setValue(value);
      return valueSlot;
    } catch (GroupCommitAlreadyClosedException e) {
      ///////// FIXME: DEBUG
      logger.info(
          "FAILED TO CREATE VALUE #1: , bufferKey={}, key={}",
          valueSlot == null ? "NULL" : valueSlot.parentBuffer.key,
          keyCandidate);
      ///////// FIXME: DEBUG

      if (valueSlot != null) {
        valueSlot.remove(keyCandidate);
      }
      throw e;
    } catch (Throwable e) {
      ///////// FIXME: DEBUG
      logger.info(
          "FAILED TO CREATE VALUE #2: , bufferKey={}, key={}",
          valueSlot == null ? "NULL" : valueSlot.parentBuffer.key,
          keyCandidate);
      ///////// FIXME: DEBUG
      GroupCommitException gce =
          new GroupCommitException(
              String.format(
                  "Failed to prepare a value for group commit. keyCandidate: %s", keyCandidate),
              e);
      // valueSlot.abort(gce);
      if (valueSlot != null) {
        valueSlot.remove(keyCandidate);
      }
      throw gce;
    }
  }

  public void addValue(K keyCandidate, ValueGenerator<K, V> valueGeneratorFromUniqueKey)
      throws GroupCommitException {
    ValueSlot<K, V> valueSlot =
        getValueSlotContainingValue(keyCandidate, valueGeneratorFromUniqueKey);

    long start = System.currentTimeMillis();
    valueSlot.waitUntilEmit();
    logger.info(
        "Waited(thread_id:{}): {} ms",
        Thread.currentThread().getId(),
        System.currentTimeMillis() - start);
  }
   */
}
