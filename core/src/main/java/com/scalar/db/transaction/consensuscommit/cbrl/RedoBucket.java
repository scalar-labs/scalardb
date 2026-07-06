package com.scalar.db.transaction.consensuscommit.cbrl;

import com.scalar.db.transaction.consensuscommit.proto.v1.Entry;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A single pass-1 bucket: the redo ops for a disjoint subset of {@link RecordKey}s. All ops sharing
 * a key land in exactly one bucket (see {@link RecordShuffler}), so a pass-2 worker owns whole
 * buckets and there is no intra-key concurrency — no CAS, no locks (the design's core
 * simplification over SSR). Append order within a bucket is irrelevant: the replay primitive is
 * cursor-driven and order-independent within a key.
 *
 * <p>Ops are <b>spilled to a temp file</b> rather than held in the heap, so a restore of a large
 * backup window never materializes the whole redo stream at once. Pass 1 appends each op ({@link
 * #add}); {@link #seal} flushes and closes the writer; pass 2 reads one bucket back at a time
 * ({@link #read}), so at most one bucket's ops per worker are resident. The file is re-readable
 * (idempotent replay may read a bucket more than once) and is removed by {@link #delete}, which the
 * caller — not {@code apply} — owns, so a bucket can be re-read.
 */
final class RedoBucket {
  private static final Logger logger = LoggerFactory.getLogger(RedoBucket.class);

  private final Path file;
  private final DataOutputStream out;
  private int size;
  private boolean sealed;

  RedoBucket() {
    try {
      file = Files.createTempFile("cbrl-redo-bucket-", ".bin");
      out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(file)));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create a redo bucket spill file", e);
    }
  }

  void add(RedoOperation operation) {
    if (sealed) {
      throw new IllegalStateException("Cannot add to a sealed bucket");
    }
    // Only txId, commit time, and the entry bytes are stored; the key and prev_tx_id are derived
    // from the entry on read, exactly as in the RedoOperation constructor.
    byte[] txId = operation.txId().getBytes(StandardCharsets.UTF_8);
    byte[] entry = operation.entry().toByteArray();
    try {
      out.writeInt(txId.length);
      out.write(txId);
      out.writeLong(operation.committedAt());
      out.writeInt(entry.length);
      out.write(entry);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to spill a redo op to " + file, e);
    }
    size++;
  }

  /** Flushes and closes the writer so the bucket can be read. Idempotent. */
  void seal() {
    if (sealed) {
      return;
    }
    try {
      out.close();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to seal redo bucket " + file, e);
    }
    sealed = true;
  }

  int size() {
    return size;
  }

  /**
   * Reads the whole bucket back from its spill file — one bucket's worth of ops in the heap at a
   * time, the memory bound pass 2 relies on. Must be sealed first; re-readable.
   */
  List<RedoOperation> read() {
    if (!sealed) {
      throw new IllegalStateException("Cannot read an unsealed bucket");
    }
    List<RedoOperation> operations = new ArrayList<>(size);
    try (DataInputStream in =
        new DataInputStream(new BufferedInputStream(Files.newInputStream(file)))) {
      for (int i = 0; i < size; i++) {
        byte[] txId = new byte[in.readInt()];
        in.readFully(txId);
        long committedAt = in.readLong();
        byte[] entry = new byte[in.readInt()];
        in.readFully(entry);
        operations.add(
            new RedoOperation(
                new String(txId, StandardCharsets.UTF_8), Entry.parseFrom(entry), committedAt));
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read redo bucket " + file, e);
    }
    return operations;
  }

  /** Removes the spill file, closing the writer first if the bucket was never sealed. */
  void delete() {
    if (!sealed) {
      try {
        out.close();
      } catch (IOException e) {
        logger.warn("Failed to close redo bucket writer before deleting {}", file, e);
      }
      sealed = true;
    }
    try {
      Files.deleteIfExists(file);
    } catch (IOException e) {
      // Best effort; the OS reclaims the temp file eventually.
      logger.warn("Failed to delete redo bucket spill file {}", file, e);
    }
  }
}
