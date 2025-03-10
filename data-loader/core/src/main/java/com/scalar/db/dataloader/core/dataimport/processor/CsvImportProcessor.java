package com.scalar.db.dataloader.core.dataimport.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.dataloader.core.DataLoaderObjectMapper;
import com.scalar.db.dataloader.core.dataimport.datachunk.ImportDataChunk;
import com.scalar.db.dataloader.core.dataimport.datachunk.ImportDataChunkStatus;
import com.scalar.db.dataloader.core.dataimport.datachunk.ImportRow;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class CsvImportProcessor extends ImportProcessor {
  private static final DataLoaderObjectMapper OBJECT_MAPPER = new DataLoaderObjectMapper();
  private static final AtomicInteger dataChunkIdCounter = new AtomicInteger(0);

  public CsvImportProcessor(ImportProcessorParams params) {
    super(params);
  }

  /**
   * Processes the source data from the given import file.
   *
   * <p>This method reads data from the provided {@link BufferedReader}, processes it in chunks, and
   * batches transactions according to the specified sizes. The method returns a list of {@link
   * ImportDataChunkStatus} objects, each representing the status of a processed data chunk.
   *
   * @param dataChunkSize the number of records to include in each data chunk
   * @param transactionBatchSize the number of records to include in each transaction batch
   * @param reader the {@link BufferedReader} used to read the source file
   * @return a list of {@link ImportDataChunkStatus} objects indicating the processing status of
   *     each data chunk
   */
  @Override
  public List<ImportDataChunkStatus> process(
      int dataChunkSize, int transactionBatchSize, BufferedReader reader) {
    int numCores = Runtime.getRuntime().availableProcessors();
    ExecutorService dataChunkExecutor = Executors.newFixedThreadPool(numCores);
    BlockingQueue<ImportDataChunk> dataChunkQueue = new LinkedBlockingQueue<>();
    List<CompletableFuture<ImportDataChunkStatus>> dataChunkFutures = new CopyOnWriteArrayList<>();

    try {
      CompletableFuture<Void> readerFuture =
          CompletableFuture.runAsync(
              () -> readDataChunks(reader, dataChunkSize, dataChunkQueue), dataChunkExecutor);

      CompletableFuture<Void> processingFuture =
          readerFuture.thenRunAsync(
              () -> {
                while (!(dataChunkQueue.isEmpty() && readerFuture.isDone())) {
                  try {
                    ImportDataChunk dataChunk = dataChunkQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (dataChunk != null) {
                      dataChunkFutures.add(
                          CompletableFuture.supplyAsync(
                              () -> processDataChunk(dataChunk, transactionBatchSize, numCores),
                              dataChunkExecutor));
                    }
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(
                        CoreError.DATA_LOADER_DATA_CHUNK_PROCESS_FAILED.buildMessage(
                            e.getMessage()),
                        e);
                  }
                }
              },
              dataChunkExecutor);

      processingFuture.join();

      return CompletableFuture.allOf(dataChunkFutures.toArray(new CompletableFuture[0]))
          .thenApply(
              v ->
                  dataChunkFutures.stream()
                      .map(
                          f ->
                              f.exceptionally(
                                      e -> {
                                        System.err.println(
                                            "Data chunk processing failed: " + e.getMessage());
                                        return null;
                                      })
                                  .join())
                      .collect(Collectors.toList()))
          .join();
    } finally {
      dataChunkExecutor.shutdown();
      try {
        if (!dataChunkExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
          dataChunkExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        dataChunkExecutor.shutdownNow();
        Thread.currentThread().interrupt();
      }
      notifyAllDataChunksCompleted();
    }
  }

  private void readDataChunks(
      BufferedReader reader, int dataChunkSize, BlockingQueue<ImportDataChunk> dataChunkQueue) {
    try {
      String delimiter =
          Optional.of(params.getImportOptions().getDelimiter())
              .map(c -> Character.toString(c).trim())
              .filter(s -> !s.isEmpty())
              .orElse(",");

      String header =
          Optional.ofNullable(params.getImportOptions().getCustomHeaderRow())
              .orElseGet(() -> safeReadLine(reader));

      String[] headerArray = header.split(delimiter);
      List<ImportRow> currentDataChunk = new ArrayList<>();
      String line;
      int rowNumber = 1;
      while ((line = reader.readLine()) != null) {
        String[] dataArray = line.split(delimiter);
        if (headerArray.length != dataArray.length) {
          throw new IllegalArgumentException(
              CoreError.DATA_LOADER_CSV_DATA_MISMATCH.buildMessage(line, header));
        }
        JsonNode jsonNode = combineHeaderAndData(headerArray, dataArray);
        if (jsonNode.isEmpty()) continue;

        currentDataChunk.add(new ImportRow(rowNumber++, jsonNode));
        if (currentDataChunk.size() == dataChunkSize) {
          enqueueDataChunk(currentDataChunk, dataChunkQueue);
          currentDataChunk = new ArrayList<>();
        }
      }
      if (!currentDataChunk.isEmpty()) enqueueDataChunk(currentDataChunk, dataChunkQueue);
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(
          CoreError.DATA_LOADER_CSV_FILE_READ_FAILED.buildMessage(e.getMessage()), e);
    }
  }

  private void enqueueDataChunk(List<ImportRow> dataChunk, BlockingQueue<ImportDataChunk> queue)
      throws InterruptedException {
    int dataChunkId = dataChunkIdCounter.getAndIncrement();
    queue.put(ImportDataChunk.builder().dataChunkId(dataChunkId).sourceData(dataChunk).build());
  }

  private String safeReadLine(BufferedReader reader) {
    try {
      return reader.readLine();
    } catch (IOException e) {
      throw new UncheckedIOException(
          CoreError.DATA_LOADER_CSV_FILE_HEADER_READ_FAILED.buildMessage(e.getMessage()), e);
    }
  }

  private JsonNode combineHeaderAndData(String[] header, String[] data) {
    ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
    for (int i = 0; i < header.length; i++) {
      objectNode.put(header[i], data[i]);
    }
    return objectNode;
  }
}
