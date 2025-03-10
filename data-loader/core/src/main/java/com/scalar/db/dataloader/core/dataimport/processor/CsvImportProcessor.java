package com.scalar.db.dataloader.core.dataimport.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
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

    CompletableFuture<Void> readerFuture =
        CompletableFuture.runAsync(() -> readDataChunks(reader, dataChunkSize, dataChunkQueue));

    List<CompletableFuture<ImportDataChunkStatus>> dataChunkFutures = new ArrayList<>();
    readerFuture
        .thenRun(
            () -> {
              ImportDataChunk dataChunk;
              while ((dataChunk = dataChunkQueue.poll()) != null) {
                ImportDataChunk finalDataChunk = dataChunk;
                CompletableFuture<ImportDataChunkStatus> future =
                    CompletableFuture.supplyAsync(
                        () -> processDataChunk(finalDataChunk, transactionBatchSize, numCores),
                        dataChunkExecutor);
                dataChunkFutures.add(future);
              }
            })
        .join();

    List<ImportDataChunkStatus> importDataChunkStatusList =
        dataChunkFutures.stream().map(CompletableFuture::join).collect(Collectors.toList());

    dataChunkExecutor.shutdown();
    notifyAllDataChunksCompleted();
    return importDataChunkStatusList;
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
          throw new IllegalArgumentException("CSV row does not match header length.");
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
      throw new RuntimeException("Failed to read CSV file", e);
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
      throw new UncheckedIOException("Failed to read header line", e);
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
