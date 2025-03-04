package com.scalar.db.dataloader.core.dataimport.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.scalar.db.dataloader.core.DataLoaderObjectMapper;
import com.scalar.db.dataloader.core.dataimport.datachunk.ImportDataChunk;
import com.scalar.db.dataloader.core.dataimport.datachunk.ImportDataChunkStatus;
import com.scalar.db.dataloader.core.dataimport.datachunk.ImportRow;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class JsonLinesImportProcessor extends ImportProcessor {

  private static final DataLoaderObjectMapper OBJECT_MAPPER = new DataLoaderObjectMapper();
  private static final AtomicInteger dataChunkIdCounter = new AtomicInteger(0);

  public JsonLinesImportProcessor(ImportProcessorParams params) {
    super(params);
  }

  /**
   * Processes the source data from the given import file.
   * <p>
   * This method reads data from the provided {@link BufferedReader}, processes it in chunks,
   * and batches transactions according to the specified sizes. The method returns a list of
   * {@link ImportDataChunkStatus} objects, each representing the status of a processed data chunk.
   * </p>
   *
   * @param dataChunkSize the number of records to include in each data chunk
   * @param transactionBatchSize the number of records to include in each transaction batch
   * @param reader the {@link BufferedReader} used to read the source file
   * @return a list of {@link ImportDataChunkStatus} objects indicating the processing status of each data chunk
   * @throws ExecutionException if an error occurs during asynchronous processing
   * @throws InterruptedException if the processing is interrupted
   */
  @Override
  public List<ImportDataChunkStatus> process(
      int dataChunkSize, int transactionBatchSize, BufferedReader reader)
      throws ExecutionException, InterruptedException {
    int numCores = Runtime.getRuntime().availableProcessors();

    // Create a thread pool for processing data batches
    ExecutorService dataChunkExecutor = Executors.newFixedThreadPool(numCores);

    // Create a queue to hold data batches
    Queue<ImportDataChunk> dataChunkQueue = new LinkedList<>();

    // Create a thread to read JSON lines and populate data batches
    Thread readerThread =
        new Thread(
            () -> {
              try {
                List<ImportRow> currentDataChunk = new ArrayList<>();
                int rowNumber = 1;
                String line;
                while ((line = reader.readLine()) != null) {
                  JsonNode jsonNode = OBJECT_MAPPER.readTree(line);
                  // TODO: do something with the null jsonNode
                  if (jsonNode == null || jsonNode.isEmpty()) {
                    continue;
                  }

                  ImportRow importRow = new ImportRow(rowNumber, jsonNode);
                  currentDataChunk.add(importRow);

                  // If the data chunk is full, add it to the queue
                  if (currentDataChunk.size() == dataChunkSize) {
                    int dataChunkId = dataChunkIdCounter.getAndIncrement();
                    ImportDataChunk importDataChunk =
                        ImportDataChunk.builder()
                            .dataChunkId(dataChunkId)
                            .sourceData(currentDataChunk)
                            .build();
                    dataChunkQueue.offer(importDataChunk);
                    currentDataChunk = new ArrayList<>();
                  }
                  rowNumber++;
                }

                // Add the last data chunk to the queue
                if (!currentDataChunk.isEmpty()) {
                  int dataChunkId = dataChunkIdCounter.getAndIncrement();
                  ImportDataChunk importDataChunk =
                      ImportDataChunk.builder()
                          .dataChunkId(dataChunkId)
                          .sourceData(currentDataChunk)
                          .build();
                  dataChunkQueue.offer(importDataChunk);
                }
              } catch (IOException e) {
                // TODO: handle this exception
                throw new RuntimeException(e);
              }
            });
    readerThread.start();
    try {
      // Wait for readerThread to finish
      readerThread.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      System.err.println("Main thread was interrupted.");
    }
    // Process data chunks in parallel
    List<Future<?>> dataChunkFutures = new ArrayList<>();
    while (!dataChunkQueue.isEmpty()) {
      ImportDataChunk dataChunk = dataChunkQueue.poll();
      Future<?> dataChunkFuture =
          dataChunkExecutor.submit(
              () -> processDataChunk(dataChunk, transactionBatchSize, numCores));
      dataChunkFutures.add(dataChunkFuture);
    }

    List<ImportDataChunkStatus> importDataChunkStatusList = new ArrayList<>();
    // Wait for all data chunk threads to complete
    for (Future<?> dataChunkFuture : dataChunkFutures) {
      importDataChunkStatusList.add((ImportDataChunkStatus) dataChunkFuture.get());
    }
    dataChunkExecutor.shutdown();
    notifyAllDataChunksCompleted();
    return importDataChunkStatusList;
  }
}
