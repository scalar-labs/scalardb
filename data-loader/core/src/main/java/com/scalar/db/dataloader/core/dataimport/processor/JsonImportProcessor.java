package com.scalar.db.dataloader.core.dataimport.processor;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.dataloader.core.DataLoaderObjectMapper;
import com.scalar.db.dataloader.core.dataimport.datachunk.ImportDataChunk;
import com.scalar.db.dataloader.core.dataimport.datachunk.ImportDataChunkStatus;
import com.scalar.db.dataloader.core.dataimport.datachunk.ImportRow;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A processor for importing JSON data into the database.
 *
 * <p>This processor handles JSON files that contain an array of JSON objects. Each object in the
 * array represents a row to be imported into the database. The processor reads the JSON file,
 * splits it into chunks of configurable size, and processes these chunks in parallel using multiple
 * threads.
 *
 * <p>The processing is done in two main phases:
 *
 * <ul>
 *   <li>Reading phase: The JSON file is read and split into chunks
 *   <li>Processing phase: Each chunk is processed independently and imported into the database
 * </ul>
 *
 * <p>The processor uses a producer-consumer pattern where one thread reads the JSON file and
 * produces data chunks, while a pool of worker threads consumes and processes these chunks.
 */
public class JsonImportProcessor extends ImportProcessor {

  private static final DataLoaderObjectMapper OBJECT_MAPPER = new DataLoaderObjectMapper();
  private static final AtomicInteger dataChunkIdCounter = new AtomicInteger(0);
  private static final int MAX_QUEUE_SIZE = 10;

  public JsonImportProcessor(ImportProcessorParams params) {
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
   * @return a map of {@link ImportDataChunkStatus} objects indicating the processing status of each
   *     data chunk
   */
  @Override
  public ConcurrentHashMap<Integer, ImportDataChunkStatus> process(
      int dataChunkSize, int transactionBatchSize, BufferedReader reader) {
    int numCores = Runtime.getRuntime().availableProcessors();
    ExecutorService dataChunkExecutor = Executors.newFixedThreadPool(numCores);
    BlockingQueue<ImportDataChunk> dataChunkQueue = new LinkedBlockingQueue<>(MAX_QUEUE_SIZE);

    try {
      CompletableFuture<Void> readerFuture =
          CompletableFuture.runAsync(
              () -> readDataChunks(reader, dataChunkSize, dataChunkQueue), dataChunkExecutor);

      ConcurrentHashMap<Integer, ImportDataChunkStatus> result = new ConcurrentHashMap<>();

      while (!(dataChunkQueue.isEmpty() && readerFuture.isDone())) {
        ImportDataChunk dataChunk = dataChunkQueue.poll(100, TimeUnit.MILLISECONDS);
        if (dataChunk != null) {
          ImportDataChunkStatus status =
              processDataChunk(dataChunk, transactionBatchSize, numCores);
          result.put(status.getDataChunkId(), status);
        }
      }

      readerFuture.join();
      return result;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(
          CoreError.DATA_LOADER_DATA_CHUNK_PROCESS_FAILED.buildMessage(e.getMessage()), e);
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

  /**
   * Reads data chunks from the JSON file and adds them to the processing queue.
   *
   * <p>This method reads the JSON file as an array of objects, creating data chunks of the
   * specified size. Each chunk is then added to the queue for processing. The method expects the
   * JSON file to start with an array token '[' and end with ']'.
   *
   * <p>Empty or null JSON nodes are skipped during processing.
   *
   * @param reader the BufferedReader containing the JSON data
   * @param dataChunkSize the maximum number of records to include in each chunk
   * @param dataChunkQueue the queue where data chunks are placed for processing
   * @throws RuntimeException if there is an error reading the JSON file or if the thread is
   *     interrupted
   */
  private void readDataChunks(
      BufferedReader reader, int dataChunkSize, BlockingQueue<ImportDataChunk> dataChunkQueue) {
    try (JsonParser jsonParser = new JsonFactory().createParser(reader)) {
      if (jsonParser.nextToken() != JsonToken.START_ARRAY) {
        throw new IOException(CoreError.DATA_LOADER_JSON_CONTENT_START_ERROR.buildMessage());
      }

      List<ImportRow> currentDataChunk = new ArrayList<>();
      int rowNumber = 1;
      while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
        JsonNode jsonNode = OBJECT_MAPPER.readTree(jsonParser);
        if (jsonNode == null || jsonNode.isEmpty()) continue;

        currentDataChunk.add(new ImportRow(rowNumber++, jsonNode));
        if (currentDataChunk.size() == dataChunkSize) {
          enqueueDataChunk(currentDataChunk, dataChunkQueue);
          currentDataChunk = new ArrayList<>();
        }
      }
      if (!currentDataChunk.isEmpty()) enqueueDataChunk(currentDataChunk, dataChunkQueue);
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(
          CoreError.DATA_LOADER_JSON_FILE_READ_FAILED.buildMessage(e.getMessage()), e);
    }
  }

  /**
   * Adds a data chunk to the processing queue.
   *
   * <p>This method creates a new ImportDataChunk with a unique ID and the provided data, then adds
   * it to the processing queue. The ID is generated using an atomic counter to ensure thread
   * safety.
   *
   * @param dataChunk the list of ImportRow objects to be processed
   * @param queue the queue where the data chunk will be added
   * @throws InterruptedException if the thread is interrupted while waiting to add to the queue
   */
  private void enqueueDataChunk(List<ImportRow> dataChunk, BlockingQueue<ImportDataChunk> queue)
      throws InterruptedException {
    int dataChunkId = dataChunkIdCounter.getAndIncrement();
    queue.put(ImportDataChunk.builder().dataChunkId(dataChunkId).sourceData(dataChunk).build());
  }
}
