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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A processor for importing CSV data into the database.
 *
 * <p>This class handles the processing of CSV files by:
 *
 * <ul>
 *   <li>Reading and parsing CSV data with configurable delimiters
 *   <li>Processing data in configurable chunk sizes for efficient batch processing
 *   <li>Supporting parallel processing using multiple threads
 *   <li>Converting CSV rows into JSON format for database import
 * </ul>
 *
 * <p>The processor supports custom headers and validates that each data row matches the header
 * structure before processing.
 */
public class CsvImportProcessor extends ImportProcessor {
  private static final DataLoaderObjectMapper OBJECT_MAPPER = new DataLoaderObjectMapper();
  private static final AtomicInteger dataChunkIdCounter = new AtomicInteger(0);

  /**
   * Creates a new CsvImportProcessor with the specified parameters.
   *
   * @param params Configuration parameters for the import processor
   */
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
   * @return a map of {@link ImportDataChunkStatus} objects indicating the processing status of each
   *     data chunk
   */
  @Override
  public ConcurrentHashMap<Integer, ImportDataChunkStatus> process(
      int dataChunkSize, int transactionBatchSize, BufferedReader reader) {
    ExecutorService dataChunkExecutor = Executors.newSingleThreadExecutor();
    BlockingQueue<ImportDataChunk> dataChunkQueue =
        new LinkedBlockingQueue<>(params.getImportOptions().getDataChunkQueueSize());

    try {
      CompletableFuture<Void> readerFuture =
          CompletableFuture.runAsync(
              () -> readDataChunks(reader, dataChunkSize, dataChunkQueue), dataChunkExecutor);

      ConcurrentHashMap<Integer, ImportDataChunkStatus> result = new ConcurrentHashMap<>();

      while (!(dataChunkQueue.isEmpty() && readerFuture.isDone())) {
        ImportDataChunk dataChunk = dataChunkQueue.poll(100, TimeUnit.MILLISECONDS);
        if (dataChunk != null) {
          ImportDataChunkStatus status =
              processDataChunk(
                  dataChunk, transactionBatchSize, params.getImportOptions().getMaxThreads());
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
   * Reads and processes CSV data in chunks from the provided reader.
   *
   * <p>This method:
   *
   * <ul>
   *   <li>Reads the CSV header (custom or from file)
   *   <li>Validates each data row against the header
   *   <li>Converts rows to JSON format
   *   <li>Batches rows into data chunks
   *   <li>Enqueues chunks for processing
   * </ul>
   *
   * @param reader the BufferedReader containing CSV data
   * @param dataChunkSize the number of rows to include in each chunk
   * @param dataChunkQueue the queue where data chunks are placed for processing
   * @throws RuntimeException if there are errors reading the file or if interrupted
   */
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

  /**
   * Adds a completed data chunk to the processing queue.
   *
   * @param dataChunk the list of ImportRows to be processed
   * @param queue the queue where the chunk should be placed
   * @throws InterruptedException if the thread is interrupted while waiting to add to the queue
   */
  private void enqueueDataChunk(List<ImportRow> dataChunk, BlockingQueue<ImportDataChunk> queue)
      throws InterruptedException {
    int dataChunkId = dataChunkIdCounter.getAndIncrement();
    queue.put(ImportDataChunk.builder().dataChunkId(dataChunkId).sourceData(dataChunk).build());
  }

  /**
   * Safely reads a line from the BufferedReader, handling IOExceptions.
   *
   * @param reader the BufferedReader to read from
   * @return the line read from the reader
   * @throws UncheckedIOException if an IOException occurs while reading
   */
  private String safeReadLine(BufferedReader reader) {
    try {
      return reader.readLine();
    } catch (IOException e) {
      throw new UncheckedIOException(
          CoreError.DATA_LOADER_CSV_FILE_HEADER_READ_FAILED.buildMessage(e.getMessage()), e);
    }
  }

  /**
   * Combines header fields with data values to create a JSON object.
   *
   * @param header array of header field names
   * @param data array of data values corresponding to the header fields
   * @return a JsonNode containing the combined header-value pairs
   */
  private JsonNode combineHeaderAndData(String[] header, String[] data) {
    ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
    for (int i = 0; i < header.length; i++) {
      objectNode.put(header[i], data[i]);
    }
    return objectNode;
  }
}
