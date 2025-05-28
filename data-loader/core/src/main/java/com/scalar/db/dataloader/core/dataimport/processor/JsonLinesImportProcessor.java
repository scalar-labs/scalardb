package com.scalar.db.dataloader.core.dataimport.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.dataloader.core.DataLoaderObjectMapper;
import com.scalar.db.dataloader.core.dataimport.datachunk.ImportDataChunk;
import com.scalar.db.dataloader.core.dataimport.datachunk.ImportRow;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A processor for importing data from JSON Lines (JSONL) formatted files.
 *
 * <p>This processor reads data from files where each line is a valid JSON object. It processes the
 * input file in chunks, allowing for parallel processing and batched transactions for efficient
 * data loading.
 *
 * <p>The processor uses a multi-threaded approach with:
 *
 * <ul>
 *   <li>A dedicated thread for reading data chunks from the input file
 *   <li>Multiple threads for processing data chunks in parallel
 *   <li>A queue-based system to manage data chunks between reader and processor threads
 * </ul>
 */
public class JsonLinesImportProcessor extends ImportProcessor {

  private static final DataLoaderObjectMapper OBJECT_MAPPER = new DataLoaderObjectMapper();
  private static final AtomicInteger dataChunkIdCounter = new AtomicInteger(0);

  /**
   * Creates a new JsonLinesImportProcessor with the specified parameters.
   *
   * @param params configuration parameters for the import processor
   */
  public JsonLinesImportProcessor(ImportProcessorParams params) {
    super(params);
  }

  /**
   * Reads data from the input file and creates data chunks for processing.
   *
   * <p>This method reads the input file line by line, parsing each line as a JSON object. It
   * accumulates rows until reaching the specified chunk size, then enqueues the chunk for
   * processing. Empty lines or invalid JSON objects are skipped.
   *
   * @param reader the BufferedReader for reading the input file
   * @param dataChunkSize the maximum number of rows to include in each data chunk
   * @param dataChunkQueue the queue where data chunks are placed for processing
   * @throws RuntimeException if there is an error reading the file or if the thread is interrupted
   */
  @Override
  protected void readDataChunks(
      BufferedReader reader, int dataChunkSize, BlockingQueue<ImportDataChunk> dataChunkQueue) {
    try {
      List<ImportRow> currentDataChunk = new ArrayList<>();
      int rowNumber = 1;
      String line;
      while ((line = reader.readLine()) != null) {
        JsonNode jsonNode = OBJECT_MAPPER.readTree(line);
        if (jsonNode == null || jsonNode.isEmpty()) continue;

        currentDataChunk.add(new ImportRow(rowNumber++, jsonNode));
        if (currentDataChunk.size() == dataChunkSize) {
          enqueueDataChunk(currentDataChunk, dataChunkQueue);
          currentDataChunk = new ArrayList<>();
        }
      }
      if (!currentDataChunk.isEmpty()) enqueueDataChunk(currentDataChunk, dataChunkQueue);
    } catch (IOException | InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(
          CoreError.DATA_LOADER_JSONLINES_FILE_READ_FAILED.buildMessage(e.getMessage()), e);
    }
  }

  /**
   * Enqueues a data chunk for processing.
   *
   * <p>Creates a new ImportDataChunk with a unique ID and adds it to the processing queue.
   *
   * @param dataChunk the list of ImportRows to be processed
   * @param queue the queue where the data chunk should be placed
   * @throws InterruptedException if the thread is interrupted while waiting to add to the queue
   */
  private void enqueueDataChunk(List<ImportRow> dataChunk, BlockingQueue<ImportDataChunk> queue)
      throws InterruptedException {
    int dataChunkId = dataChunkIdCounter.getAndIncrement();
    queue.put(ImportDataChunk.builder().dataChunkId(dataChunkId).sourceData(dataChunk).build());
  }
}
