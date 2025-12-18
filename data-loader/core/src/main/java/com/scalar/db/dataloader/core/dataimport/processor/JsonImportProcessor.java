package com.scalar.db.dataloader.core.dataimport.processor;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.scalar.db.dataloader.core.DataLoaderError;
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
  private final AtomicInteger dataChunkIdCounter = new AtomicInteger(0);

  /**
   * Creates a new JsonImportProcessor with the specified parameters.
   *
   * @param params Configuration parameters for the import processor
   */
  public JsonImportProcessor(ImportProcessorParams params) {
    super(params);
  }

  /**
   * Reads data from the JSON file and adds it to the processing queue.
   *
   * <p>This method reads the JSON file as an array of objects and enqueues all records as a single
   * data chunk. The method expects the JSON file to start with an array token '[' and end with ']'.
   *
   * <p>Empty or null JSON nodes are skipped during processing.
   *
   * @param reader the BufferedReader containing the JSON data
   * @param dataChunkQueue the queue where data chunks are placed for processing
   * @throws RuntimeException if there is an error reading the JSON file or if the thread is
   *     interrupted
   */
  @Override
  protected void readDataChunks(
      BufferedReader reader, BlockingQueue<ImportDataChunk> dataChunkQueue) {
    try (JsonParser jsonParser = new JsonFactory().createParser(reader)) {
      if (jsonParser.nextToken() != JsonToken.START_ARRAY) {
        throw new IOException(DataLoaderError.JSON_CONTENT_START_ERROR.buildMessage());
      }

      List<ImportRow> allRows = new ArrayList<>();
      int rowNumber = 1;
      while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
        JsonNode jsonNode = OBJECT_MAPPER.readTree(jsonParser);
        if (jsonNode == null || jsonNode.isEmpty()) continue;

        allRows.add(new ImportRow(rowNumber++, jsonNode));
      }
      if (!allRows.isEmpty()) enqueueDataChunk(allRows, dataChunkQueue);
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(
          DataLoaderError.JSON_FILE_READ_FAILED.buildMessage(e.getMessage()), e);
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
