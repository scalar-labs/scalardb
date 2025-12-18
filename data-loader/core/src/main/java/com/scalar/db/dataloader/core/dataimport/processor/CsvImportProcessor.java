package com.scalar.db.dataloader.core.dataimport.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalar.db.dataloader.core.DataLoaderError;
import com.scalar.db.dataloader.core.DataLoaderObjectMapper;
import com.scalar.db.dataloader.core.dataimport.datachunk.ImportDataChunk;
import com.scalar.db.dataloader.core.dataimport.datachunk.ImportRow;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
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
  private final AtomicInteger dataChunkIdCounter = new AtomicInteger(0);

  /**
   * Creates a new CsvImportProcessor with the specified parameters.
   *
   * @param params Configuration parameters for the import processor
   */
  public CsvImportProcessor(ImportProcessorParams params) {
    super(params);
  }

  /**
   * Reads and processes CSV data from the provided reader.
   *
   * <p>This method:
   *
   * <ul>
   *   <li>Reads the CSV header (custom or from file)
   *   <li>Validates each data row against the header
   *   <li>Converts rows to JSON format
   *   <li>Enqueues all rows as a single data chunk for processing
   * </ul>
   *
   * @param reader the BufferedReader containing CSV data
   * @param dataChunkQueue the queue where data chunks are placed for processing
   * @throws RuntimeException if there are errors reading the file or if interrupted
   */
  @Override
  protected void readDataChunks(
      BufferedReader reader, BlockingQueue<ImportDataChunk> dataChunkQueue) {
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
      List<ImportRow> allRows = new ArrayList<>();
      String line;
      int rowNumber = 1;
      while ((line = reader.readLine()) != null) {
        String[] dataArray = line.split(delimiter);
        if (headerArray.length != dataArray.length) {
          throw new IllegalArgumentException(
              DataLoaderError.CSV_DATA_MISMATCH.buildMessage(line, header));
        }
        JsonNode jsonNode = combineHeaderAndData(headerArray, dataArray);
        if (jsonNode.isEmpty()) continue;

        allRows.add(new ImportRow(rowNumber++, jsonNode));
      }
      if (!allRows.isEmpty()) enqueueDataChunk(allRows, dataChunkQueue);
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(
          DataLoaderError.CSV_FILE_READ_FAILED.buildMessage(e.getMessage()), e);
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
          DataLoaderError.CSV_FILE_HEADER_READ_FAILED.buildMessage(e.getMessage()), e);
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
