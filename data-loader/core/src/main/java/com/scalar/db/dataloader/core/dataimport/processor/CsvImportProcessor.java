package com.scalar.db.dataloader.core.dataimport.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalar.db.dataloader.core.DataLoaderError;
import com.scalar.db.dataloader.core.DataLoaderObjectMapper;
import com.scalar.db.dataloader.core.dataimport.ImportRow;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A processor for importing CSV data into the database.
 *
 * <p>This class handles the processing of CSV files by:
 *
 * <ul>
 *   <li>Reading and parsing CSV data with configurable delimiters
 *   <li>Processing data in batches according to transaction batch size
 *   <li>Converting CSV rows into JSON format for database import
 * </ul>
 *
 * <p>The processor supports custom headers and validates that each data row matches the header
 * structure before processing.
 */
public class CsvImportProcessor extends ImportProcessor {
  private static final DataLoaderObjectMapper OBJECT_MAPPER = new DataLoaderObjectMapper();

  private String[] headerArray;
  private String delimiter;
  private int currentRowNumber = 1;
  private boolean initialized = false;

  /**
   * Creates a new CsvImportProcessor with the specified parameters.
   *
   * @param params Configuration parameters for the import processor
   */
  public CsvImportProcessor(ImportProcessorParams params) {
    super(params);
  }

  /**
   * Reads the next batch of records from the CSV file.
   *
   * <p>This method:
   *
   * <ul>
   *   <li>Initializes the header on first call (custom or from file)
   *   <li>Reads up to batchSize rows from the file
   *   <li>Validates each data row against the header
   *   <li>Converts rows to JSON format
   * </ul>
   *
   * @param reader the BufferedReader containing CSV data
   * @param batchSize the maximum number of records to read
   * @return a list of ImportRow objects, or an empty list if no more records
   * @throws RuntimeException if there are errors reading the file
   */
  @Override
  protected List<ImportRow> readNextBatch(BufferedReader reader, int batchSize) {
    List<ImportRow> batch = new ArrayList<>();
    try {
      // Initialize header and delimiter on first call
      if (!initialized) {
        delimiter =
            Optional.of(params.getImportOptions().getDelimiter())
                .map(c -> Character.toString(c).trim())
                .filter(s -> !s.isEmpty())
                .orElse(",");

        String header =
            Optional.ofNullable(params.getImportOptions().getCustomHeaderRow())
                .orElseGet(() -> safeReadLine(reader));

        headerArray = header.split(delimiter);
        initialized = true;
      }

      String line;
      while (batch.size() < batchSize && (line = reader.readLine()) != null) {
        String[] dataArray = line.split(delimiter);
        if (headerArray.length != dataArray.length) {
          throw new IllegalArgumentException(
              DataLoaderError.CSV_DATA_MISMATCH.buildMessage(
                  line, String.join(delimiter, headerArray)));
        }
        JsonNode jsonNode = combineHeaderAndData(headerArray, dataArray);
        if (jsonNode.isEmpty()) continue;

        batch.add(new ImportRow(currentRowNumber++, jsonNode));
      }
    } catch (IOException e) {
      throw new RuntimeException(
          DataLoaderError.CSV_FILE_READ_FAILED.buildMessage(e.getMessage()), e);
    }
    return batch;
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
