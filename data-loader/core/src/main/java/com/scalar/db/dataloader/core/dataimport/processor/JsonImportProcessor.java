package com.scalar.db.dataloader.core.dataimport.processor;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.scalar.db.dataloader.core.DataLoaderError;
import com.scalar.db.dataloader.core.DataLoaderObjectMapper;
import com.scalar.db.dataloader.core.dataimport.ImportRow;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A processor for importing JSON data into the database.
 *
 * <p>This processor handles JSON files that contain an array of JSON objects. Each object in the
 * array represents a row to be imported into the database. The processor reads the JSON file in
 * batches according to the transaction batch size, allowing for efficient streaming and constant
 * memory usage regardless of file size.
 */
public class JsonImportProcessor extends ImportProcessor {

  private static final DataLoaderObjectMapper OBJECT_MAPPER = new DataLoaderObjectMapper();
  private static final JsonFactory JSON_FACTORY = new JsonFactory();

  private JsonParser jsonParser;
  private int currentRowNumber = 1;
  private boolean initialized = false;
  private boolean endOfArray = false;

  /**
   * Creates a new JsonImportProcessor with the specified parameters.
   *
   * @param params Configuration parameters for the import processor
   */
  public JsonImportProcessor(ImportProcessorParams params) {
    super(params);
  }

  /**
   * Reads the next batch of records from the JSON array file.
   *
   * <p>This method reads up to batchSize objects from the JSON array. The method expects the JSON
   * file to start with an array token '[' and end with ']'. Empty or null JSON nodes are skipped
   * during processing.
   *
   * @param reader the BufferedReader containing the JSON data
   * @param batchSize the maximum number of records to read
   * @return a list of ImportRow objects, or an empty list if no more records
   * @throws RuntimeException if there is an error reading the JSON file
   */
  @Override
  protected List<ImportRow> readNextBatch(BufferedReader reader, int batchSize) {
    List<ImportRow> batch = new ArrayList<>();
    try {
      // Initialize parser on first call
      if (!initialized) {
        jsonParser = JSON_FACTORY.createParser(reader);
        if (jsonParser.nextToken() != JsonToken.START_ARRAY) {
          throw new IOException(DataLoaderError.JSON_CONTENT_START_ERROR.buildMessage());
        }
        initialized = true;
      }

      // If we've already reached the end, return empty
      if (endOfArray) {
        return batch;
      }

      while (batch.size() < batchSize) {
        JsonToken token = jsonParser.nextToken();
        if (token == JsonToken.END_ARRAY) {
          endOfArray = true;
          break;
        }
        JsonNode jsonNode = OBJECT_MAPPER.readTree(jsonParser);
        if (jsonNode == null || jsonNode.isEmpty()) continue;

        batch.add(new ImportRow(currentRowNumber++, jsonNode));
      }
    } catch (IOException e) {
      throw new RuntimeException(
          DataLoaderError.JSON_FILE_READ_FAILED.buildMessage(e.getMessage()), e);
    }
    return batch;
  }
}
