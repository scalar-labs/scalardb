package com.scalar.db.dataloader.core.dataimport.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.scalar.db.dataloader.core.DataLoaderError;
import com.scalar.db.dataloader.core.DataLoaderObjectMapper;
import com.scalar.db.dataloader.core.dataimport.ImportRow;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A processor for importing data from JSON Lines (JSONL) formatted files.
 *
 * <p>This processor reads data from files where each line is a valid JSON object. It processes the
 * input file in batches according to the transaction batch size, allowing for efficient streaming
 * and constant memory usage regardless of file size.
 */
public class JsonLinesImportProcessor extends ImportProcessor {

  private static final DataLoaderObjectMapper OBJECT_MAPPER = new DataLoaderObjectMapper();
  private int currentRowNumber = 1;

  /**
   * Creates a new JsonLinesImportProcessor with the specified parameters.
   *
   * @param params configuration parameters for the import processor
   */
  public JsonLinesImportProcessor(ImportProcessorParams params) {
    super(params);
  }

  /**
   * Reads the next batch of records from the JSONL file.
   *
   * <p>This method reads up to batchSize lines from the input file, parsing each line as a JSON
   * object. Empty lines or invalid JSON objects are skipped.
   *
   * @param reader the BufferedReader for reading the input file
   * @param batchSize the maximum number of records to read
   * @return a list of ImportRow objects, or an empty list if no more records
   * @throws RuntimeException if there is an error reading the file
   */
  @Override
  protected List<ImportRow> readNextBatch(BufferedReader reader, int batchSize) {
    List<ImportRow> batch = new ArrayList<>();
    try {
      String line;
      while (batch.size() < batchSize && (line = reader.readLine()) != null) {
        JsonNode jsonNode = OBJECT_MAPPER.readTree(line);
        if (jsonNode == null || jsonNode.isEmpty()) continue;

        batch.add(new ImportRow(currentRowNumber++, jsonNode));
      }
    } catch (IOException e) {
      throw new RuntimeException(
          DataLoaderError.JSONLINES_FILE_READ_FAILED.buildMessage(e.getMessage()), e);
    }
    return batch;
  }
}
