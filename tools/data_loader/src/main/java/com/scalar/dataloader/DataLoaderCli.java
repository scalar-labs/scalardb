package com.scalar.dataloader;

import static com.scalar.dataloader.common.Constants.ERROR_LOADING_SCHEMA;
import static com.scalar.dataloader.common.Constants.ERROR_LOADING_TABLE_SCHEMA;
import static com.scalar.dataloader.common.Constants.ERROR_READING_DATA_FILE;
import static com.scalar.dataloader.common.Constants.ERROR_READING_TABLE;
import static com.scalar.dataloader.common.Constants.ERROR_VALIDATING_TABLE;
import static com.scalar.dataloader.common.Constants.JSON_TYPE;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalar.dataloader.exception.DataLoaderException;
import com.scalar.dataloader.model.Rule;
import com.scalar.dataloader.model.TableMetadata;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Option;

@CommandLine.Command(
    name = "data_loader",
    description = "Load data into the specified Scalar DB table",
    mixinStandardHelpOptions = true,
    version = "1.0")
public class DataLoaderCli implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(DataLoaderCli.class);

  @Option(
      names = "--rule",
      defaultValue = "insert",
      description = "${COMPLETION-CANDIDATES}. Default value is ${DEFAULT-VALUE}")
  private static Rule rule;

  @Option(
      names = "--properties",
      required = true,
      description = "Path to the Scalar DB property file, ex: /path/to/scalardb.properties")
  private Path properties;

  @Option(
      names = "--data-file",
      required = true,
      description = "Path to the data file, ex: /path/to/data.json")
  private Path dataFile;

  @Option(
      names = "--table-schema",
      required = true,
      description = "Path to the table schema file, ex: /path/to/table-schema.json")
  private Path tableSchema;

  public static void main(String[] args) {
    int exitCode = new CommandLine(new DataLoaderCli()).execute(args);
    System.exit(exitCode);
  }

  @Override
  public void run() {
    try {
      TableMetadata table = readTable();
      JSONArray dataFileArray = readDataFile();
      String dataFileFormat =
          dataFile.toString().substring(dataFile.toString().lastIndexOf(".") + 1);
      if (!dataFileFormat.equals(JSON_TYPE)) {
        throw new DataLoaderException("The specified data file format is not of json type");
      }
      validateTable();

      ScalarDbRepository scalardb = new ScalarDbRepository(properties, table, dataFileArray);
      scalardb.loadDataIntoDb(rule);
    } catch (Exception e) {
      logger.error("An error occurred while loading data into database", e);
      throw e;
    }
    String ruleMessage = rule == Rule.insert ? "inserted" : rule.name() + "d";
    logger.info(String.format("Data has been successfully %s !", ruleMessage));
  }

  private JSONArray readDataFile() {
    try {
      String content = new String(Files.readAllBytes(dataFile));
      return new JSONArray(content);
    } catch (IOException e) {
      throw new DataLoaderException(ERROR_READING_DATA_FILE, e);
    }
  }

  private void validateTable() {
    try {
      String tableSchemaString = new String(Files.readAllBytes(tableSchema));
      JSONObject tableJsonNode = new JSONObject(tableSchemaString);
      getScalarDbTableSchemaValidator().validate(tableJsonNode);
    } catch (IOException e) {
      throw new DataLoaderException(ERROR_LOADING_TABLE_SCHEMA, e);
    } catch (ValidationException e) {
      String errorsMessage = String.join(", ", e.getAllMessages());
      throw new DataLoaderException(
          String.format("%s [%s]", ERROR_VALIDATING_TABLE, errorsMessage));
    }
  }

  private Schema getScalarDbTableSchemaValidator() {
    try {
      String scalarDbTableSchemaString = getResourceFileAsString("scalar_db_table_schema.json");
      return SchemaLoader.load(new JSONObject(scalarDbTableSchemaString));
    } catch (IOException e) {
      throw new DataLoaderException(ERROR_LOADING_SCHEMA, e);
    }
  }

  private TableMetadata readTable() {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);
    try {
      return objectMapper.readValue(Files.newInputStream(tableSchema), TableMetadata.class);
    } catch (IOException e) {
      throw new DataLoaderException(ERROR_READING_TABLE, e);
    }
  }

  /**
   * Reads given resource file as a string.
   *
   * @param fileName path to the resource file
   * @return the file's contents
   * @throws IOException if read fails for any reason
   */
  private String getResourceFileAsString(String fileName) throws IOException {
    ClassLoader classLoader = ClassLoader.getSystemClassLoader();
    try (InputStream is = classLoader.getResourceAsStream(fileName)) {
      if (is == null) return null;
      try (InputStreamReader isr = new InputStreamReader(is);
          BufferedReader reader = new BufferedReader(isr)) {
        return reader.lines().collect(Collectors.joining(System.lineSeparator()));
      }
    }
  }
}
