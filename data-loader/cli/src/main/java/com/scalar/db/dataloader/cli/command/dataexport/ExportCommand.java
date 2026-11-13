package com.scalar.db.dataloader.cli.command.dataexport;

import static com.scalar.db.dataloader.cli.util.CommandLineInputUtils.validateDeprecatedOptionPair;
import static com.scalar.db.dataloader.cli.util.CommandLineInputUtils.validatePositiveValue;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.cli.exception.DirectoryValidationException;
import com.scalar.db.dataloader.cli.util.DirectoryUtils;
import com.scalar.db.dataloader.cli.util.FileUtils;
import com.scalar.db.dataloader.cli.util.InvalidFilePathException;
import com.scalar.db.dataloader.core.ColumnKeyValue;
import com.scalar.db.dataloader.core.DataLoaderError;
import com.scalar.db.dataloader.core.FileFormat;
import com.scalar.db.dataloader.core.ScanRange;
import com.scalar.db.dataloader.core.dataexport.CsvExportManager;
import com.scalar.db.dataloader.core.dataexport.ExportManager;
import com.scalar.db.dataloader.core.dataexport.ExportOptions;
import com.scalar.db.dataloader.core.dataexport.ExportReport;
import com.scalar.db.dataloader.core.dataexport.JsonExportManager;
import com.scalar.db.dataloader.core.dataexport.JsonLineExportManager;
import com.scalar.db.dataloader.core.dataexport.producer.ProducerTaskFactory;
import com.scalar.db.dataloader.core.dataimport.dao.ScalarDbDao;
import com.scalar.db.dataloader.core.exception.ColumnParsingException;
import com.scalar.db.dataloader.core.exception.KeyParsingException;
import com.scalar.db.dataloader.core.tablemetadata.TableMetadataException;
import com.scalar.db.dataloader.core.tablemetadata.TableMetadataService;
import com.scalar.db.dataloader.core.util.KeyUtils;
import com.scalar.db.io.Key;
import com.scalar.db.service.TransactionFactory;
import java.io.BufferedWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import org.apache.commons.lang3.StringUtils;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@CommandLine.Command(name = "export", description = "export data from a ScalarDB table")
public class ExportCommand extends ExportCommandOptions implements Callable<Integer> {

  @Spec CommandSpec spec;

  @Override
  public Integer call() throws Exception {
    validateDeprecatedOptions();
    applyDeprecatedOptions();
    warnAboutIgnoredDeprecatedOptions();
    String scalarDbPropertiesFilePath = getScalarDbPropertiesFilePath();

    try {
      validateOutputDirectory();
      FileUtils.validateFilePath(scalarDbPropertiesFilePath);
      validatePositiveValue(
          spec.commandLine(), dataChunkSize, DataLoaderError.INVALID_DATA_CHUNK_SIZE);
      // Only validate the argument when provided by the user, if not set a default
      if (maxThreads != null) {
        validatePositiveValue(spec.commandLine(), maxThreads, DataLoaderError.INVALID_MAX_THREADS);
      } else {
        maxThreads = Runtime.getRuntime().availableProcessors();
      }

      TransactionFactory transactionFactory = TransactionFactory.create(scalarDbPropertiesFilePath);
      TableMetadata tableMetadata;
      try (DistributedTransactionAdmin admin = transactionFactory.getTransactionAdmin()) {
        TableMetadataService metaDataService = new TableMetadataService(admin);
        tableMetadata = metaDataService.getTableMetadata(namespace, table);
      }
      ScalarDbDao scalarDbDao = new ScalarDbDao();

      ExportManager exportManager =
          createExportManager(transactionFactory, scalarDbDao, outputFormat);

      Key partitionKey =
          partitionKeyValue != null ? getKeysFromList(partitionKeyValue, tableMetadata) : null;
      Key scanStartKey =
          scanStartKeyValue != null
              ? getKey(scanStartKeyValue, namespace, table, tableMetadata)
              : null;
      Key scanEndKey =
          scanEndKeyValue != null ? getKey(scanEndKeyValue, namespace, table, tableMetadata) : null;

      ScanRange scanRange =
          new ScanRange(scanStartKey, scanEndKey, scanStartInclusive, scanEndInclusive);
      ExportOptions exportOptions = buildExportOptions(partitionKey, scanRange);

      String filePath =
          getOutputAbsoluteFilePath(
              outputDirectory, outputFileName, exportOptions.getOutputFileFormat());

      try (BufferedWriter writer =
          Files.newBufferedWriter(Paths.get(filePath), Charset.defaultCharset(), CREATE, APPEND)) {
        ConsoleExportProgressReporter reporter = new ConsoleExportProgressReporter(filePath);
        ExportReport report = exportManager.startExport(exportOptions, tableMetadata, writer);
        reporter.reportCompletion(report.getExportedRowCount());
      }

    } catch (DirectoryValidationException e) {
      ConsoleExportProgressReporter.reportError("Invalid output directory: " + outputDirectory, e);
      return 1;
    } catch (InvalidFilePathException e) {
      ConsoleExportProgressReporter.reportError(
          "Invalid ScalarDB connection file path: " + scalarDbPropertiesFilePath, e);
      return 1;
    } catch (TableMetadataException e) {
      ConsoleExportProgressReporter.reportError("Failed to retrieve table metadata", e);
      return 1;
    }
    return 0;
  }

  /**
   * Validates that deprecated and new options are not both specified.
   *
   * @throws CommandLine.ParameterException if both old and new options are specified
   */
  private void validateDeprecatedOptions() {
    validateDeprecatedOptionPair(
        spec.commandLine(),
        DEPRECATED_START_EXCLUSIVE_OPTION,
        START_INCLUSIVE_OPTION,
        START_INCLUSIVE_OPTION_SHORT);
    validateDeprecatedOptionPair(
        spec.commandLine(),
        DEPRECATED_END_EXCLUSIVE_OPTION,
        END_INCLUSIVE_OPTION,
        END_INCLUSIVE_OPTION_SHORT);
    validateDeprecatedOptionPair(
        spec.commandLine(),
        DEPRECATED_THREADS_OPTION,
        MAX_THREADS_OPTION,
        MAX_THREADS_OPTION_SHORT);
  }

  /** Warns about deprecated options that are no longer used and have been completely ignored. */
  private void warnAboutIgnoredDeprecatedOptions() {
    CommandLine.ParseResult parseResult = spec.commandLine().getParseResult();
    boolean hasIncludeMetadata =
        parseResult.hasMatchedOption(DEPRECATED_INCLUDE_METADATA_OPTION)
            || parseResult.hasMatchedOption(DEPRECATED_INCLUDE_METADATA_OPTION_SHORT);

    if (hasIncludeMetadata) {
      // Use picocli's ANSI support for colored warning output
      CommandLine.Help.Ansi ansi = CommandLine.Help.Ansi.AUTO;
      String warning =
          ansi.string(
              "@|bold,yellow The "
                  + DEPRECATED_INCLUDE_METADATA_OPTION
                  + " option is deprecated and no longer has any effect. "
                  + "Use the 'scalar.db.consensus_commit.include_metadata.enabled' configuration property "
                  + "in your ScalarDB properties file to control whether transaction metadata is included in scan operations.|@");

      //      logger.warn(warning);
      ConsoleExportProgressReporter.reportWarning(warning);
    }
  }

  private String getScalarDbPropertiesFilePath() {
    if (StringUtils.isBlank(configFilePath)) {
      throw new IllegalArgumentException(DataLoaderError.CONFIG_FILE_PATH_BLANK.buildMessage());
    }
    return Objects.equals(configFilePath, DEFAULT_CONFIG_FILE_NAME)
        ? Paths.get("").toAbsolutePath().resolve(DEFAULT_CONFIG_FILE_NAME).toString()
        : configFilePath;
  }

  private void validateOutputDirectory() throws DirectoryValidationException {
    if (StringUtils.isBlank(outputDirectory)) {
      DirectoryUtils.validateWorkingDirectory();
    } else {
      DirectoryUtils.validateOrCreateTargetDirectory(outputDirectory);
    }
  }

  private ExportManager createExportManager(
      TransactionFactory transactionFactory, ScalarDbDao scalarDbDao, FileFormat fileFormat) {
    ProducerTaskFactory taskFactory = new ProducerTaskFactory(delimiter, prettyPrintJson);
    DistributedTransactionManager manager = transactionFactory.getTransactionManager();
    switch (fileFormat) {
      case JSON:
        return new JsonExportManager(manager, scalarDbDao, taskFactory);
      case JSONL:
        return new JsonLineExportManager(manager, scalarDbDao, taskFactory);
      case CSV:
        return new CsvExportManager(manager, scalarDbDao, taskFactory);
      default:
        throw new AssertionError("Invalid file format" + fileFormat);
    }
  }

  private ExportOptions buildExportOptions(Key partitionKey, ScanRange scanRange) {
    ExportOptions.ExportOptionsBuilder builder =
        ExportOptions.builder(namespace, table, partitionKey, outputFormat)
            .sortOrders(sortOrders)
            .excludeHeaderRow(excludeHeader)
            .delimiter(delimiter)
            .limit(limit)
            .maxThreadCount(maxThreads)
            .dataChunkSize(dataChunkSize)
            .prettyPrintJson(prettyPrintJson)
            .scanRange(scanRange);

    if (projectionColumns != null) {
      builder.projectionColumns(projectionColumns);
    }

    return builder.build();
  }

  private String getOutputAbsoluteFilePath(
      String outputDirectory, String outputFileName, FileFormat outputFormat) {
    String fileName =
        StringUtils.isBlank(outputFileName)
            ? String.format(
                "export.%s.%s.%s.%s",
                namespace, table, System.nanoTime(), outputFormat.toString().toLowerCase())
            : outputFileName;

    if (StringUtils.isBlank(outputDirectory)) {
      return Paths.get("").toAbsolutePath().resolve(fileName).toAbsolutePath().toString();
    } else {
      return Paths.get(outputDirectory).resolve(fileName).toAbsolutePath().toString();
    }
  }

  /**
   * Convert ColumnKeyValue list to a key
   *
   * @param keyValueList key value list
   * @param tableMetadata table metadata
   * @return key
   * @throws ColumnParsingException if any error occur during parsing column value
   */
  private Key getKeysFromList(List<ColumnKeyValue> keyValueList, TableMetadata tableMetadata)
      throws ColumnParsingException {
    return KeyUtils.parseMultipleKeyValues(keyValueList, tableMetadata);
  }

  /**
   * Convert ColumnKeyValue to a key
   *
   * @param keyValue key value
   * @param tableMetadata table metadata
   * @return key
   * @throws KeyParsingException if any error occur during decoding key
   */
  private Key getKey(
      ColumnKeyValue keyValue, String namespace, String table, TableMetadata tableMetadata)
      throws KeyParsingException {
    return KeyUtils.parseKeyValue(keyValue, namespace, table, tableMetadata);
  }
}
