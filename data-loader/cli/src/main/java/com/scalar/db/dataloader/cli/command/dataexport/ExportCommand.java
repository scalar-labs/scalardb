package com.scalar.db.dataloader.cli.command.dataexport;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.cli.exception.DirectoryValidationException;
import com.scalar.db.dataloader.cli.util.DirectoryUtils;
import com.scalar.db.dataloader.cli.util.FileUtils;
import com.scalar.db.dataloader.cli.util.InvalidFilePathException;
import com.scalar.db.dataloader.core.ColumnKeyValue;
import com.scalar.db.dataloader.core.FileFormat;
import com.scalar.db.dataloader.core.ScanRange;
import com.scalar.db.dataloader.core.dataexport.ExportManager;
import com.scalar.db.dataloader.core.dataexport.ExportOptions;
import com.scalar.db.dataloader.core.dataexport.producer.ProducerTaskFactory;
import com.scalar.db.dataloader.core.dataimport.dao.ScalarDBDao;
import com.scalar.db.dataloader.core.exception.Base64Exception;
import com.scalar.db.dataloader.core.exception.ColumnParsingException;
import com.scalar.db.dataloader.core.tablemetadata.TableMetadataException;
import com.scalar.db.dataloader.core.tablemetadata.TableMetadataService;
import com.scalar.db.dataloader.core.util.KeyUtils;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import java.io.BufferedWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@CommandLine.Command(name = "export", description = "export data from a ScalarDB table")
public class ExportCommand extends ExportCommandOptions implements Callable<Integer> {

  private static final String EXPORT_FILE_NAME_FORMAT = "export_%s.%s_%s.%s";
  private static final Logger LOGGER = LoggerFactory.getLogger(ExportCommand.class);

  @Spec CommandSpec spec;

  @Override
  public Integer call() throws Exception {
    String scalarDbPropertiesFilePath = getScalarDbPropertiesFilePath();

    try {
      validateOutputDirectory();
      FileUtils.validateFilePath(scalarDbPropertiesFilePath);

      StorageFactory storageFactory = StorageFactory.create(scalarDbPropertiesFilePath);
      TableMetadataService metaDataService =
          new TableMetadataService(storageFactory.getStorageAdmin());
      ScalarDBDao scalarDBDao = new ScalarDBDao();

      ExportManager exportManager = createExportManager(storageFactory, scalarDBDao);

      TableMetadata tableMetadata = metaDataService.getTableMetadata(namespace, table);

      Key partitionKey =
          partitionKeyValue != null ? getKey(partitionKeyValue, tableMetadata) : null;
      Key scanStartKey =
          scanStartKeyValue != null ? getKey(scanStartKeyValue, tableMetadata) : null;
      Key scanEndKey = scanEndKeyValue != null ? getKey(scanEndKeyValue, tableMetadata) : null;

      ScanRange scanRange =
          new ScanRange(scanStartKey, scanEndKey, scanStartInclusive, scanEndInclusive);
      ExportOptions exportOptions = buildExportOptions(partitionKey, scanRange);

      String filePath =
          getOutputAbsoluteFilePath(
              outputDirectory, outputFileName, exportOptions.getOutputFileFormat());
      LOGGER.info("Exporting data to file: {}", filePath);

      try (BufferedWriter writer =
          Files.newBufferedWriter(Paths.get(filePath), Charset.defaultCharset(), CREATE, APPEND)) {
        exportManager.startExport(exportOptions, tableMetadata, writer);
      }

    } catch (DirectoryValidationException e) {
      LOGGER.error("Invalid output directory path: {}", outputDirectory);
      return 1;
    } catch (InvalidFilePathException e) {
      LOGGER.error(
          "The ScalarDB connection settings file path is invalid or the file is missing: {}",
          scalarDbPropertiesFilePath);
      return 1;
    } catch (TableMetadataException e) {
      LOGGER.error("Failed to retrieve table metadata: {}", e.getMessage());
      return 1;
    }
    return 0;
  }

  private String getScalarDbPropertiesFilePath() {
    return Objects.equals(configFilePath, DEFAULT_CONFIG_FILE_NAME)
        ? Paths.get("").toAbsolutePath().resolve(DEFAULT_CONFIG_FILE_NAME).toString()
        : configFilePath;
  }

  private void validateOutputDirectory() throws DirectoryValidationException {
    if (StringUtils.isBlank(outputDirectory)) {
      DirectoryUtils.validateWorkingDirectory();
    } else {
      DirectoryUtils.validateTargetDirectory(outputDirectory);
    }
  }

  private ExportManager createExportManager(
      StorageFactory storageFactory, ScalarDBDao scalarDBDao) {
    return new ExportManager(
        storageFactory.getStorage(),
        scalarDBDao,
        new ProducerTaskFactory(delimiter, includeTransactionMetadata, prettyPrintJson));
  }

  private ExportOptions buildExportOptions(Key partitionKey, ScanRange scanRange) {
    ExportOptions.ExportOptionsBuilder builder =
        ExportOptions.builder(namespace, table, partitionKey, outputFormat)
            .sortOrders(sortOrders)
            .excludeHeaderRow(excludeHeader)
            .includeTransactionMetadata(includeTransactionMetadata)
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
                EXPORT_FILE_NAME_FORMAT,
                namespace,
                table,
                System.nanoTime(),
                outputFormat.toString().toLowerCase())
            : outputFileName;

    if (StringUtils.isBlank(outputDirectory)) {
      return Paths.get("").toAbsolutePath().resolve(fileName).toAbsolutePath().toString();
    } else {
      return Paths.get(outputDirectory).resolve(fileName).toAbsolutePath().toString();
    }
  }

  /**
   * *
   *
   * @param keyValueList key value list
   * @param tableMetadata table metadata
   * @return key
   * @throws Base64Exception if any error occur during decoding key
   */
  private Key getKey(List<ColumnKeyValue> keyValueList, TableMetadata tableMetadata)
      throws Base64Exception, ColumnParsingException {
    return KeyUtils.parseMultipleKeyValues(keyValueList, tableMetadata);
  }
}
