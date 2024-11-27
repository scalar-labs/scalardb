package com.scalar.db.dataloader.cli.command.dataexport;

import com.scalar.db.common.error.CoreError;
import com.scalar.db.dataloader.cli.exception.DirectoryValidationException;
import com.scalar.db.dataloader.cli.exception.InvalidFileExtensionException;
import com.scalar.db.dataloader.cli.util.DirectoryUtils;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@CommandLine.Command(name = "export", description = "Export data from a ScalarDB table")
public class ExportCommand extends ExportCommandOptions implements Callable<Integer> {

  private static final List<String> ALLOWED_EXTENSIONS = Arrays.asList("csv", "json", "jsonl");

  @Spec CommandSpec spec;

  @Override
  public Integer call() throws Exception {
    validateOutputDirectory(outputFilePath);
    return 0;
  }

  private void validateOutputDirectory(@Nullable String path)
      throws DirectoryValidationException, InvalidFileExtensionException {
    if (path == null || path.isEmpty()) {
      // It is ok for the output file path to be null or empty as a default file name will be used
      // if not provided
      return;
    }

    File file = new File(path);

    if (file.isDirectory()) {
      validateDirectory(path);
    } else {
      validateFileExtension(file.getName());
      validateDirectory(file.getParent());
    }
  }

  private void validateDirectory(String directoryPath) throws DirectoryValidationException {
    // If the directory path is null or empty, use the current working directory
    if (directoryPath == null || directoryPath.isEmpty()) {
      DirectoryUtils.validateTargetDirectory(DirectoryUtils.getCurrentWorkingDirectory());
    } else {
      DirectoryUtils.validateTargetDirectory(directoryPath);
    }
  }

  private void validateFileExtension(String filename) throws InvalidFileExtensionException {
    String extension = FilenameUtils.getExtension(filename);
    if (StringUtils.isBlank(extension)) {
      throw new InvalidFileExtensionException(
          CoreError.DATA_LOADER_MISSING_FILE_EXTENSION.buildMessage(filename));
    }
    if (!ALLOWED_EXTENSIONS.contains(extension.toLowerCase())) {
      throw new InvalidFileExtensionException(
          CoreError.DATA_LOADER_INVALID_FILE_EXTENSION.buildMessage(
              extension, String.join(", ", ALLOWED_EXTENSIONS)));
    }
  }
}
