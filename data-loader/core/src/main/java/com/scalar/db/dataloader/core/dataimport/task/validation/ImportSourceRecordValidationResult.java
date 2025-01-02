package com.scalar.db.dataloader.core.dataimport.task.validation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.concurrent.Immutable;

/** The validation result for a data source record */
@Immutable
public final class ImportSourceRecordValidationResult {

  private final List<String> errorMessages;
  private final Set<String> columnsWithErrors;

  /** Constructor */
  public ImportSourceRecordValidationResult() {
    this.errorMessages = new ArrayList<>();
    this.columnsWithErrors = new HashSet<>();
  }

  /**
   * Add a validation error message for a column. Also marking the column as containing an error.
   *
   * @param columnName column name
   * @param errorMessage error message
   */
  public void addErrorMessage(String columnName, String errorMessage) {
    this.columnsWithErrors.add(columnName);
    this.errorMessages.add(errorMessage);
  }

  /** @return Immutable list of validation error messages */
  public List<String> getErrorMessages() {
    return Collections.unmodifiableList(this.errorMessages);
  }

  /** @return Immutable set of columns that had errors */
  public Set<String> getColumnsWithErrors() {
    return Collections.unmodifiableSet(this.columnsWithErrors);
  }

  /** @return Validation is valid or not */
  public boolean isValid() {
    return this.errorMessages.isEmpty();
  }
}
