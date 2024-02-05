package com.scalar.db.common.error;

public enum CoreError implements ScalarDbError {
  OPERATION_CHECK_ERROR_INDEX1(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "001",
      "Only a single column index is supported. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_INDEX2(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "002",
      "The column of the specified index key is not indexed. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_INDEX3(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "003",
      "The index key is not properly specified. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_INDEX4(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "004",
      "Clustering keys cannot be specified when using an index. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_INDEX5(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "005",
      "Orderings cannot be specified when using an index. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_LIMIT(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "006",
      "The limit cannot be negative. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_CROSS_PARTITION_SCAN(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "007",
      "Cross-partition scan is not enabled. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_CROSS_PARTITION_SCAN_ORDERING(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "008",
      "Cross-partition scan ordering is not enabled. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_CROSS_PARTITION_SCAN_FILTERING(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "009",
      "Cross-partition scan filtering is not enabled. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_PROJECTION(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "010",
      "The specified projection is not found. Invalid projection: %s, Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_CLUSTERING_KEY_BOUNDARY(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "011",
      "The clustering key boundary is not properly specified. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_START_CLUSTERING_KEY(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "012",
      "The start clustering key is not properly specified. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_END_CLUSTERING_KEY(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "013",
      "The end clustering key is not properly specified. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_ORDERING1(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "014",
      "Orderings are not properly specified. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_ORDERING2(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "015",
      "The specified ordering column is not found. Invalid ordering: %s, Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_CONDITION(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "016",
      "The condition is not properly specified. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_TABLE_NOT_FOUND(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "017",
      "The specified table is not found: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_INVALID_COLUMN(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "018",
      "The column value is not properly specified. Invalid column: %s, Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_MUTATIONS_EMPTY(
      Category.USER_ERROR, SubCategory.ILLEGAL_ARGUMENT, "019", "The mutations are empty", "", ""),
  OPERATION_CHECK_ERROR_MULTI_PARTITION_MUTATION(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "020",
      "Mutations that span multi-partition are not supported. Mutations: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_PARTITION_KEY(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "021",
      "The partition key is not properly specified. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_CLUSTERING_KEY(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "022",
      "The clustering key is not properly specified. Operation: %s",
      "",
      ""),
  ;

  private static final String COMPONENT_NAME = "CORE";

  private final Category category;
  private final SubCategory subCategory;
  private final String code;
  private final String message;
  private final String cause;
  private final String solution;

  CoreError(
      Category category,
      SubCategory subCategory,
      String code,
      String message,
      String cause,
      String solution) {
    validate(COMPONENT_NAME, category, subCategory, code, message, cause, solution);

    this.category = category;
    this.subCategory = subCategory;
    this.code = code;
    this.message = message;
    this.cause = cause;
    this.solution = solution;
  }

  @Override
  public String getComponentName() {
    return COMPONENT_NAME;
  }

  @Override
  public Category getCategory() {
    return category;
  }

  @Override
  public SubCategory getSubCategory() {
    return subCategory;
  }

  @Override
  public String getCode() {
    return code;
  }

  @Override
  public String getMessage() {
    return message;
  }

  @Override
  public String getCause() {
    return cause;
  }

  @Override
  public String getSolution() {
    return solution;
  }
}
