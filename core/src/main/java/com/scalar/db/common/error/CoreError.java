package com.scalar.db.common.error;

public enum CoreError implements ScalarDbError {
  OPERATION_CHECK_ERROR_INDEX_ONLY_SINGLE_COLUMN_INDEX_SUPPORTED(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "000",
      "Only a single column index is supported. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_INDEX_NON_INDEXED_COLUMN_SPECIFIED(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "001",
      "The column of the specified index key is not indexed. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_INDEX_INDEX_KEY_NOT_PROPERLY_SPECIFIED(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "002",
      "The index key is not properly specified. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_INDEX_CLUSTERING_KEY_SPECIFIED(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "003",
      "Clustering keys cannot be specified when using an index. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_INDEX_ORDERING_SPECIFIED(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "004",
      "Orderings cannot be specified when using an index. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_LIMIT(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "005",
      "The limit cannot be negative. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_CROSS_PARTITION_SCAN(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "006",
      "Cross-partition scan is not enabled. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_CROSS_PARTITION_SCAN_ORDERING(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "007",
      "Cross-partition scan ordering is not enabled. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_CROSS_PARTITION_SCAN_FILTERING(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "008",
      "Cross-partition scan filtering is not enabled. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_PROJECTION(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "009",
      "The specified projection is not found. Invalid projection: %s, Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_CLUSTERING_KEY_BOUNDARY(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "010",
      "The clustering key boundary is not properly specified. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_START_CLUSTERING_KEY(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "011",
      "The start clustering key is not properly specified. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_END_CLUSTERING_KEY(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "012",
      "The end clustering key is not properly specified. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_ORDERING_NOT_PROPERLY_SPECIFIED(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "013",
      "Orderings are not properly specified. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_ORDERING_COLUMN_NOT_FOUND(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "014",
      "The specified ordering column is not found. Invalid ordering: %s, Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_CONDITION(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "015",
      "The condition is not properly specified. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_TABLE_NOT_FOUND(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "016",
      "The specified table is not found: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_INVALID_COLUMN(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "017",
      "The column value is not properly specified. Invalid column: %s, Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_MUTATIONS_EMPTY(
      Category.USER_ERROR, SubCategory.ILLEGAL_ARGUMENT, "018", "The mutations are empty", "", ""),
  OPERATION_CHECK_ERROR_MULTI_PARTITION_MUTATION(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "019",
      "Mutations that span multi-partition are not supported. Mutations: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_PARTITION_KEY(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "020",
      "The partition key is not properly specified. Operation: %s",
      "",
      ""),
  OPERATION_CHECK_ERROR_CLUSTERING_KEY(
      Category.USER_ERROR,
      SubCategory.ILLEGAL_ARGUMENT,
      "021",
      "The clustering key is not properly specified. Operation: %s",
      "",
      ""),
  ;

  private static final String COMPONENT_NAME = "CORE";

  private final Category category;
  private final SubCategory subCategory;
  private final String id;
  private final String message;
  private final String cause;
  private final String solution;

  CoreError(
      Category category,
      SubCategory subCategory,
      String id,
      String message,
      String cause,
      String solution) {
    validate(COMPONENT_NAME, category, subCategory, id, message, cause, solution);

    this.category = category;
    this.subCategory = subCategory;
    this.id = id;
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
  public String getId() {
    return id;
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
