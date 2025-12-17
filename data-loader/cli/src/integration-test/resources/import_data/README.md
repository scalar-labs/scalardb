# Test Data Files Documentation

This directory contains test data files used by integration tests for the data-loader CLI.

## File Naming Convention

Test data files follow a consistent naming pattern:
- **Format**: `{purpose}_{scope}_{format}.{ext}`
- **Purpose**: What the file tests (e.g., `single`, `multi`, `all`, `with_null`)
- **Scope**: Data scope (e.g., `mapped`, `trn`, `full`)
- **Format**: File format (e.g., `csv`, `json`, `jsonl`)
- **Extension**: Matches format

## File Descriptions

### Single Record Files
Files containing a single record for basic import testing:

- **`import_single.csv`** - Single employee record in CSV format
- **`import_single.json`** - Single employee record in JSON format
- **`import_single.jsonl`** - Single employee record in JSON Lines format

### Multi-Record Files
Files containing multiple records for batch import testing:

- **`import_data_all.csv`** - Multiple employee records in CSV format
- **`import_multi_mapped.csv`** - Multiple records with column mapping in CSV
- **`import_multi_mapped.json`** - Multiple records with column mapping in JSON
- **`import_multi_mapped.jsonl`** - Multiple records with column mapping in JSON Lines

### Special Format Files
Files with special formatting or delimiters:

- **`import_single_delimiter.csv`** - CSV with custom delimiter
- **`import_single_without_header.csv`** - CSV without header row

### Transaction Files
Files for testing transaction-related features:

- **`import_single_trn_full.csv`** - Single record with transaction metadata in CSV
- **`import_single_trn_full.json`** - Single record with transaction metadata in JSON
- **`import_single_trn_full.jsonl`** - Single record with transaction metadata in JSON Lines

### Special Data Files
Files with special data scenarios:

- **`import_data_with_null.json`** - Records containing null values for null handling tests

### Mapped Files
Files with column mapping configurations:

- **`import_single_mapped.csv`** - Single record with column mapping in CSV
- **`import_single_mapped.json`** - Single record with column mapping in JSON
- **`import_single_mapped.jsonl`** - Single record with column mapping in JSON Lines

## Usage in Tests

### CSV Files
```java
Path filePath = Paths.get(
    Objects.requireNonNull(
        getClass().getClassLoader().getResource("import_data/import_single.csv"))
        .toURI());
```

### JSON Files
```java
Path filePath = Paths.get(
    Objects.requireNonNull(
        getClass().getClassLoader().getResource("import_data/import_single.json"))
        .toURI());
```

### JSON Lines Files
```java
Path filePath = Paths.get(
    Objects.requireNonNull(
        getClass().getClassLoader().getResource("import_data/import_single.jsonl"))
        .toURI());
```

## Data Schema

All test data files use the following schema for the `employee` table:

- **`id`** (INT, PARTITION KEY) - Employee ID
- **`name`** (TEXT) - Employee name
- **`email`** (TEXT) - Employee email

For transaction-enabled tables (`employee_trn`), additional fields include:
- **`tx_id`** (TEXT) - Transaction ID
- **`tx_state`** (INT) - Transaction state
- **`tx_version`** (INT) - Transaction version
- **`tx_prepared_at`** (BIGINT) - Transaction prepared timestamp
- **`tx_committed_at`** (BIGINT) - Transaction committed timestamp
- Plus `before_*` fields for transaction history

## Adding New Test Data Files

When adding new test data files:

1. **Follow naming convention**: `{purpose}_{scope}_{format}.{ext}`
2. **Update this README**: Add description of the new file
3. **Use consistent schema**: Match existing table schemas
4. **Document purpose**: Explain what scenario the file tests
5. **Keep files small**: Test data should be minimal but representative

## File Organization

Files are organized by:
- **Format** (CSV, JSON, JSONL)
- **Purpose** (single, multi, special cases)
- **Scope** (basic, mapped, transaction-enabled)

This organization makes it easy to find the right test data file for a specific test scenario.

