# Performance test execution for ScalarDB Data Loader

## Instructions to run the script

Execute the e2e_test.sh script from the `performance-test` folder of the repository:

```
./e2e_test.sh [options]
```

## Available command-line arguments

```
./e2e_test.sh [--memory=mem1,mem2,...] [--cpu=cpu1,cpu2,...] [--data-size=size] [--image-tag=tag] [--import-args=args] [--export-args=args] [--network=network-name] [--skip-data-gen] [--disable-import] [--disable-export] [--no-clean-data] [--database-dir=path] [--use-jar] [--jar-path=path]
```

Options:

- `--memory=mem1,mem2,...`: Comma-separated list of memory limits for Docker containers (e.g., 1g,2g,4g)
- `--cpu=cpu1,cpu2,...`: Comma-separated list of CPU limits for Docker containers (e.g., 1,2,4)
- `--data-size=size`: Size of data to generate (e.g., 1MB, 2GB)
- `--num-rows=number`: Number of rows to generate (e.g., 1000, 10000)

Note: Either `--data-size` or `--num-rows` must be provided, but not both.

- `--image-tag=tag`: Docker image tag to use (default: 4.0.0-SNAPSHOT)
- `--import-args=args`: Arguments for import command
- `--export-args=args`: Arguments for export command
- `--network=network-name`: Docker network name (default: my-network)
- `--skip-data-gen`: Skip data generation step
- `--disable-import`: Skip import test
- `--disable-export`: Skip export test
- `--no-clean-data`: Don't clean up generated files after test
- `--database-dir=path`: Path to database directory
- `--use-jar`: Use JAR file instead of Docker container
- `--jar-path=path`: Path to JAR file (when using --use-jar)

Examples:

```
# Using data size
./e2e_test.sh --memory=1g,2g,4g --cpu=1,2,4 --data-size=2MB --image-tag=4.0.0-SNAPSHOT
```

```
# Using number of rows
./e2e_test.sh --memory=1g,2g,4g --cpu=1,2,4 --num-rows=10000 --image-tag=4.0.0-SNAPSHOT
```

Example with JAR:

```
./e2e_test.sh --use-jar --jar-path=./scalardb-data-loader-cli.jar --import-args="--format csv --import-mode insert --mode transaction --transaction-size 10 --data-chunk-size 500 --max-threads 16" --export-args="--format csv --max-threads 8 --data-chunk-size 500"
```

### Import-Only Examples

To run only the import test (skipping export):

```
# Using data size
./e2e_test.sh --disable-export --memory=2g --cpu=2 --data-size=1MB --import-args="--format csv --import-mode insert --mode transaction --transaction-size 10 --max-threads 16"
```

```
# Using number of rows
./e2e_test.sh --disable-export --memory=2g --cpu=2 --num-rows=10000 --import-args="--format csv --import-mode insert --mode transaction --transaction-size 10 --max-threads 16"
```

With JAR:

```
./e2e_test.sh --disable-export --use-jar --jar-path=./scalardb-data-loader-cli.jar --import-args="--format csv --import-mode insert --mode transaction --transaction-size 10 --max-threads 16"
```

### Export-Only Examples

To run only the export test (skipping import):

```
./e2e_test.sh --disable-import --memory=2g --cpu=2 --export-args="--format csv --max-threads 8 --data-chunk-size 500"
```

With JAR:

```
./e2e_test.sh --disable-import --use-jar --jar-path=./scalardb-data-loader-cli.jar --export-args="--format csv --max-threads 8 --data-chunk-size 500"
```
