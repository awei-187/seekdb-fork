# Competition regression checks

`run_regression.py` runs the seven checked-in `ai_function`, `file_sql`, and
`ai_column` mysqltest cases against a local seekdb instance. It compares results
with the existing `.result` files and rejects unsupported mysqltest directives.
It requires Python 3 and PyMySQL; it does not require a live model service.

Start a disposable seekdb instance with `secure_file_priv` pointing to an empty
fixture directory. Run from the repository root (adjust the port and paths):

```powershell
python unittest/sql/competition/run_regression.py --port 2881 --fixture-root E:/seekdb-test/fixtures --report-dir E:/seekdb-test/results
```

Use `--case ai_column/ai_column_worker` to run a single case. Each invocation
owns its mock HTTP processes and a UUID fixture subdirectory. The adapter leaves
actual output, unified diffs, and mock logs in the report directory, and removes
its temporary input files and mock processes on exit. The SQL cases create and
drop their own databases and model definitions; use an isolated test instance.

The adapter normalizes SQL continuation indentation. All result values and
expected server errors are checked. The fixture data and expected result files
are in `tools/deploy/mysql_test/test_suite`. The AI vector EXPLAIN expectation
includes the visibility filter that excludes invalidated AI results from ANN
candidates in the current transaction.

`run_edge_cases.py` adds malformed-response checks, HTTP batch-size and row-order
assertions, reranking across several provider batches, ALTER ADD/MODIFY backfill,
and strict reads whose SQL predicates would otherwise discard all rows. It takes
the same connection and directory arguments as `run_regression.py`.

For persistence coverage, run `run_restart_case.py prepare` with the same
arguments, restart that isolated server externally, then run
`run_restart_case.py recover`. The preparation commits a business row together
with a leased task. Recovery verifies persistence, expires that lease, and
checks that a worker publishes the result and removes the task. The script
does not manage the server process.

The file readers also have independent C++ tests in `../engine/basic`.
Configure that directory as a standalone CMake project and run CTest. To include
Parquet reference files, run `generate_parquet_fixtures.py` with PyArrow installed
and set `FILE_SQL_FIXTURE_DIR` to its output directory. These tests cover CSV and
JSON parsing, path restrictions, file identity checks, rescan, atomic COPY
publication, projection, Snappy, Parquet page/dictionary variants, and decoded
memory limits. The C++ tests themselves do not depend on Python or PyArrow.
