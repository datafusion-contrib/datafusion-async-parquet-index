# DataFusion secondary index example using SQLx

This example demonstrates how to integrate a secondary index built using SQLite via SQLx with DataFusion.

SQLite is used as a stand-in for an external remote relational database, it should be easy to adapt this example to use another database.

This examples should be considered incomplete: it does not try to handle **many** edge cases or push down filters as much as possible.
It is meant to sketch out the basic idea, not be a complete implementation.

## Running the example

To run the example you just need to have Rust and Cargo installed. Then you can run just `cargo run`.

You'll see output like this:

```text
** Table Provider:
IndexTableProvider
---- Index ----
SQLiteIndex()


** Select data, no predicates:
+---------------+-------+
| file_name     | value |
+---------------+-------+
| file1.parquet | 0     |
| file1.parquet | 1     |
| file1.parquet | 2     |
| file1.parquet | 3     |
| file1.parquet | 4     |
| file1.parquet | 5     |
| file1.parquet | 6     |
| file1.parquet | 7     |
| file1.parquet | 8     |
| file1.parquet | 9     |
+---------------+-------+
Files scanned: [("file1.parquet", ParquetAccessPlan { row_groups: [Scan, Scan] }), ("file2.parquet", ParquetAccessPlan { row_groups: [Scan, Scan] }), ("file3.parquet", ParquetAccessPlan { row_groups: [Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan] })]

** Select data, predicate `value = 150`
+---------------+-------+
| file_name     | value |
+---------------+-------+
| file2.parquet | 150   |
+---------------+-------+
Files scanned: [("file2.parquet", ParquetAccessPlan { row_groups: [Skip, Scan] })]

** Select data, predicate `value < 20 OR value > 500`
+---------------+--------------------------+
| file_name     | COUNT(index_table.value) |
+---------------+--------------------------+
| file1.parquet | 20                       |
| file3.parquet | 2499                     |
+---------------+--------------------------+
Files scanned: [("file1.parquet", ParquetAccessPlan { row_groups: [Scan, Skip] }), ("file3.parquet", ParquetAccessPlan { row_groups: [Skip, Skip, Skip, Skip, Skip, Skip, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan, Scan] })]

** Select data, predicate `value < 20 AND text = 'a'`
+---------------+--------------------------+
| file_name     | COUNT(index_table.value) |
+---------------+--------------------------+
| file1.parquet | 1                        |
+---------------+--------------------------+
Files scanned: [("file1.parquet", ParquetAccessPlan { row_groups: [Scan, Skip] })]

** Select data, predicate `value > 500 AND text = 'a'`
+-----------+--------------------------+
| file_name | COUNT(index_table.value) |
+-----------+--------------------------+
+-----------+--------------------------+
Files scanned: []
```

As you can see the index is being used to select which row groups to read from the Parquet files.
