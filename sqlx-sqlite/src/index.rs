use std::{collections::HashMap, fmt::Display, fs::File, path::Path, sync::Arc};

use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::{
    Int16Type, Int32Type, Int64Type, Int8Type, SchemaRef, UInt16Type, UInt32Type, UInt64Type,
    UInt8Type,
};
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::{
    datasource::physical_plan::parquet::{ParquetAccessPlan, RowGroupAccess, StatisticsConverter},
    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder,
};
use datafusion_common::{internal_datafusion_err, DataFusionError, Result};
use datafusion_expr::{col, lit};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_sql::unparser::expr_to_sql;
use sea_query::{
    Alias, Expr as SeaQExpr, OnConflict, Query, SimpleExpr,
    SqliteQueryBuilder,
};
use sea_query_binder::SqlxBinder;
use sqlx::SqlitePool;

/// SQLite secondary index for a set of parquet files
///
/// It stores file-level data (filename and file size) as well as statistics for each column
/// in each row group of each file.
///
/// When we scan a table we push down filters to the index to get a list of row groups that match
/// and hence the files that need to be read.
///
/// It is possible for the index to store finer grained statistics or a complete row oriented index
/// that filters down to individual rows within row groups.
/// For example, if you have a table with an `id` column and you want to enable fast point lookups
/// you could store the entire `id` column in the secondary index as a key/value map from `id` to
/// (file_name, row_group, row_number) and use that to enable fast point lookups on parquet files.
/// This is not implemented in this example.
///
/// The index is implemented as a SQLite database with two tables:
/// - `file_statistics` with columns `file_id`, `file_name`, `file_size_bytes`, `row_group_count`, `row_count`
/// - `column_statistics` with columns `file_id`, `column_name`, `row_group`, `null_count`, `row_count`,
///    and min/max values for each data type we support
///
/// Here is roughly what `SELECT * FROM file_statistics` would look like:
/// | file_id | file_name     | file_size_bytes | row_group_count | row_count |
/// | 1       | file1.parquet | 1234            | 3               | 1000      |
///
/// And `SELECT * FROM column_statistics`:
/// | file_id | column_name | row_group | null_count | row_count | int_min_value | int_max_value | string_min_value | string_max_value |
/// |---------|-------------|-----------|------------|-----------|---------------|---------------|------------------|------------------|
/// | 1       | column1     | 0         | 0          | 1000      | 1             | 100           |                  |                  |
/// | 1       | column1     | 1         | 0          | 1000      | 101           | 200           |                  |                  |
/// | 1       | column1     | 2         | 0          | 1000      | 201           | 300           |                  |                  |
/// | 1       | column2     | 0         | 0          | 1000      |               |               | a                | c                |
/// | 1       | column2     | 1         | 0          | 1000      |               |               | c                | x                |
/// | 1       | column2     | 2         | 0          | 1000      |               |               | x                | z                |
///
/// To do filtering on `column_statistics` we need to self-join the table on `file_id` and `row_group` for each column:
///
/// ```sql
/// WITH column1_stats AS (
///   SELECT file_id, row_group, int_min_value AS column1_min, int_max_value AS column1_max FROM column_statistics WHERE column_name = 'column1'
/// ), column2_stats AS (
///  SELECT file_id, row_group, string_min_value AS column2_min, string_max_value AS column2_max FROM column_statistics WHERE column_name = 'column2'
/// )
/// SELECT *
/// FROM column1_stats
/// JOIN column2_stats USING (file_id, row_group)
/// ```
///
/// Then to prune files we apply the filter to the joined table, let's call it `wide_column_statistics`:
///
/// ```sql
/// SELECT file_name, file_size_bytes, row_group_count, row_group
/// FROM wide_column_statistics
/// JOIN file_statistics USING (file_id)
/// WHERE column1_min <= 10 AND column1_max >= 10 AND column2_min <= 'b' AND column2_max >= 'b'
/// ```
///
/// While we use SQLite in this example, the index could be implemented with other databases or system.
/// SQLite is just a convenient example that is also very similar to other RDBMS systems that you might use.
#[derive(Debug)]
pub struct SQLiteIndex {
    pool: SqlitePool,
    /// The index for the schema. Not all columns in the table need to be indexed.
    schema: SchemaRef,
}

impl Display for SQLiteIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "SQLiteIndex()")?;
        Ok(())
    }
}

impl SQLiteIndex {
    pub fn new(pool: SqlitePool, schema: SchemaRef) -> Self {
        Self { pool, schema }
    }

    /// Return the filenames / row groups that match the filter
    ///
    /// This function pushes down the filter to the index to get a list of row groups that match
    /// and hence the files that need to be read.
    ///
    /// The filter is pushed down to the index by converting it to a set of SQL expressions
    /// that can be evaluated by the index.
    ///
    /// The return value is a list of `(file_name, FileScanPlan)` tuples where `FileScanPlan` contains
    /// the file metadata and the row groups that need to be scanned.
    pub async fn get_files(
        &self,
        filter: Arc<dyn PhysicalExpr>,
        schema: SchemaRef,
    ) -> Result<Vec<(String, FileScanPlan)>> {
        let pruning = PruningPredicate::try_new(filter, schema.clone())?;

        let statistics_predicate = pruning.predicate_expr();

        // TODO: we can either convert the PhysicalExpr to an Expr and use expr_to_sql
        // Or we convert it manually into SeaQuery expressions
        // The former is likely less code, the latter would generalize more (there is no guarantee
        // the index database supports DataFusion flavored SQL)
        let expr = col("value_min").gt(lit(1));
        let sql = format!(
            "r#
                WITH row_groups AS (
                    SELECT row_group
                    FROM row_group_statistics
                    WHERE {}
                )
                SELECT file_name, file_size_bytes, row_group_count, row_group
                FROM row_groups
                JOIN file_statistics USING (file_id)
            #",
            expr_to_sql(&expr)?
        );

        // TODO: we could aggregate the row groups into an array in the query to transmit less data over the wire
        // (and maybe avoid the join), leaving that as a TODO since it introduces more complexity and coupling to the index's backing store
        // Result is in the form of (file_name, file_size, row_group_count, row_group_to_scan)
        let row_groups: Vec<(String, i64, i64, i64)> = sqlx::query_as(&sql)
            .fetch_all(&self.pool)
            .await
            .unwrap(); // TODO: handle error, possibly failing gracefully by scanning all files?

        let mut file_scans: HashMap<String, (i64, ParquetAccessPlan)> = HashMap::new(); // file_name -> (file_size, row_groups)

        for (file_name, file_size, file_row_group_counts, row_group_to_scan) in row_groups {
            let (_, access_plan) = file_scans.entry(file_name).or_insert((
                file_size,
                ParquetAccessPlan::new_none(file_row_group_counts as usize),
            ));
            // Here we could do finer grained row-level filtering, but this example does not implement that
            access_plan.set(row_group_to_scan as usize, RowGroupAccess::Scan)
        }

        Ok(file_scans
            .into_iter()
            .map(|(file_name, (file_size, access_plan))| {
                (
                    file_name,
                    FileScanPlan {
                        file_size: file_size as u64,
                        access_plan,
                    },
                )
            })
            .collect())
    }

    /// Add a new file to the index
    pub async fn add_file(&mut self, file: &Path) -> anyhow::Result<()> {
        let file_name = file
            .file_name()
            .ok_or_else(|| internal_datafusion_err!("No filename"))?
            .to_str()
            .ok_or_else(|| internal_datafusion_err!("Invalid filename"))?;
        let file_size = file.metadata()?.len();

        let file = File::open(file).map_err(|e| {
            DataFusionError::from(e).context(format!("Error opening file {file:?}"))
        })?;

        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?;

        // extract the parquet statistics from the file's footer
        let metadata = reader.metadata();
        let schema = reader.schema();
        let parquet_schema = reader.parquet_schema();
        let row_groups = metadata.row_groups();
        let row_counts = StatisticsConverter::row_group_row_counts(row_groups.iter())?;
        let mut row_group_statistics: Vec<_> = row_counts
            .iter()
            .enumerate()
            .map(|(row_group, row_count)| {
                RowGroupStatisticsInsert::new(row_group as i64, row_count.unwrap() as i64)
            })
            .collect();

        for field in self.schema.fields() {
            let column_name = field.name().clone();
            let converter = StatisticsConverter::try_new(&column_name, schema, parquet_schema)?;
            let min_values = converter.row_group_mins(row_groups.iter())?;
            let max_values = converter.row_group_maxes(row_groups.iter())?;
            let null_counts = converter.row_group_null_counts(row_groups.iter())?;
            let null_counts = null_counts.as_primitive::<UInt64Type>();

            for row_group in 0..metadata.num_row_groups() {
                match field.data_type() {
                    datafusion::arrow::datatypes::DataType::Int8 => {
                        let min_values = min_values.as_primitive::<Int8Type>();
                        let max_values = max_values.as_primitive::<Int8Type>();
                        let min = min_values.value(row_group) as i64;
                        let max = max_values.value(row_group) as i64;
                        let column_statistics = ColumnStatistics {
                            null_count: null_counts.value(row_group) as i64,
                            stats: MinMaxStats::Int(min, max),
                        };
                        row_group_statistics[row_group]
                            .column_statistics
                            .push(column_statistics);
                    }
                    datafusion::arrow::datatypes::DataType::UInt8 => {
                        let min_values = min_values.as_primitive::<UInt8Type>();
                        let max_values = max_values.as_primitive::<UInt8Type>();
                        let min = min_values.value(row_group) as i64;
                        let max = max_values.value(row_group) as i64;
                        let column_statistics = ColumnStatistics {
                            null_count: null_counts.value(row_group) as i64,
                            stats: MinMaxStats::Int(min, max),
                        };
                        row_group_statistics[row_group]
                            .column_statistics
                            .push(column_statistics);
                    }
                    datafusion::arrow::datatypes::DataType::Int16 => {
                        let min_values = min_values.as_primitive::<Int16Type>();
                        let max_values = max_values.as_primitive::<Int16Type>();
                        let min = min_values.value(row_group) as i64;
                        let max = max_values.value(row_group) as i64;
                        let column_statistics = ColumnStatistics {
                            null_count: null_counts.value(row_group) as i64,
                            stats: MinMaxStats::Int(min, max),
                        };
                        row_group_statistics[row_group]
                            .column_statistics
                            .push(column_statistics);
                    }
                    datafusion::arrow::datatypes::DataType::UInt16 => {
                        let min_values = min_values.as_primitive::<UInt16Type>();
                        let max_values = max_values.as_primitive::<UInt16Type>();
                        let min = min_values.value(row_group) as i64;
                        let max = max_values.value(row_group) as i64;
                        let column_statistics = ColumnStatistics {
                            null_count: null_counts.value(row_group) as i64,
                            stats: MinMaxStats::Int(min, max),
                        };
                        row_group_statistics[row_group]
                            .column_statistics
                            .push(column_statistics);
                    }
                    datafusion::arrow::datatypes::DataType::Int32 => {
                        let min_values = min_values.as_primitive::<Int32Type>();
                        let max_values = max_values.as_primitive::<Int32Type>();
                        let min = min_values.value(row_group) as i64;
                        let max = max_values.value(row_group) as i64;
                        let column_statistics = ColumnStatistics {
                            null_count: null_counts.value(row_group) as i64,
                            stats: MinMaxStats::Int(min, max),
                        };
                        row_group_statistics[row_group]
                            .column_statistics
                            .push(column_statistics);
                    }
                    datafusion::arrow::datatypes::DataType::UInt32 => {
                        let min_values = min_values.as_primitive::<UInt32Type>();
                        let max_values = max_values.as_primitive::<UInt32Type>();
                        let min = min_values.value(row_group) as i64;
                        let max = max_values.value(row_group) as i64;
                        let column_statistics = ColumnStatistics {
                            null_count: null_counts.value(row_group) as i64,
                            stats: MinMaxStats::Int(min, max),
                        };
                        row_group_statistics[row_group]
                            .column_statistics
                            .push(column_statistics);
                    }
                    datafusion::arrow::datatypes::DataType::Int64 => {
                        let min_values = min_values.as_primitive::<Int64Type>();
                        let max_values = max_values.as_primitive::<Int64Type>();
                        let min = min_values.value(row_group);
                        let max = max_values.value(row_group);
                        let column_statistics = ColumnStatistics {
                            null_count: null_counts.value(row_group) as i64,
                            stats: MinMaxStats::Int(min, max),
                        };
                        row_group_statistics[row_group]
                            .column_statistics
                            .push(column_statistics);
                    }
                    datafusion::arrow::datatypes::DataType::Utf8 => {
                        let min_values = min_values.as_string::<i32>();
                        let max_values = max_values.as_string::<i32>();
                        let min = min_values.value(row_group).to_string();
                        let max = max_values.value(row_group).to_string();
                        let column_statistics = ColumnStatistics {
                            null_count: null_counts.value(row_group) as i64,
                            stats: MinMaxStats::String(min, max),
                        };
                        row_group_statistics[row_group]
                            .column_statistics
                            .push(column_statistics);
                    }
                    datafusion::arrow::datatypes::DataType::LargeUtf8 => {
                        let min_values = min_values.as_string::<i64>();
                        let max_values = max_values.as_string::<i64>();
                        let min = min_values.value(row_group).to_string();
                        let max = max_values.value(row_group).to_string();
                        let column_statistics = ColumnStatistics {
                            null_count: null_counts.value(row_group) as i64,
                            stats: MinMaxStats::String(min, max),
                        };
                        row_group_statistics[row_group]
                            .column_statistics
                            .push(column_statistics);
                    }
                    _ => {} // ignore other types, we just don't put them in the index and filters will not be pushed down
                }
            }
        }

        let file_statistics = FileStatisticsInsert {
            file_name: file_name.to_string(),
            file_size_bytes: file_size as i64,
            row_group_count: metadata.num_row_groups() as i64,
            row_count: metadata.file_metadata().num_rows(),
        };

        self.add_row(file_statistics, row_group_statistics).await?;
        Ok(())
    }

    async fn add_row(
        &self,
        file_statistics: FileStatisticsInsert,
        row_group_statistics: Vec<RowGroupStatisticsInsert>,
    ) -> anyhow::Result<()> {
        self.initialize().await?;

        let mut transaction = self.pool.begin().await?;

        let (sql, values) = Query::insert()
            .into_table(Alias::new("file_statistics"))
            .columns(vec![
                Alias::new("file_name"),
                Alias::new("file_size_bytes"),
                Alias::new("row_group_count"),
                Alias::new("row_count"),
            ])
            .values_panic(vec![
                file_statistics.file_name.into(),
                file_statistics.file_size_bytes.into(),
                file_statistics.row_group_count.into(),
                file_statistics.row_count.into(),
            ])
            .on_conflict(
                OnConflict::columns(vec![Alias::new("file_name")])
                    .update_columns(vec![
                        Alias::new("file_size_bytes"),
                        Alias::new("row_group_count"),
                        Alias::new("row_count"),
                    ])
                    .to_owned(),
            )
            .returning(Query::returning().column(Alias::new("file_id")))
            .build_sqlx(SqliteQueryBuilder);
        let (file_id,): (i64,) = sqlx::query_as_with(&sql, values)
            .fetch_one(&mut *transaction)
            .await?;

        // Delete any existing column statistics for this file
        let (sql, values) = Query::delete()
            .from_table(Alias::new("row_group_statistics"))
            .and_where(SeaQExpr::col(Alias::new("file_id")).eq(file_id))
            .build_sqlx(SqliteQueryBuilder);
        sqlx::query_with(&sql, values)
            .execute(&mut *transaction)
            .await?;

        let mut columns = vec![
            Alias::new("file_id"),
            Alias::new("row_group"),
            Alias::new("row_count"),
        ];

        for field in self.schema.fields() {
            columns.push(Alias::new(format!("{}_null_count", field.name())));
            columns.push(Alias::new(format!("{}_min", field.name())));
            columns.push(Alias::new(format!("{}_max", field.name())));
        }

        let mut query = Query::insert()
            .into_table(Alias::new("row_group_statistics"))
            .columns(columns)
            .to_owned();

        for statistics in row_group_statistics {
            let mut values: Vec<SimpleExpr> = vec![
                file_id.into(),
                statistics.row_group.into(),
                statistics.row_count.into(),
            ];
            for stats in statistics.column_statistics {
                match stats.stats {
                    MinMaxStats::Int(min, max) => {
                        values.push(stats.null_count.into());
                        values.push(min.into());
                        values.push(max.into());
                    }
                    MinMaxStats::String(min, max) => {
                        values.push(stats.null_count.into());
                        values.push(min.into());
                        values.push(max.into());
                    }
                }
            }

            query = query.values_panic(values).to_owned();
        }

        let (sql, values) = query.build_sqlx(SqliteQueryBuilder);

        sqlx::query_with(&sql, values)
            .execute(&mut *transaction)
            .await?;

        transaction.commit().await?;

        Ok(())
    }

    /// Simple migration function that idempotently creates the table for the index
    pub async fn initialize(&self) -> anyhow::Result<()> {
        let query = r#"
            CREATE TABLE IF NOT EXISTS file_statistics (
                file_id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_name TEXT NOT NULL UNIQUE,
                file_size_bytes INTEGER NOT NULL,
                row_group_count INTEGER NOT NULL,
                row_count INTEGER NOT NULL
            )
        "#;
        sqlx::query(&query).execute(&self.pool).await?;

        // The statistics columns are hardcoded in this example
        // It would be up to you to decide if this is appropriate for your use case
        // You could also store the statistics in a more flexible way, e.g. as a JSON blob or as an entity-attribute-value table
        let query = r#"
            CREATE TABLE IF NOT EXISTS column_statistics (
                file_id INTEGER NOT NULL,
                row_group INTEGER NOT NULL,
                row_count INTEGER NOT NULL,
                file_name_null_count INTEGER NOT NULL,
                file_name_min_value TEXT,
                file_name_max_value TEXT,
                value_null_count INTEGER NOT NULL,
                value_min_value INTEGER,
                value_max_value INTEGER,
                text_null_count INTEGER NOT NULL,
                text_min_value TEXT,
                text_max_value TEXT,
                PRIMARY KEY (file_id, column_name, row_group),
                FOREIGN KEY (file_id) REFERENCES file_statistics(file_id)
            )
        "#;
        sqlx::query(&query).execute(&self.pool).await?;

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct FileScanPlan {
    pub file_size: u64,
    pub access_plan: ParquetAccessPlan,
}

#[derive(Debug, Clone)]
pub enum MinMaxStats {
    Int(i64, i64),
    String(String, String),
}

#[derive(Debug, Clone)]
pub struct RowGroupStatisticsInsert {
    pub row_group: i64,
    pub row_count: i64,
    /// Per-column statistics
    pub column_statistics: Vec<ColumnStatistics>,
}

impl RowGroupStatisticsInsert {
    pub fn new(row_group: i64, row_count: i64) -> Self {
        Self {
            row_group,
            row_count,
            column_statistics: vec![],
        }
    }
}

#[derive(Debug, Clone)]
pub struct ColumnStatistics {
    null_count: i64,
    stats: MinMaxStats,
}

#[derive(Debug, Clone)]
struct FileStatisticsInsert {
    file_name: String,
    file_size_bytes: i64,
    row_group_count: i64,
    row_count: i64,
}