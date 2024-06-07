use std::{
    collections::HashMap, fmt::Display, fs::File, path::Path
};

use arrow::{
    array::AsArray,
    datatypes::{Int16Type, Int32Type, Int64Type, UInt16Type, UInt32Type, UInt64Type},
};
use datafusion::{
    datasource::physical_plan::parquet::{ParquetAccessPlan, RequestedStatistics, RowGroupAccess, StatisticsConverter},
    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder,
    prelude::*,
};
use datafusion_common::{internal_datafusion_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::Operator;
use sqlx::SqlitePool;
use sea_query::{Expr as SeaQExpr, Iden, OnConflict, Query, SimpleExpr, SqliteQueryBuilder, Value as SqlValue};
use sea_query_binder::SqlxBinder;

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
#[derive(Debug)]
pub struct SQLiteIndex {
    pool: SqlitePool,
}

impl Display for SQLiteIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "SQLiteIndex()")?;
        Ok(())
    }
}

impl SQLiteIndex {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Return all the files matching the predicate
    ///
    /// Returns a tuple `(file_name, file_size)`
    pub async fn get_files(&self, filter: Option<Expr>) -> Result<Vec<(String, FileScanPlan)>> {
        let (sql, values) = Query::select()
            .columns(vec![
                FileStatistics::FileName,
                FileStatistics::FileSizeBytes,
                FileStatistics::RowGroupCount,
            ])
            .column(ColumnStatistics::RowGroup)
            .distinct() // could be distinct_on(vec![ColumnStatistics::FileId, ColumnStatistics::RowGroup]) if the backing store supports it
            .from(FileStatistics::Table)
            .inner_join(
                ColumnStatistics::Table,
                SeaQExpr::col((FileStatistics::Table, FileStatistics::FileId)).equals((ColumnStatistics::Table, ColumnStatistics::FileId)),
            )
            .and_where_option(filter.map(|f| push_down_filter(&f)).flatten())
            .build_sqlx(SqliteQueryBuilder);

        // TODO: we could aggregate the row groups into an array in the query to transmit less data over the wire
        // (and maybe avoid the join), leaving that as a TODO since it introduces more complexity and coupling to the index's backing store
        // Result is in the form of (file_name, file_size, row_group_count, row_group_to_scan)
        let row_groups: Vec<(String, i64, i64, i64)> = sqlx::query_as_with(&sql, values)
            .fetch_all(&self.pool)
            .await.unwrap(); // TODO: handle error, possibly failing gracefully by scanning all files?
        
        let mut file_scans: HashMap<String, (i64, ParquetAccessPlan)> = HashMap::new(); // file_name -> (file_size, row_groups)

        for (file_name, file_size, file_row_group_counts, row_group_to_scan) in row_groups {
            let (_, access_plan) = file_scans.entry(file_name).or_insert((file_size, ParquetAccessPlan::new_none(file_row_group_counts as usize)));
            // Here we could do finer grained row-level filtering, but this example does not implement that
            access_plan.set(row_group_to_scan as usize, RowGroupAccess::Scan)
        }
        
        Ok(
            file_scans.into_iter().map(|(file_name, (file_size, access_plan))| {
                (
                    file_name,
                    FileScanPlan {
                        file_size: file_size as u64,
                        access_plan,
                    }
                )
            }).collect()
        )
            
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

        let mut column_statistics: Vec<ColumnStatisticsInsert> = Vec::with_capacity(reader.schema().fields().len() * metadata.num_row_groups());

        for column in 0..reader.schema().fields().len() {
            let column_name = reader.schema().field(column).name().clone();
    
            let row_counts = StatisticsConverter::row_counts(reader.metadata())?;
            let null_counts_array = StatisticsConverter::try_new(&column_name, RequestedStatistics::NullCount, reader.schema())?.extract(reader.metadata())?;
            let null_counts = null_counts_array.as_primitive::<UInt64Type>();

            let min_values =
                StatisticsConverter::try_new(&column_name.clone(), RequestedStatistics::Min, reader.schema())?
                    .extract(reader.metadata())?;
            let max_values =
                StatisticsConverter::try_new(&column_name.clone(), RequestedStatistics::Max, reader.schema())?
                    .extract(reader.metadata())?;

            for row_group in 0..reader.metadata().num_row_groups() {
                let stats = ColumnStatisticsInsertBuilder::new(
                    column_name.clone(),
                    row_group as i64,
                    null_counts.value(row_group) as i64,
                    row_counts.value(row_group) as i64,
                );
                // match on the data type of the column, downcast the array and extract the min/max values and build the statistics
                match reader.schema().field(column).data_type() {
                    arrow::datatypes::DataType::Int8 => {
                        let min_values = min_values.as_primitive::<Int32Type>();
                        let max_values = max_values.as_primitive::<Int32Type>();
                        let min = min_values.value(row_group) as i64;
                        let max = max_values.value(row_group) as i64;
                        let stats = stats.build(MinMaxStats::Int(min, max));
                        column_statistics.push(stats);
                    }
                    arrow::datatypes::DataType::UInt8 => {
                        let min_values = min_values.as_primitive::<UInt16Type>();
                        let max_values = max_values.as_primitive::<UInt16Type>();
                        let min = min_values.value(row_group) as i64;
                        let max = max_values.value(row_group) as i64;
                        let stats = stats.build(MinMaxStats::Int(min, max));
                        column_statistics.push(stats);
                    }
                    arrow::datatypes::DataType::Int16 => {
                        let min_values = min_values.as_primitive::<Int16Type>();
                        let max_values = max_values.as_primitive::<Int16Type>();
                        let min = min_values.value(row_group) as i64;
                        let max = max_values.value(row_group) as i64;
                        let stats = stats.build(MinMaxStats::Int(min, max));
                        column_statistics.push(stats);
                    }
                    arrow::datatypes::DataType::UInt16 => {
                        let min_values = min_values.as_primitive::<UInt16Type>();
                        let max_values = max_values.as_primitive::<UInt16Type>();
                        let min = min_values.value(row_group) as i64;
                        let max = max_values.value(row_group) as i64;
                        let stats = stats.build(MinMaxStats::Int(min, max));
                        column_statistics.push(stats);
                    }
                    arrow::datatypes::DataType::Int32 => {
                        let min_values = min_values.as_primitive::<Int32Type>();
                        let max_values = max_values.as_primitive::<Int32Type>();
                        let min = min_values.value(row_group) as i64;
                        let max = max_values.value(row_group) as i64;
                        let stats = stats.build(MinMaxStats::Int(min, max));
                        column_statistics.push(stats);
                    }
                    arrow::datatypes::DataType::UInt32 => {
                        let min_values = min_values.as_primitive::<UInt32Type>();
                        let max_values = max_values.as_primitive::<UInt32Type>();
                        let min = min_values.value(row_group) as i64;
                        let max = max_values.value(row_group) as i64;
                        let stats = stats.build(MinMaxStats::Int(min, max));
                        column_statistics.push(stats);
                    }
                    arrow::datatypes::DataType::Int64 => {
                        let min_values = min_values.as_primitive::<Int64Type>();
                        let max_values = max_values.as_primitive::<Int64Type>();
                        let min = min_values.value(row_group) as i64;
                        let max = max_values.value(row_group) as i64;
                        let stats = stats.build(MinMaxStats::Int(min, max));
                        column_statistics.push(stats);
                    }
                    arrow::datatypes::DataType::Utf8 => {
                        let min_values = min_values.as_string::<i32>();
                        let max_values = max_values.as_string::<i32>();
                        let min = min_values.value(row_group).to_string();
                        let max = max_values.value(row_group).to_string();
                        let stats = stats.build(MinMaxStats::String(min, max));
                        column_statistics.push(stats);
                    }
                    arrow::datatypes::DataType::LargeUtf8 => {
                        let min_values = min_values.as_string::<i64>();
                        let max_values = max_values.as_string::<i64>();
                        let min = min_values.value(row_group).to_string();
                        let max = max_values.value(row_group).to_string();
                        let stats = stats.build(MinMaxStats::String(min, max));
                        column_statistics.push(stats);
                    }
                    _ => {} // ignore other types, we just don't put them in the index and filters will not be pushed down
                }
            }
        }

        let file_statistics = FileStatisticsInsert {
            file_name: file_name.to_string(),
            file_size_bytes: file_size as i64,
            row_group_count: metadata.num_row_groups() as i64,
            row_count: metadata.file_metadata().num_rows() as i64,
        };

        self.add_row(file_statistics, column_statistics).await?;
        Ok(())
    }

    async fn add_row(
        &self,
        file_statistics: FileStatisticsInsert,
        column_statistics: Vec<ColumnStatisticsInsert>,
    ) -> anyhow::Result<()> {
        self.initialize().await?;

        let mut transaction = self.pool.begin().await?;

        let (sql, values) = Query::insert()
        .into_table(FileStatistics::Table)
        .columns(vec![
            FileStatistics::FileName,
            FileStatistics::FileSizeBytes,
            FileStatistics::RowGroupCount,
            FileStatistics::RowCount,
        ])
        .values_panic(vec![
            file_statistics.file_name.into(),
            file_statistics.file_size_bytes.into(),
            file_statistics.row_group_count.into(),
            file_statistics.row_count.into(),
        ])
        .on_conflict(
            OnConflict::columns(vec![FileStatistics::FileName]).update_columns(
                vec![
                    FileStatistics::FileSizeBytes,
                    FileStatistics::RowGroupCount,
                    FileStatistics::RowCount,
                ]
            ).to_owned()
        )
        .returning(Query::returning().column(FileStatistics::FileId))
        .build_sqlx(SqliteQueryBuilder);
        let (file_id, ): (i64, ) = sqlx::query_as_with(&sql, values).fetch_one(&mut *transaction).await?;

        // Delete any existing column statistics for this file
        let (sql, values) = Query::delete()
            .from_table(ColumnStatistics::Table)
            .and_where(SeaQExpr::col(ColumnStatistics::FileId).eq(file_id))
            .build_sqlx(SqliteQueryBuilder);
        sqlx::query_with(&sql, values).execute(&mut *transaction).await?;

        for row_group_statistics in column_statistics {
            let (sql, values) = Query::insert()
                .into_table(ColumnStatistics::Table)
                .columns(vec![
                    ColumnStatistics::FileId,
                    ColumnStatistics::ColumnName,
                    ColumnStatistics::RowGroup,
                    ColumnStatistics::NullCount,
                    ColumnStatistics::RowCount,
                    ColumnStatistics::IntMinValue,
                    ColumnStatistics::IntMaxValue,
                    ColumnStatistics::StringMinValue,
                    ColumnStatistics::StringMaxValue,
                ])
                .values_panic({
                    match row_group_statistics.stats {
                        MinMaxStats::Int(min, max) => vec![
                            file_id.into(),
                            row_group_statistics.column_name.into(),
                            row_group_statistics.row_group.into(),
                            row_group_statistics.null_count.into(),
                            row_group_statistics.row_count.into(),
                            min.into(),
                            max.into(),
                            SqlValue::String(None).into(),
                            SqlValue::String(None).into(),
                        ],
                        MinMaxStats::String(min, max) => vec![
                            file_id.into(),
                            row_group_statistics.column_name.into(),
                            row_group_statistics.row_group.into(),
                            row_group_statistics.null_count.into(),
                            row_group_statistics.row_count.into(),
                            SqlValue::Int(None).into(),
                            SqlValue::Int(None).into(),
                            min.into(),
                            max.into(),
                        ],
                }})
                .build_sqlx(SqliteQueryBuilder);
            sqlx::query_with(&sql, values).execute(&mut *transaction).await?;
        }

        transaction.commit().await?;

        Ok(())
    }

    /// Simple migration function that idempotently creates the table for the index
    pub async fn initialize(&self) -> anyhow::Result<()> {
        let query = sea_query::Table::create()
            .table(FileStatistics::Table)
            .if_not_exists()
            .col(sea_query::ColumnDef::new(FileStatistics::FileId).big_integer().auto_increment().primary_key())
            .col(sea_query::ColumnDef::new(FileStatistics::FileName).string().not_null().unique_key())
            .col(sea_query::ColumnDef::new(FileStatistics::FileSizeBytes).big_integer().not_null())
            .col(sea_query::ColumnDef::new(FileStatistics::RowGroupCount).big_integer().not_null())
            .col(sea_query::ColumnDef::new(FileStatistics::RowCount).big_integer().not_null())
            .build(SqliteQueryBuilder);

        sqlx::query(&query).execute(&self.pool).await?;

        let query = sea_query::Table::create()
            .table(ColumnStatistics::Table)
            .if_not_exists()
            .col(sea_query::ColumnDef::new(ColumnStatistics::FileId).big_integer().not_null())
            .col(sea_query::ColumnDef::new(ColumnStatistics::ColumnName).string().not_null())
            .col(sea_query::ColumnDef::new(ColumnStatistics::RowGroup).big_integer().not_null())
            .col(sea_query::ColumnDef::new(ColumnStatistics::NullCount).big_integer())
            .col(sea_query::ColumnDef::new(ColumnStatistics::RowCount).big_integer())
            .col(sea_query::ColumnDef::new(ColumnStatistics::IntMinValue).big_integer())
            .col(sea_query::ColumnDef::new(ColumnStatistics::IntMaxValue).big_integer())
            .col(sea_query::ColumnDef::new(ColumnStatistics::StringMinValue).string())
            .col(sea_query::ColumnDef::new(ColumnStatistics::StringMaxValue).string())
            .build(SqliteQueryBuilder);

        sqlx::query(&query).execute(&self.pool).await?;

        Ok(())
    }
}


#[derive(Debug, Clone)]
pub struct FileScanPlan {
    pub file_size: u64,
    pub access_plan: ParquetAccessPlan,
}

#[derive(Debug, Clone, Iden)]
enum FileStatistics {
    Table,
    FileId,
    FileName,
    FileSizeBytes,
    RowGroupCount,
    RowCount,
}

#[derive(Debug, Clone, Iden)]
enum ColumnStatistics {
    Table,
    FileId,
    ColumnName,
    RowGroup,
    NullCount,
    RowCount,
    IntMinValue,
    IntMaxValue,
    StringMinValue,
    StringMaxValue,
    // Extend with other types as needed
}

#[derive(Debug, Clone)]
pub enum MinMaxStats {
    Int(i64, i64),
    String(String, String),
}

#[derive(Debug, Clone)]
pub struct ColumnStatisticsInsert {
    pub column_name: String,
    pub row_group: i64,
    pub null_count: i64,
    pub row_count: i64,
    stats: MinMaxStats,
}


#[derive(Debug, Clone)]
pub struct ColumnStatisticsInsertBuilder {
    column_name: String,
    row_group: i64,
    null_count: i64,
    row_count: i64,
}

impl ColumnStatisticsInsertBuilder {
    pub fn new(column_name: String, row_group: i64, null_count: i64, row_count: i64) -> Self {
        Self {
            column_name,
            row_group,
            null_count,
            row_count,
        }
    }

    pub fn build(self, stats: MinMaxStats) -> ColumnStatisticsInsert {
        ColumnStatisticsInsert {
            column_name: self.column_name,
            row_group: self.row_group,
            null_count: self.null_count,
            row_count: self.row_count,
            stats,
        }
    }
}

#[derive(Debug, Clone)]
struct FileStatisticsInsert {
    file_name: String,
    file_size_bytes: i64,
    row_group_count: i64,
    row_count: i64,
}


pub fn push_down_filter(filter: &Expr) -> Option<SimpleExpr> {
    match filter {
        Expr::BinaryExpr(binary_expr) => {
            match (*binary_expr.left.clone(), *binary_expr.right.clone()) {
                (Expr::Column(column), Expr::Literal(value)) => {
                    // This is something we can push down!
                    let column_name = column.name;
                    let filter = push_down_binary_filter(&value, &binary_expr.op);
                    if let Some(filter) = filter {
                        Some(SeaQExpr::col(ColumnStatistics::ColumnName).eq(column_name).and(filter))
                    } else {
                        None
                    }
                }
                (left, right) => {
                    let left_pushdown = push_down_filter(&left);
                    let right_pushdown = push_down_filter(&right);
                    match (left_pushdown, right_pushdown) {
                        (Some(left_pushdown), Some(right_pushdown)) => {
                            match binary_expr.op {
                                Operator::And => {
                                    Some(left_pushdown.and(right_pushdown))
                                },
                                Operator::Or => {
                                    Some(left_pushdown.or(right_pushdown))
                                },
                                _ => {
                                    None
                                }
                            }
                        }
                        _ => None
                    }
                }
            }
        },
        Expr::Not(inner) => {
            let inner_pushdown = push_down_filter(&*inner);
            match inner_pushdown {
                Some(inner_pushdown) => {
                    Some(inner_pushdown.not())
                },
                None => None
            }
        },
        // We could handle more cases here, at least simple ones involving nulls, negations, etc.
        // But this example does not implement that
        _ => None
    }
}

/// Push down a simple binary expression to the index
/// Only a subset of expressions are supported since `a = 1` has to be rewritten as `a_int_max_value >= 1 AND a_int_min_value <= 1`
fn push_down_binary_filter(value: &ScalarValue, op: &Operator) -> Option<SimpleExpr> {
    let (min_col, max_col, sql_value) = match value {
        ScalarValue::Int8(v) => {
            match v {
                Some(v) => (ColumnStatistics::IntMinValue, ColumnStatistics::IntMaxValue, SqlValue::Int(Some(*v as i32))),
                None => (ColumnStatistics::IntMinValue, ColumnStatistics::IntMaxValue, SqlValue::Int(None)),
            }
        },
        ScalarValue::UInt8(v) => {
            match v {
                Some(v) => (ColumnStatistics::IntMinValue, ColumnStatistics::IntMaxValue, SqlValue::Int(Some(*v as i32))),
                None => (ColumnStatistics::IntMinValue, ColumnStatistics::IntMaxValue, SqlValue::Int(None)),
            }
        },
        ScalarValue::Int16(v) => {
            match v {
                Some(v) => (ColumnStatistics::IntMinValue, ColumnStatistics::IntMaxValue, SqlValue::Int(Some(*v as i32))),
                None => (ColumnStatistics::IntMinValue, ColumnStatistics::IntMaxValue, SqlValue::Int(None)),
            }
        },
        ScalarValue::UInt16(v) => {
            match v {
                Some(v) => (ColumnStatistics::IntMinValue, ColumnStatistics::IntMaxValue, SqlValue::Int(Some(*v as i32))),
                None => (ColumnStatistics::IntMinValue, ColumnStatistics::IntMaxValue, SqlValue::Int(None)),
            }
        },
        ScalarValue::Int32(v) => {
            match v {
                Some(v) => (ColumnStatistics::IntMinValue, ColumnStatistics::IntMaxValue, SqlValue::Int(Some(*v))),
                None => (ColumnStatistics::IntMinValue, ColumnStatistics::IntMaxValue, SqlValue::Int(None)),
            }
        },
        ScalarValue::UInt32(v) => {
            match v {
                Some(v) => (ColumnStatistics::IntMinValue, ColumnStatistics::IntMaxValue, SqlValue::BigInt(Some(*v as i64))),
                None => (ColumnStatistics::IntMinValue, ColumnStatistics::IntMaxValue, SqlValue::BigInt(None)),
            }
        },
        ScalarValue::Int64(v) => {
            match v {
                Some(v) => (ColumnStatistics::IntMinValue, ColumnStatistics::IntMaxValue, SqlValue::BigInt(Some(*v))),
                None => (ColumnStatistics::IntMinValue, ColumnStatistics::IntMaxValue, SqlValue::BigInt(None)),
            }
        },
        ScalarValue::Utf8(v) => {
            match v {
                Some(v) => (ColumnStatistics::StringMinValue, ColumnStatistics::StringMaxValue, SqlValue::String(Some(Box::new(v.clone())))),
                None => (ColumnStatistics::StringMinValue, ColumnStatistics::StringMaxValue, SqlValue::String(None)),
            }
        },
        ScalarValue::LargeUtf8(v) => {
            match v {
                Some(v) => (ColumnStatistics::StringMinValue, ColumnStatistics::StringMaxValue, SqlValue::String(Some(Box::new(v.clone())))),
                None => (ColumnStatistics::StringMinValue, ColumnStatistics::StringMaxValue, SqlValue::String(None)),
            }
        },
        _ => return None,
    };
    let min_col = SeaQExpr::col(min_col);
    let max_col = SeaQExpr::col(max_col);
    let expr = match op {
        Operator::Eq => {
            min_col.lte(sql_value.clone()).and(max_col.gte(sql_value))
        },
        Operator::Gt => {
            max_col.gt(sql_value)
        },
        Operator::Lt => {
            min_col.lt(sql_value)
        },
        Operator::GtEq => {
            max_col.gte(sql_value)
        },
        Operator::LtEq => {
            min_col.lte(sql_value)
        },
        // In theory we could handle LIKE for the limited but common case of a prefix match
        // and maybe other operators, but this example does not implement that
        _ => return None
    };
    Some(expr)
}