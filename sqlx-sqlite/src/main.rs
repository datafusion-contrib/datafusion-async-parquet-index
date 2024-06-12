use std::{
    any::Any, cell::RefCell, fmt::Display, fs::{self, DirEntry, File}, ops::Range, path::{Path, PathBuf}, sync::{Arc, Mutex}
};

use datafusion::arrow::{array::{ArrayRef, Int32Array, RecordBatch, StringArray}, datatypes::{DataType, Field, Schema}};
use datafusion::arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    datasource::{
        listing::PartitionedFile,
        physical_plan::{parquet::ParquetAccessPlan, FileScanConfig, ParquetExec},
        TableProvider,
    },
    execution::{context::SessionState, object_store::ObjectStoreUrl},
    parquet::{arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter}, file::properties::WriterProperties},
    physical_plan::ExecutionPlan,
    prelude::*,
};
use datafusion_common::{internal_datafusion_err, DFSchema, DataFusionError, Result};
use datafusion_expr::{utils::conjunction, TableProviderFilterPushDown, TableType};
use sqlx::SqlitePool;
use tempfile::TempDir;
use url::Url;

use crate::index::SQLiteIndex;

mod index;
mod rewrite;

/// This example demonstrates building a secondary index over multiple Parquet
/// files and using that index during query to skip ("prune") files and row groups
/// that do not contain relevant data.
///
/// This example rules out irrelevant data using min/max values of a column
/// extracted from the Parquet metadata. In a real system, the index could be
/// more sophisticated, e.g. using inverted indices, bloom filters or other
/// techniques.
///
/// Note this is a low level example for people who want to build their own
/// custom indexes. To read a directory of parquet files as a table, you can use
/// a higher level API such as [`SessionContext::read_parquet`] or
/// [`ListingTable`], which also do file pruning based on parquet statistics
/// (using the same underlying APIs)
///
/// # Diagram
///
/// ```text
///                                   ┏━━━━━━━━━━━━━━━━━━━━━━━━┓
///                                   ┃     Index              ┃
///                                   ┃                        ┃
///  step 1: predicate is   ┌ ─ ─ ─ ─▶┃ (sometimes referred to ┃
///  evaluated against                ┃ as a "catalog" or      ┃
///  data in the index      │         ┃ "metastore")           ┃
///                                   ┗━━━━━━━━━━━━━━━━━━━━━━━━┛
///                         │                      │
///
///                         │                      │
/// ┌──────────────┐
/// │  value = 150 │─ ─ ─ ─ ┘                      │
/// └──────────────┘                                   ┌─────────────┐
///  Predicate from query                          │   │             │
///                                                    │  skip file  │
///                                                │   │             │
///                                                    └─────────────┘
///                                                │   ┌─────────────┐
///                   step 2: Index returns only       │  ┌────────┐ │
///                   parquet files that might     │   │  │ scan   │ │
///                   have matching data.              │  │ rg 0   │ │
///                                                │   │  └────────┘ │
///                                                    │  ┌────────┐ │
///                                                │   │  │ skip   │ │
///                                                ─ ▶ │  │ rg 3   │ │
///                   The index can choose to          │  └────────┘ │
///                   scan entire files,               │     ...     │
///                   only some row-groups within      │  ┌────────┐ │
///                   each file, or even               │  │  scan  │ │
///                   individual rows within each      │  │  rg n  │ │
///                   row group (not shown in this)    │  └────────┘ │
///                   example                          └─────────────┘
///                                                          ...
///                                                    ┌─────────────┐
///                                                    │             │
///                                                    └─────────────┘
///                                                     Parquet Files
/// ```
///
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // We use an in-memory SQLite database to store the index for this example
    // But the index could be stored in any database that SQLx supports, including a remote Postgres database
    let pool = SqlitePool::connect("sqlite:index.db").await?;

    // Demo data has three files, each with schema
    // * file_name (string)
    // * value (int32)
    //
    // The files are as follows:
    // * file1.parquet (value: 0..100)
    // * file2.parquet (value: 100..200)
    // * file3.parquet (value: 200..3000)
    let data = DemoData::try_new()?;

    // Create a table provider with and  our special index.
    let index = SQLiteIndex::try_new(
        pool.clone(),
        // You probably don't want to index _every_ column in your data
        // For example, indexing a column of random strings like a UUID would be pointless
        // using a min/max based index like this.
        // In this example we choose to index only the "value" and "text" columns, ignoring "file_name"
        Arc::new(
            Schema::new(
                vec![
                    Field::new("value", DataType::Int32, false),
                    Field::new("text", DataType::Utf8, false),
                ]
            )
        )
    ).await?;
    let provider = Arc::new(IndexTableProvider::try_new(data.path(), index).await?);
    println!("** Table Provider:");
    println!("{provider}\n");

    // Create a SessionContext for running queries that has the table provider
    // registered as "index_table"
    let ctx = SessionContext::new();
    ctx.register_table("index_table", Arc::clone(&provider) as _)?;

    // register object store provider for urls like `file://` work
    let url = Url::try_from("file://").unwrap();
    let object_store = object_store::local::LocalFileSystem::new();
    ctx.register_object_store(&url, Arc::new(object_store));

    // Select data from the table without any predicates (and thus no pruning)
    println!("** Select data, no predicates:");
    ctx.sql("SELECT file_name, value FROM index_table LIMIT 10")
        .await?
        .show()
        .await?;
    println!("Files scanned: {:?}\n", provider.last_execution());

    // Run a query that uses the index to prune files.
    //
    // Using the predicate "value = 150", the IndexTable can skip reading file 1
    // (max value 100) and file 3 (min value of 200)
    println!("** Select data, predicate `value = 150`");
    ctx.sql("SELECT file_name, value FROM index_table WHERE value = 150")
        .await?
        .show()
        .await?;
    println!("Files scanned: {:?}\n", provider.last_execution());

    // likewise, we can use a more complicated predicate like
    // "value < 20 OR value > 500" to read only file 1 and file 3
    println!("** Select data, predicate `value < 20 OR value > 500`");
    ctx.sql(
        "SELECT file_name, count(value) FROM index_table \
            WHERE value < 20 OR value > 500 GROUP BY file_name",
    )
    .await?
    .show()
    .await?;
    println!("Files scanned: {:?}\n", provider.last_execution());


    // it's also possible to combine predicates on multiple columns
    // for example `value < 20 AND text = 'a'` would only read file 1
    // while `value > 500 AND text = 'a'` would read no files
    println!("** Select data, predicate `value < 20 AND text = 'a'`");
    ctx.sql(
        "SELECT file_name, count(value) FROM index_table \
            WHERE value < 20 AND text = 'a' GROUP BY file_name",
    )
    .await?
    .show()
    .await?;
    println!("Files scanned: {:?}\n", provider.last_execution());

    println!("** Select data, predicate `value > 500 AND text = 'a'`");
    ctx.sql(
        "SELECT file_name, count(value) FROM index_table \
            WHERE value > 500 AND text = 'a' GROUP BY file_name",
    )
    .await?
    .show()
    .await?;
    println!("Files scanned: {:?}\n", provider.last_execution());

    pool.close().await;

    Ok(())
}

/// DataFusion `TableProvider` that uses [`IndexTableProvider`], a secondary
/// index to decide which Parquet files and row groups to read.
#[derive(Debug)]
pub struct IndexTableProvider {
    /// The index of the parquet files in the directory
    index: SQLiteIndex,
    /// the directory in which the files are stored
    dir: PathBuf,
    /// The schema of the table
    schema: SchemaRef,
    /// A simple log of the last execution
    last_execution: Mutex<RefCell<SimpleExecutionLog>>
}

impl Display for IndexTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "IndexTableProvider")?;
        writeln!(f, "---- Index ----")?;
        write!(f, "{}", self.index)
    }
}

impl IndexTableProvider {
    /// Create a new IndexTableProvider
    pub async fn try_new(
        dir: impl Into<PathBuf>,
        mut index: SQLiteIndex,
    ) -> anyhow::Result<Self> {
        let dir = dir.into();

        let files = read_dir(&dir)?;
        for file in &files {
            index.add_file(&file.path()).await?;
        }

        // Get the schema of the first file, assume they all have the same schema
        let file = files.first().ok_or_else(|| {
            internal_datafusion_err!("No files found in directory {dir:?}")
        })?;
        let file = File::open(file.path()).map_err(|e| {
            DataFusionError::from(e).context(format!("Error opening file {file:?}"))
        })?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let schema = reader.schema().clone();

        Ok(Self { index, dir, schema, last_execution: Mutex::new(RefCell::new(SimpleExecutionLog::new())) })
    }
}

#[async_trait]
impl TableProvider for IndexTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let df_schema = DFSchema::try_from(self.schema())?;

        // convert filters like [`a = 1`, `b = 2`] to a single filter like `a = 1 AND b = 2`
        let predicate = conjunction(filters.to_vec());
        let predicate = predicate
            .map(|predicate| state.create_physical_expr(predicate, &df_schema))
            .transpose()?
            // if there are no filters, use a literal true to have a predicate
            // that always evaluates to true we can pass to the index
            .unwrap_or_else(|| datafusion_physical_expr::expressions::lit(true));

        // Use the index to find the files that might have data that matches the
        // predicate. Any file that can not have data that matches the predicate
        // will not be returned.
        let files = self.index.get_files(predicate.clone(), self.schema()).await?;

        // Record the last execution for debugging
        self.last_execution.lock().unwrap().get_mut().record(files.iter().map(|(filename, plan)| (filename.clone(), plan.access_plan.clone())).collect());

        let object_store_url = ObjectStoreUrl::parse("file://")?;
        let mut file_scan_config = FileScanConfig::new(object_store_url, self.schema())
            .with_projection(projection.cloned())
            .with_limit(limit);

        // Transform to the format needed to pass to ParquetExec
        // Create one file group per file (default to scanning them all in parallel)
        for (file_name, file_scan_plan) in files {
            let path = self.dir.join(file_name);
            let canonical_path = fs::canonicalize(path)?;
            file_scan_config = file_scan_config.with_file(
                PartitionedFile::new(
                    canonical_path.display().to_string(),
                    file_scan_plan.file_size,
                ).with_extensions(Arc::new(file_scan_plan.access_plan))
            );
        }

        let exec = ParquetExec::builder(file_scan_config)
            .with_predicate(predicate)
            .build_arc();

        Ok(exec)
    }

    /// Tell DataFusion to push filters down to the scan method
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // Inexact because the pruning can't handle all expressions and pruning
        // is not done at the row level -- there may be rows in returned files
        // that do not pass the filter
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

impl IndexTableProvider {
    pub fn last_execution(&self) -> Vec<(String, ParquetAccessPlan)> {
        self.last_execution.lock().unwrap().borrow().last_execution()
    }
}

/// Demonstration Data
///
/// Makes a directory with three parquet files
///
/// The schema of the files is
/// * file_name (string)
/// * value (int32)
///
/// The files are as follows:
/// * file1.parquet (values 0..100)
/// * file2.parquet (values 100..200)
/// * file3.parquet (values 200..3000)
struct DemoData {
    tmpdir: TempDir,
}

impl DemoData {
    fn try_new() -> Result<Self> {
        let tmpdir = TempDir::new()?;
        make_demo_file(tmpdir.path().join("file1.parquet"), 0..100)?;
        make_demo_file(tmpdir.path().join("file2.parquet"), 100..200)?;
        make_demo_file(tmpdir.path().join("file3.parquet"), 200..3000)?;

        Ok(Self { tmpdir })
    }

    fn path(&self) -> PathBuf {
        self.tmpdir.path().into()
    }
}

/// Creates a new parquet file at the specified path.
///
/// The `value` column  increases sequentially from `min_value` to `max_value`
/// with the following schema:
///
/// * file_name: Utf8
/// * value: Int32
/// * text: Utf8
fn make_demo_file(path: impl AsRef<Path>, value_range: Range<i32>) -> Result<()> {
    let path = path.as_ref();
    let file = File::create(path)?;
    let filename = path
        .file_name()
        .ok_or_else(|| internal_datafusion_err!("No filename"))?
        .to_str()
        .ok_or_else(|| internal_datafusion_err!("Invalid filename"))?;

    let num_values = value_range.len();
    let file_names = StringArray::from_iter_values(std::iter::repeat(&filename).take(num_values));
    let values = Int32Array::from_iter_values(value_range.clone());

    fn int_to_chars(mut n: i32) -> String {
        let mut result = String::new();
        while n > 0 {
            n -= 1;
            let c = (n % 26) as u8 + b'a';
            result.push(c as char);
            n /= 26;
        }
        result.chars().rev().collect()
    }

    let texts: StringArray = value_range
        .map(int_to_chars)
        .collect::<Vec<_>>()
        .into();
    let batch = RecordBatch::try_from_iter(vec![
        ("file_name", Arc::new(file_names) as ArrayRef),
        ("value", Arc::new(values) as ArrayRef),
        ("text", Arc::new(texts) as ArrayRef),
    ])?;

    let schema = batch.schema();

    // write the actual values to the file
    let props = WriterProperties::builder().set_max_row_group_size(50).build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.finish()?;

    Ok(())
}

/// Return a list of the directory entries in the given directory, sorted by name
fn read_dir(dir: &Path) -> Result<Vec<DirEntry>> {
    let mut files = dir
        .read_dir()
        .map_err(|e| DataFusionError::from(e).context(format!("Error reading directory {dir:?}")))?
        .map(|entry| {
            entry.map_err(|e| {
                DataFusionError::from(e)
                    .context(format!("Error reading directory entry in {dir:?}"))
            })
        })
        .collect::<Result<Vec<DirEntry>>>()?;
    files.sort_by_key(|entry| entry.file_name());
    Ok(files)
}


#[derive(Debug, Clone)]
pub struct SimpleExecutionLog {
    last_execution: Vec<(String, ParquetAccessPlan)>,
}

impl SimpleExecutionLog {
    fn new() -> Self {
        Self {
            last_execution: vec![],
        }
    }

    fn record(&mut self, plan: Vec<(String, ParquetAccessPlan)>) {
        self.last_execution = plan;
    }

    fn last_execution(&self) -> Vec<(String, ParquetAccessPlan)> {
        self.last_execution.clone()
    }
}
