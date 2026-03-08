use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::arrow::array::{Array, Float64Array, Int32Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::functions_aggregate::expr_fn::{avg, count, sum};

// ── pretty-printer ────────────────────────────────────────────────────────────

fn cell_str(batch: &RecordBatch, col_idx: usize, row: usize) -> String {
    let col = batch.column(col_idx);
    if col.is_null(row) { return "NULL".to_string(); }
    let any = col.as_any();
    if let Some(a) = any.downcast_ref::<Int64Array>()   { return format!("{}", a.value(row)); }
    if let Some(a) = any.downcast_ref::<Int32Array>()   { return format!("{}", a.value(row)); }
    if let Some(a) = any.downcast_ref::<Float64Array>() { return format!("{:.4}", a.value(row)); }
    if let Some(a) = any.downcast_ref::<StringArray>()  { return a.value(row).to_string(); }
    datafusion::arrow::util::display::array_value_to_string(col.as_ref(), row)
        .unwrap_or_else(|_| "?".to_string())
}

fn print_batches(batches: &[RecordBatch]) {
    if batches.is_empty() { println!("  (no results)"); return; }
    let schema   = batches[0].schema();
    let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    let num_cols = col_names.len();

    let mut rows: Vec<Vec<String>> = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            rows.push((0..num_cols).map(|c| cell_str(batch, c, row)).collect());
        }
    }

    let mut widths: Vec<usize> = col_names.iter().map(|n| n.len()).collect();
    for row in &rows {
        for (i, cell) in row.iter().enumerate() { widths[i] = widths[i].max(cell.len()); }
    }

    let sep    = widths.iter().map(|w| "-".repeat(w + 2)).collect::<Vec<_>>();
    let border = format!("+{}+", sep.join("+"));
    println!("{border}");
    let hdr = col_names.iter().enumerate()
        .map(|(i, n)| format!(" {:width$} ", n, width = widths[i]))
        .collect::<Vec<_>>();
    println!("|{}|", hdr.join("|"));
    println!("{border}");
    for row in &rows {
        let cells = row.iter().enumerate()
            .map(|(i, c)| format!(" {:width$} ", c, width = widths[i]))
            .collect::<Vec<_>>();
        println!("|{}|", cells.join("|"));
    }
    println!("{border}");
}

// ── Aggregation 1: Trips & Revenue by Month ───────────────────────────────────

async fn agg1_dataframe(ctx: &SessionContext) -> datafusion::error::Result<()> {
    println!("\n--- DataFrame API ---");
    let df = ctx
        .table("yellow_trips").await?
        .with_column("pickup_month", date_part(lit("month"), col("tpep_pickup_datetime")))?
        .aggregate(
            vec![col("pickup_month")],
            vec![
                count(lit(1i64)).alias("trip_count"),
                sum(col("total_amount")).alias("total_revenue"),
                avg(col("fare_amount")).alias("avg_fare"),
            ],
        )?
        .sort(vec![col("pickup_month").sort(true, true)])?;
    print_batches(&df.collect().await?);
    Ok(())
}

async fn agg1_sql(ctx: &SessionContext) -> datafusion::error::Result<()> {
    println!("\n--- SQL ---");
    let df = ctx.sql(
        "SELECT
             date_part('month', tpep_pickup_datetime) AS pickup_month,
             COUNT(*)                                  AS trip_count,
             ROUND(SUM(total_amount), 2)               AS total_revenue,
             ROUND(AVG(fare_amount),  4)               AS avg_fare
         FROM yellow_trips
         GROUP BY pickup_month
         ORDER BY pickup_month ASC"
    ).await?;
    print_batches(&df.collect().await?);
    Ok(())
}

// ── Aggregation 2: Tip Behavior by Payment Type ───────────────────────────────

async fn agg2_dataframe(ctx: &SessionContext) -> datafusion::error::Result<()> {
    println!("\n--- DataFrame API ---");

    // Step 1: aggregate — collect sum_tip and sum_total as separate columns
    // (DataFusion does not allow dividing two agg expressions inside .aggregate())
    let df = ctx
        .table("yellow_trips").await?
        .aggregate(
            vec![col("payment_type")],
            vec![
                count(lit(1i64)).alias("trip_count"),
                avg(col("tip_amount")).alias("avg_tip_amount"),
                sum(col("tip_amount")).alias("sum_tip"),
                sum(col("total_amount")).alias("sum_total"),
            ],
        )?
        // Step 2: derive tip_rate = sum_tip / sum_total as a new column
        .with_column("tip_rate", (col("sum_tip") / col("sum_total")).alias("tip_rate"))?
        // Step 3: drop the helper columns, keep only the required ones
        .select(vec![
            col("payment_type"),
            col("trip_count"),
            col("avg_tip_amount"),
            col("tip_rate"),
        ])?
        .sort(vec![col("trip_count").sort(false, true)])?;

    print_batches(&df.collect().await?);
    Ok(())
}

async fn agg2_sql(ctx: &SessionContext) -> datafusion::error::Result<()> {
    println!("\n--- SQL ---");
    let df = ctx.sql(
        "SELECT
             payment_type,
             COUNT(*)                                       AS trip_count,
             ROUND(AVG(tip_amount),  4)                     AS avg_tip_amount,
             ROUND(SUM(tip_amount) / SUM(total_amount), 6)  AS tip_rate
         FROM yellow_trips
         GROUP BY payment_type
         ORDER BY trip_count DESC"
    ).await?;
    print_batches(&df.collect().await?);
    Ok(())
}

// ── main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║   NYC TLC Yellow Taxi 2025  —  DataFusion Analysis          ║");
    println!("╚══════════════════════════════════════════════════════════════╝");

    // ── find parquet files ────────────────────────────────────────────────────
    let data_dir = std::path::Path::new("data");
    if !data_dir.exists() {
        eprintln!("ERROR: 'data/' directory not found. Run: bash download_data.sh");
        std::process::exit(1);
    }

    let mut file_paths: Vec<String> = std::fs::read_dir(data_dir)
        .expect("Cannot read data/ directory")
        .filter_map(|e: Result<std::fs::DirEntry, _>| e.ok())
        .filter(|e: &std::fs::DirEntry| {
            e.path().extension().map(|x| x == "parquet").unwrap_or(false)
        })
        .map(|e: std::fs::DirEntry| e.path().to_string_lossy().to_string())
        .collect();

    if file_paths.is_empty() {
        eprintln!("ERROR: No Parquet files found in data/. Run: bash download_data.sh");
        std::process::exit(1);
    }

    file_paths.sort();
    println!("\nFound {} Parquet file(s):", file_paths.len());
    for p in &file_paths { println!("  • {p}"); }

    // ── register table ────────────────────────────────────────────────────────
    let ctx = SessionContext::new();

    let df = ctx
        .read_parquet(file_paths[0].clone(), ParquetReadOptions::default())
        .await
        .expect("Failed to read first parquet file");

    let df_schema = df.schema();
    let arrow_schema = Arc::new(Schema::new(
        df_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect::<Vec<_>>(),
    ));

    let listing_url  = ListingTableUrl::parse("data/")?;
    let listing_opts = ListingOptions::new(Arc::new(ParquetFormat::new()))
        .with_file_extension(".parquet");

    let listing_cfg = ListingTableConfig::new(listing_url)
        .with_listing_options(listing_opts)
        .with_schema(arrow_schema);

    ctx.register_table(
        "yellow_trips",
        Arc::new(ListingTable::try_new(listing_cfg)?),
    )?;

    println!("\nTable 'yellow_trips' registered ({} file(s)).\n", file_paths.len());

    // ── run aggregations ──────────────────────────────────────────────────────
    println!("════════════════════════════════════════════════════════════════");
    println!("  AGGREGATION 1: Trips & Revenue by Month");
    println!("  Group by: pickup month  |  Sort: month ASC");
    println!("  Columns:  pickup_month, trip_count, total_revenue, avg_fare");
    println!("════════════════════════════════════════════════════════════════");
    agg1_dataframe(&ctx).await?;
    agg1_sql(&ctx).await?;

    println!("\n════════════════════════════════════════════════════════════════");
    println!("  AGGREGATION 2: Tip Behavior by Payment Type");
    println!("  Group by: payment_type  |  Sort: trip_count DESC");
    println!("  Columns:  payment_type, trip_count, avg_tip_amount, tip_rate");
    println!("  (1=Credit card  2=Cash  3=No charge  4=Dispute  5=Unknown)");
    println!("════════════════════════════════════════════════════════════════");
    agg2_dataframe(&ctx).await?;
    agg2_sql(&ctx).await?;

    Ok(())
}