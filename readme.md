# NYC TLC Yellow Taxi 2025 — DataFusion Analysis

A Rust application that reads all 11 monthly NYC TLC Yellow Taxi Parquet files for
2025 and runs multiple aggregations using **both** the DataFusion DataFrame API and
DataFusion SQL, printing results as formatted tables in the terminal.

---

## What the project does

- **Downloads** all 11 monthly Parquet files for 2025 from the NYC TLC open-data endpoint and registers them as a single logical DataFusion table.
- **Runs Aggregation 1** — trips and revenue by pickup month — via the DataFrame API *and* via plain SQL.
- **Runs Aggregation 2** — tip behaviour grouped by payment type — via the DataFrame API *and* via plain SQL.
- **Prints** every result as a readable, aligned table directly in the terminal.

---

## Dataset source

[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

Data dictionary:  
[PDF — data_dictionary_trip_records_yellow.pdf](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)

---

## How to download the data

Create a `data/` folder and download each file:

```
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-02.parquet
...
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-12.parquet
```

> **Note:** the repo's `.gitignore` explicitly excludes `data/` and `*.parquet`.  
> **Never commit Parquet files to the repo.**

---

## How to run the project

### Prerequisites

| Tool | Version |
|------|---------|
| Rust | ≥ 1.80 (stable) |
| Cargo | bundled with Rust |

Install Rust via [rustup](https://rustup.rs/):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

### Steps

```bash
# 1. Clone the repo
git clone https://github.com/<your-username>/nyc_taxi_datafusion.git
cd nyc_taxi_datafusion

# 2. Download all 12 monthly Parquet files for 2025
bash download_data.sh

# 3. Build and run (release mode is much faster for large data)
cargo run --release
```

The first `cargo build` will take a few minutes (DataFusion is a large crate).
Subsequent runs compile instantly.

---

## What each aggregation computes

### Aggregation 1 — Trips & Revenue by Month

Groups all trips by the calendar month extracted from `tpep_pickup_datetime` and computes:

| Column | Meaning |
|--------|---------|
| `pickup_month` | Calendar month (1 = January … 12 = December) |
| `trip_count` | Total number of taxi trips in that month |
| `total_revenue` | Sum of `total_amount` (all charges including tips & surcharges) |
| `avg_fare` | Average `fare_amount` (metered fare, before extras) |

Results are sorted by month ascending so you can see seasonal trends at a glance.

### Aggregation 2 — Tip Behaviour by Payment Type

Groups all trips by `payment_type` (1 = Credit card, 2 = Cash, 3 = No charge, 4 = Dispute, …) and computes:

| Column | Meaning |
|--------|---------|
| `payment_type` | TLC payment type code |
| `trip_count` | Number of trips with this payment type |
| `avg_tip` | Average `tip_amount` per trip |
| `tip_rate` | Ratio of total tips to total revenue (`SUM(tip_amount) / SUM(total_amount)`) |

Results are sorted by `trip_count` descending so the most common payment methods appear first.

---

## Screenshot

![output](screenshots\output_1.png)

---

## Project structure

```
nyc_taxi_datafusion/
├── src/
│   └── main.rs          # All Rust source — DataFusion setup + aggregations
├── screenshots/
│   └── output.png       # Terminal screenshot showing all results
├── download_data.sh     # Automated Parquet downloader
├── Cargo.toml
├── .gitignore           # Excludes data/ and *.parquet
└── README.md
```