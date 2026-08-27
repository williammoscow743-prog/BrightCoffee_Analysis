# Databricks notebook source
# MAGIC %md
# MAGIC # ☕ Bright Coffee Shop — Sales EDA & Dashboard
# MAGIC
# MAGIC This notebook explores the Bright Coffee Shop sales dataset (Jan–Jun 2023, ~149K transactions
# MAGIC across 3 NYC store locations) and builds a set of dashboard-ready visualizations.
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC 1. Loads and cleans the raw transaction data
# MAGIC 2. Engineers useful date/time and revenue features
# MAGIC 3. Runs descriptive / data-quality checks
# MAGIC 4. Builds a set of `display()` charts (KPIs, trends, category & product breakdowns,
# MAGIC    peak-hour analysis, store comparison) that can each be pinned to a Databricks **Dashboard**
# MAGIC 5. Produces a few static Matplotlib charts suitable for dropping into a slide deck
# MAGIC 6. Auto-generates a short written summary of business insights at the end
# MAGIC
# MAGIC **How to use:** run all cells top-to-bottom. Every `display()` cell that renders a chart has a
# MAGIC little camera/pin icon in the top-right of the chart — click it (or the "..." menu) and choose
# MAGIC "Add to dashboard" to build a live Databricks Dashboard from this notebook. Instructions are
# MAGIC repeated at the bottom of the notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Configuration
# MAGIC Set the path to the raw CSV. This expects the file to be sitting in a **Unity Catalog Volume**
# MAGIC (paths look like `/Volumes/<catalog>/<schema>/<volume>/...`) rather than the legacy DBFS root —
# MAGIC most current workspaces have the public DBFS root (`/FileStore/...`) disabled for security, so
# MAGIC that path will fail with a `DBFS_DISABLED` error. To upload the file:
# MAGIC 1. In the left sidebar, go to **Catalog**.
# MAGIC 2. Pick (or create) a catalog → schema → **Volume**.
# MAGIC 3. Open the Volume and use **Upload to this volume** to add `Bright_Coffee_Shop_Sales.csv`.
# MAGIC 4. Copy the resulting path and paste it into the `data_path` widget below (or edit the default).

# COMMAND ----------

# Create a notebook widget so the file path is easy to change from the notebook UI
# without editing code — useful once this becomes a scheduled/production notebook.
# NOTE: update this default to your actual catalog/schema/volume names, e.g.
#   /Volumes/main/default/bright_coffee/Bright_Coffee_Shop_Sales.csv
dbutils.widgets.text("data_path", "/Volumes/main/default/bright_coffee/Bright_Coffee_Shop_Sales.csv", "CSV file path")
data_path = dbutils.widgets.get("data_path")
print(f"Reading data from: {data_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports
# MAGIC We use PySpark for the heavy lifting (so this scales past a laptop-sized dataset and so
# MAGIC `display()` gives us native, pin-to-dashboard charts), plus a little Pandas/Matplotlib at the
# MAGIC end for static charts we can export straight into a slide deck.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import matplotlib.pyplot as plt
import pandas as pd

# Cosmetic: keep matplotlib charts clean and readable when we get to the "for slides" section
plt.rcParams["figure.dpi"] = 110

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load raw data
# MAGIC The source CSV is `;`-delimited and uses a **comma as the decimal separator**
# MAGIC (e.g. `3,1` = 3.1), which is common for European-locale exports. We read every column as a
# MAGIC string first so nothing gets silently mangled, then cast types explicitly in the cleaning step.

# COMMAND ----------

raw_df = (
    spark.read
    .option("header", "true")
    .option("sep", ";")
    .option("encoding", "UTF-8")
    .csv(data_path)
)

print(f"Rows loaded: {raw_df.count():,}")
print(f"Columns: {raw_df.columns}")
raw_df.printSchema()

# COMMAND ----------

# Quick visual sanity check of the first few rows straight from the source file
display(raw_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data quality checks
# MAGIC Before trusting any chart, confirm there are no unexpected nulls or duplicate transactions.

# COMMAND ----------

# Count nulls/blank strings per column — a null-heavy column would need a decision
# (drop, impute, or flag) before we build anything on top of it.
null_counts = raw_df.select([
    F.count(F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), c)).alias(c)
    for c in raw_df.columns
])
display(null_counts)

# COMMAND ----------

# Duplicate transaction_id would mean double-counted sales — check for that specifically,
# then check for fully-duplicate rows as a second sanity check.
dup_ids = (
    raw_df.groupBy("transaction_id").count().filter(F.col("count") > 1).count()
)
dup_rows = raw_df.count() - raw_df.dropDuplicates().count()
print(f"Duplicate transaction_id values: {dup_ids}")
print(f"Fully duplicate rows: {dup_rows}")

# COMMAND ----------

# MAGIC %md
# MAGIC Data is clean: no nulls, no duplicate transactions. We can move straight to feature engineering.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Clean & engineer features
# MAGIC - Cast numeric columns to proper types (fixing the comma decimal separator on `unit_price`)
# MAGIC - Parse `transaction_date` / `transaction_time` into real date/time types
# MAGIC - Add a `revenue` column (`transaction_qty * unit_price`) — the dataset has no pre-computed
# MAGIC   sales total, so every downstream revenue chart depends on this
# MAGIC - Add calendar features (month, day-of-week, hour, weekend flag) to support trend & peak-time analysis

# COMMAND ----------

df = (
    raw_df
    # unit_price arrives as e.g. "3,1" — swap the decimal comma for a dot before casting to double
    .withColumn("unit_price", F.regexp_replace(F.col("unit_price"), ",", ".").cast(DoubleType()))
    .withColumn("transaction_qty", F.col("transaction_qty").cast("int"))
    .withColumn("store_id", F.col("store_id").cast("int"))
    .withColumn("product_id", F.col("product_id").cast("int"))
    .withColumn("transaction_date", F.to_date("transaction_date", "yyyy-MM-dd"))
    .withColumn("transaction_time", F.to_timestamp("transaction_time", "HH:mm:ss"))
    # Revenue per line item — the core metric nearly every chart below is built from
    .withColumn("revenue", F.round(F.col("transaction_qty") * F.col("unit_price"), 2))
    # Calendar features for trend / seasonality analysis
    .withColumn("month", F.date_format("transaction_date", "yyyy-MM"))
    .withColumn("day_of_week", F.date_format("transaction_date", "E"))       # Mon, Tue, ...
    .withColumn("hour_of_day", F.hour("transaction_time"))
    .withColumn("is_weekend", F.dayofweek("transaction_date").isin([1, 7]))  # Sun=1, Sat=7
)

# NOTE: not calling .cache() here — explicit caching/persist isn't supported on
# serverless compute (NOT_SUPPORTED_WITH_SERVERLESS). Serverless automatically handles
# reuse/optimization of repeated scans under the hood, so this is safe to skip; on a
# classic (non-serverless) cluster you could add df.cache() back in for a speed-up.
print(f"Cleaned rows: {df.count():,}")
display(df.limit(10))

# COMMAND ----------

# Register as a SQL temp view — lets us mix %sql cells or Databricks SQL dashboard
# queries against the same cleaned data without re-running the Python cleaning logic.
df.createOrReplaceTempView("bright_coffee_sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Descriptive statistics
# MAGIC A standard EDA pass over the numeric columns — sanity-checks ranges (e.g. no negative quantities
# MAGIC or prices) and gives a feel for the spread of order sizes and prices before we dashboard on top.

# COMMAND ----------

display(df.select("transaction_qty", "unit_price", "revenue").describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. 📊 Dashboard section
# MAGIC From here on, each cell produces one `display()` chart. In the Databricks UI, hover over any
# MAGIC chart and use the **"+ Add to dashboard"** button (top-right of the chart) to pin it — repeat
# MAGIC for each chart below and you'll have a full Bright Coffee sales dashboard built from this
# MAGIC notebook. Cell titles below match a natural dashboard layout: KPIs → trends → category/product
# MAGIC breakdowns → timing → store comparison.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 Headline KPIs
# MAGIC A single summary row — total revenue, total transactions, units sold, and average order value.
# MAGIC Pin this as a table/counter tile at the top of the dashboard.

# COMMAND ----------

kpis = df.agg(
    F.round(F.sum("revenue"), 2).alias("total_revenue"),
    F.count("transaction_id").alias("total_transactions"),
    F.sum("transaction_qty").alias("total_units_sold"),
    F.round(F.avg("revenue"), 2).alias("avg_line_item_value"),
    F.countDistinct("store_location").alias("store_count"),
)
display(kpis)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Daily sales trend
# MAGIC Total revenue per calendar day across the full Jan–Jun 2023 window — the main "is the business
# MAGIC growing" chart. Recommended chart type when pinning: **Line**.

# COMMAND ----------

daily_sales = (
    df.groupBy("transaction_date")
    .agg(F.round(F.sum("revenue"), 2).alias("daily_revenue"))
    .orderBy("transaction_date")
)
display(daily_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 Monthly sales trend
# MAGIC Same idea rolled up to month-level, which is easier to read at a glance and better for
# MAGIC month-over-month growth commentary. Recommended chart type: **Bar** or **Line**.

# COMMAND ----------

monthly_sales = (
    df.groupBy("month")
    .agg(
        F.round(F.sum("revenue"), 2).alias("monthly_revenue"),
        F.sum("transaction_qty").alias("units_sold"),
        F.countDistinct("transaction_date").alias("days_in_period"),
    )
    .orderBy("month")
)
display(monthly_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.4 Revenue by product category
# MAGIC Which categories (Coffee, Tea, Bakery, etc.) drive the most revenue. Recommended chart type:
# MAGIC **Bar**, sorted descending — or **Pie** if you want a share-of-revenue view.

# COMMAND ----------

revenue_by_category = (
    df.groupBy("product_category")
    .agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.sum("transaction_qty").alias("units_sold"),
    )
    .orderBy(F.desc("total_revenue"))
)
display(revenue_by_category)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.5 Top 10 products by revenue
# MAGIC Drills below category into the specific `product_type` driving the most sales — useful for
# MAGIC "what should we stock more of" conversations. Recommended chart type: **Bar** (horizontal).

# COMMAND ----------

top_products = (
    df.groupBy("product_type")
    .agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.sum("transaction_qty").alias("units_sold"),
    )
    .orderBy(F.desc("total_revenue"))
    .limit(10)
)
display(top_products)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.6 Peak sales hours
# MAGIC Revenue and transaction count by hour of day — identifies staffing/inventory peaks (coffee
# MAGIC shops classically spike in the morning). Recommended chart type: **Bar** or **Line**.

# COMMAND ----------

sales_by_hour = (
    df.groupBy("hour_of_day")
    .agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.count("transaction_id").alias("transaction_count"),
    )
    .orderBy("hour_of_day")
)
display(sales_by_hour)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.7 Sales by day of week
# MAGIC Shows whether weekends outperform weekdays (or vice versa) — useful for weekend
# MAGIC staffing/promo decisions. Recommended chart type: **Bar**.

# COMMAND ----------

# Order the categorical day names Mon->Sun instead of the default alphabetical sort
day_order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

sales_by_dow = (
    df.groupBy("day_of_week")
    .agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.count("transaction_id").alias("transaction_count"),
    )
    .withColumn("day_order", F.array_position(F.array(*[F.lit(d) for d in day_order]), F.col("day_of_week")))
    .orderBy("day_order")
    .drop("day_order")
)
display(sales_by_dow)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.8 Revenue by store location
# MAGIC Compares the three store locations head-to-head. Recommended chart type: **Bar** or **Pie**.

# COMMAND ----------

revenue_by_store = (
    df.groupBy("store_location")
    .agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.count("transaction_id").alias("transaction_count"),
        F.round(F.avg("revenue"), 2).alias("avg_line_item_value"),
    )
    .orderBy(F.desc("total_revenue"))
)
display(revenue_by_store)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.9 Order value distribution
# MAGIC How big is a typical line-item sale? A right-skewed histogram here would suggest most sales
# MAGIC are small/single-item with a long tail of bigger orders. Recommended chart type: **Histogram**.

# COMMAND ----------

# Cap the display at the 99th percentile so a handful of large outlier orders don't
# squash the histogram's bin widths and hide the shape of the bulk of the data.
p99 = df.approxQuantile("revenue", [0.99], 0.01)[0]
display(df.filter(F.col("revenue") <= p99).select("revenue"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Static charts for a slide deck / presentation
# MAGIC The `display()` charts above are great for a live Databricks Dashboard, but for dropping into
# MAGIC PowerPoint/Google Slides you generally want flat PNG images. This section re-renders the two or
# MAGIC three most "headline" charts with Matplotlib and saves them to disk.

# COMMAND ----------

# Pull the small, already-aggregated tables to the driver as Pandas — safe to do here because
# these are group-by summaries (a handful of rows), not the raw 149K-row dataset.
monthly_pd = monthly_sales.toPandas()
category_pd = revenue_by_category.toPandas()
top_products_pd = top_products.toPandas()
hour_pd = sales_by_hour.toPandas()

# COMMAND ----------

# Chart 1: Monthly revenue trend — the "how's the business doing over time" slide
fig, ax = plt.subplots(figsize=(8, 4.5))
ax.plot(monthly_pd["month"], monthly_pd["monthly_revenue"], marker="o", linewidth=2, color="#6F4E37")
ax.set_title("Bright Coffee — Monthly Revenue (Jan–Jun 2023)")
ax.set_xlabel("Month")
ax.set_ylabel("Revenue ($)")
ax.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.savefig("/tmp/monthly_revenue_trend.png")
plt.show()

# COMMAND ----------

# Chart 2: Revenue by category — the "what are we selling" slide
category_sorted = category_pd.sort_values("total_revenue", ascending=True)
fig, ax = plt.subplots(figsize=(8, 4.5))
ax.barh(category_sorted["product_category"], category_sorted["total_revenue"], color="#A9746E")
ax.set_title("Bright Coffee — Revenue by Product Category")
ax.set_xlabel("Revenue ($)")
plt.tight_layout()
plt.savefig("/tmp/revenue_by_category.png")
plt.show()

# COMMAND ----------

# Chart 3: Peak hours — the "when are we busiest" slide
fig, ax = plt.subplots(figsize=(8, 4.5))
ax.bar(hour_pd["hour_of_day"], hour_pd["total_revenue"], color="#4B3621")
ax.set_title("Bright Coffee — Revenue by Hour of Day")
ax.set_xlabel("Hour (24h)")
ax.set_ylabel("Revenue ($)")
plt.tight_layout()
plt.savefig("/tmp/revenue_by_hour.png")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC PNG files were saved to `/tmp/` on the cluster driver. To get them into a slide deck, copy them
# MAGIC to a Unity Catalog Volume (e.g. `dbutils.fs.cp("file:/tmp/monthly_revenue_trend.png", "/Volumes/main/default/bright_coffee/charts/")`)
# MAGIC and download from there via the Catalog file browser (DBFS/FileStore paths won't work if the
# MAGIC public DBFS root is disabled on your workspace).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Auto-generated business insights
# MAGIC Pulls the top row from each summary table computed above into a short, readable write-up —
# MAGIC handy as the narrative/talking-points slide to go alongside the charts.

# COMMAND ----------

# Grab single "winner" rows from tables we already computed — no new aggregations needed
top_category_row = category_pd.sort_values("total_revenue", ascending=False).iloc[0]
top_product_row = top_products_pd.iloc[0]
peak_hour_row = hour_pd.sort_values("total_revenue", ascending=False).iloc[0]
best_store_row = revenue_by_store.orderBy(F.desc("total_revenue")).toPandas().iloc[0]
best_month_row = monthly_pd.sort_values("monthly_revenue", ascending=False).iloc[0]
total_revenue_val = kpis.collect()[0]["total_revenue"]

insights = f"""
BRIGHT COFFEE — KEY INSIGHTS (Jan–Jun 2023)

• Total revenue across all stores: ${total_revenue_val:,.2f}
• Top-selling category: {top_category_row['product_category']} (${top_category_row['total_revenue']:,.2f})
• Best-selling product type: {top_product_row['product_type']} (${top_product_row['total_revenue']:,.2f})
• Peak sales hour: {int(peak_hour_row['hour_of_day']):02d}:00 (${peak_hour_row['total_revenue']:,.2f} in revenue)
• Top-performing store: {best_store_row['store_location']} (${best_store_row['total_revenue']:,.2f})
• Strongest month: {best_month_row['month']} (${best_month_row['monthly_revenue']:,.2f})

RECOMMENDATIONS
• Double-check staffing levels around the {int(peak_hour_row['hour_of_day']):02d}:00 peak hour to avoid long queues.
• Consider promoting or bundling top categories/products in weaker categories to lift their share.
• Investigate what the top-performing store is doing differently (foot traffic, staffing, local
  promotions) and evaluate rolling out those practices to the other locations.
• Layer on the previous years' data (if available) to distinguish real seasonality from short-term
  noise before making inventory or staffing changes.
"""
print(insights)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Building a Databricks Dashboard from this notebook
# MAGIC 1. Run every cell above at least once so each `display()` output is rendered.
# MAGIC 2. Hover over a chart output and click **"+ Add to dashboard"** (or the "..." menu → *Add to
# MAGIC    Dashboard*) — do this for the KPI table and each chart in Section 6.
# MAGIC 3. Choose *New Dashboard* the first time, then *Bright Coffee Sales Dashboard* for every
# MAGIC    subsequent chart so they all land in the same place.
# MAGIC 4. Open the Dashboard from the notebook's right-hand sidebar, drag tiles into a layout
# MAGIC    (e.g. KPIs across the top, trend line full-width below, category/product/store charts in a
# MAGIC    grid, peak-hour and day-of-week charts side by side).
# MAGIC 5. Click **Publish** (or **Schedule**) to share a live, refreshable dashboard with the team —
# MAGIC    it will re-run this notebook's queries on whatever schedule you set.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Summary
# MAGIC This notebook took the raw Bright Coffee transaction export, cleaned and typed it correctly,
# MAGIC engineered the revenue and time features the business questions depend on, and produced a set
# MAGIC of dashboard-ready visualizations covering trends, category/product performance, timing, and
# MAGIC store comparison — plus static charts and a narrative summary ready for a presentation deck.
