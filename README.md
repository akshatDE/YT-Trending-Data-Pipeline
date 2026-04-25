# YouTube Trending Intelligence — Data Pipeline & Campaign Dashboard

> A cloud-native ETL pipeline that ingests YouTube trending video data across 10 regions,
> transforms it through a medallion architecture (Bronze → Silver → Gold), enforces data
> quality gates, and surfaces campaign intelligence via Amazon QuickSight.

![Dashboard Preview](dashboard/screenshots/00_full_dashboard.png)

---

## What's New (Evolved from Original)

The original pipeline has been extended with three additions:

| Addition | Description |
|---|---|
| `ingestion/download_kaggle.py` | Automates Kaggle dataset download — replaces manual CSV downloads |
| `ingestion/lambda_data_validation.py` | Pre-ingestion validation before data hits Bronze layer |
| `sql/athena/` | 4 Athena queries powering QuickSight SPICE datasets |
| `dashboard/` | Amazon QuickSight campaign dashboard with 4 targeted visuals |

Historical data is now uploaded to S3 with Python-driven partitioning by `region` — replacing the original `aws_copy.sh` shell script approach.

---

## Problem Statement

Marketing team is launching a data-driven YouTube campaign. This project analyzes
trending video data across regions to answer 4 key campaign questions:

| # | Question | Visual |
|---|---|---|
| Q1 | Where should we target? | Scatter plot — engagement vs views by region |
| Q2 | What content should we align with? | Line chart — category view share over time |
| Q3 | Who should we partner with? | Horizontal bar — top 15 channels by consistency |
| Q4 | When should we publish? | Combo chart — daily views vs engagement pulse |

---

## Architecture

```
Data Sources       Bronze            Silver          Quality Gate        Gold             Analytics
┌──────────┐   ┌──────────────┐  ┌──────────────┐  ┌────────────┐  ┌──────────────┐  ┌──────────────┐
│ YouTube  │   │              │  │              │  │            │  │  trending_   │  │    Athena    │
│ API v3   │──>│  Raw JSON    │─>│  Cleansed    │─>│ DQ Lambda  │─>│  analytics   │─>│    Queries   │
│          │   │  (S3)        │  │  Parquet     │  │            │  │              │  │              │
├──────────┤   │              │  │  (S3)        │  │  Validates │  │  channel_    │  ├──────────────┤
│  Kaggle  │   │  Raw CSV     │  │              │  │  row count │  │  analytics   │  │  QuickSight  │
│ Dataset  │──>│  (S3)        │  │  Reference   │  │  nulls     │  │              │  │  Dashboard   │
│(auto dl) │   │              │  │  Parquet     │  │  schema    │  │  category_   │  │              │
└──────────┘   └──────────────┘  └──────────────┘  │  freshness │  │  analytics   │  └──────────────┘
                                                    └────────────┘  └──────────────┘
                                                         │
                                                    fail │
                                                         ▼
                                                   ┌────────────┐
                                                   │ SNS Alert  │
                                                   └────────────┘
```

Orchestration is handled by AWS Step Functions with retry logic, parallel execution,
and SNS failure notifications at every stage.

---

## Tech Stack

| Component | Technology |
|---|---|
| Compute | AWS Lambda, AWS Glue (PySpark) |
| Storage | Amazon S3 (Parquet, Snappy) |
| Orchestration | AWS Step Functions |
| Scheduling | Amazon EventBridge |
| Metadata | AWS Glue Data Catalog |
| Query Engine | Amazon Athena |
| Visualization | Amazon QuickSight (SPICE) |
| Alerting | Amazon SNS |
| Monitoring | Amazon CloudWatch |
| Languages | Python 3, PySpark, SQL (Athena/Presto) |
| Libraries | Pandas, AWS Wrangler, Boto3, kaggle |
| Data Format | Parquet (Snappy compression) |

---

## Project Structure

```
youtube-trending-campaign-analytics/
│
├── data/                              # Reference & historical data
│   ├── {region}videos.csv             # Kaggle trending datasets (10 regions)
│   └── {region}_category_id.json      # YouTube category ID mappings
│
├── ingestion/
│   ├── download_kaggle.py             # Auto-downloads Kaggle dataset + partitions to S3
│   ├── lambda_data_validation.py      # Pre-ingestion validation Lambda
│   └── lambda_youtube_API_Ingestion.py # Fetches live trending data from YouTube API v3
│
├── etl/
│   ├── AWS_glue_bronze_silver.py      # PySpark: raw data → cleansed statistics
│   ├── AWS_glue_silver_gold.py        # PySpark: cleansed data → business aggregations
│   └── lambda_json_parquet.py         # Converts JSON category mappings to Parquet
│
├── sql/
│   └── athena/
│       ├── ds_region_targeting.sql    # Q1 — region engagement comparison
│       ├── ds_category_momentum.sql   # Q2 — category view share over time
│       ├── ds_channel_partners.sql    # Q3 — channel consistency ranking
│       └── ds_publish_timing.sql      # Q4 — daily views vs engagement pulse
│
├── dashboard/
│   ├── screenshots/
│   │   ├── 00_full_dashboard.png
│   │   ├── 01_where_to_target.png
│   │   ├── 02_category_momentum.png
│   │   ├── 03_channel_partners.png
│   │   └── 04_publish_timing.png
│   ├── aws-screenshots/
│   │   ├── step-functions/
│   │   ├── glue-jobs/
│   │   ├── lambda/
│   │   └── athena/
│   └── README.md                      # QuickSight field mappings per visual
│
├── infrastructure/
│   ├── aws_boto.py                    # AWS resource provisioning via Boto3
│   ├── step-functions/
│   │   └── pipeline-state-machine.json # Step Functions state machine (ARNs parameterized)
│   ├── glue/
│   │   └── README.md                  # Crawler config and schedule documentation
│   └── s3/
│       └── lifecycle-rules.json       # Athena results cleanup lifecycle rule
│
├── notebooks/
│   └── testing.ipynb                  # Local exploration and testing
│
├── .env.example                       # Environment variable template
├── .gitignore
├── information.md                     # AWS resource names reference
├── requirements.txt
└── README.md
```

---

## Data Flow

### Bronze Layer
**YouTube API (Lambda)** fetches top 50 trending videos per region, stored as raw JSON:
```
s3://yt-data-pipeline-bronze-${AWS_REGION}-dev/
  youtube/raw_statistics/region=US/date=2026-04-01/hour=12/
  youtube/raw_statistics_reference_data/region=US/
```

**Kaggle historical data** is auto-downloaded via `download_kaggle.py` and uploaded
to Bronze with Python-driven `region=` partitioning — replacing the original shell script.

### Silver Layer
Two parallel transformations run on Bronze data:

**Glue Job: `AWS_glue_bronze_silver.py`**
- Schema enforcement across API JSON and Kaggle CSV formats
- Type casting, null handling, deduplication
- Derived metrics: `like_ratio`, `engagement_rate`
- Output: Parquet/Snappy, partitioned by `region`

**Lambda: `lambda_json_parquet.py`**
- Normalizes JSON category mappings to tabular Parquet
- Deduplicated, partitioned by `region`

### Data Quality Gate
Before Gold, the DQ Lambda validates Silver data:

| Check | Threshold |
|---|---|
| Row count | >= 10 rows |
| Null percentage | <= 5% on critical columns |
| Schema validation | Required columns present |
| Data freshness | < 48 hours since last write |

Pipeline halts and sends SNS alert on failure. Gold does not execute.

### Gold Layer
**Glue Job: `AWS_glue_silver_gold.py`** produces three analytics tables:

**`trending_analytics`** — Daily metrics per region
| Column | Description |
|---|---|
| `region` | Country code |
| `trending_date_parsed` | Date of snapshot |
| `total_views` | Sum of all views |
| `avg_engagement_rate` | Average engagement rate |
| `unique_channels` | Distinct channel count |

**`channel_analytics`** — Channel-level performance
| Column | Description |
|---|---|
| `channel_title` | YouTube channel name |
| `times_trending` | Times appeared in trending |
| `rank_in_region` | Performance rank within region |

**`category_analytics`** — Category breakdowns
| Column | Description |
|---|---|
| `category_name` | Video category |
| `view_share_pct` | Percentage of total views |
| `video_count` | Videos in category |

All Gold tables: Parquet (Snappy), partitioned by `region`, registered in Glue Data Catalog.

---

## Campaign Dashboard

Built on Amazon QuickSight connected to Athena → Glue Data Catalog → Gold S3 tables.
Each visual answers one campaign decision.

### Q1 — Where should we target?
![Q1](dashboard/screenshots/01_where_to_target.png)
Scatter plot: `avg_engagement_rate` (Y) vs `total_views` (X), bubble size = `unique_channels`.
Top-right quadrant = priority markets.

### Q2 — What content should we align with?
![Q2](dashboard/screenshots/02_category_momentum.png)
Line chart: `view_share_pct` over `trending_date_parsed` per `category_name`.
Rising lines = content with growing momentum. Filter by `region`.

### Q3 — Who should we partner with?
![Q3](dashboard/screenshots/03_channel_partners.png)
Horizontal bar: Top 15 channels ranked by `times_trending`.
Long bar = consistent presence. Filter by `region`.

### Q4 — When should we publish?
![Q4](dashboard/screenshots/04_publish_timing.png)
Combo chart: `total_views` (bars) + `avg_engagement_rate` (line) by date.
Days where both peak = optimal publish window.

### QuickSight Setup
1. Data source → Athena → workgroup `primary` → database `yt-pipeline-gold-dev`
2. Create 4 SPICE datasets using queries from `sql/athena/`
3. Build visuals using field mappings in `dashboard/README.md`
4. Add sheet-level `region` filter linked across Q2, Q3, Q4

---

## Setup

### Prerequisites
- AWS account with Lambda, Glue, S3, Step Functions, SNS, Athena, QuickSight access
- YouTube Data API v3 key from Google Cloud Console
- Kaggle API credentials (`~/.kaggle/kaggle.json`)
- AWS CLI configured
- Python 3.9+

### Environment Variables

Create a `.env` file based on `.env.example`:

```bash
# AWS
AWS_ACCOUNT_ID=your-account-id
AWS_REGION=ap-south-1

# S3 Buckets
S3_BUCKET_BRONZE=yt-data-pipeline-bronze-${AWS_REGION}-dev
S3_BUCKET_SILVER=yt-data-pipeline-silver-${AWS_REGION}-dev
S3_BUCKET_GOLD=yt-data-pipeline-gold-${AWS_REGION}-dev

# APIs
YOUTUBE_API_KEY=your-youtube-api-key
YOUTUBE_REGIONS=US,GB,CA,DE,FR,IN,JP,KR,MX,RU

# Glue Databases
GLUE_DB_BRONZE=yt_pipeline_bronze_dev
GLUE_DB_SILVER=yt_pipeline_silver_dev
GLUE_DB_GOLD=yt_pipeline_gold_dev

# Alerts
SNS_ALERT_TOPIC_ARN=arn:aws:sns:${AWS_REGION}:${AWS_ACCOUNT_ID}:yt-data-pipeline-alerts-dev
```

### Install dependencies
```bash
pip install -r requirements.txt
```

### Download historical data
```bash
python ingestion/download_kaggle.py
```

### Deploy infrastructure
```bash
# Create S3 buckets
aws s3 mb s3://yt-data-pipeline-bronze-${AWS_REGION}-dev
aws s3 mb s3://yt-data-pipeline-silver-${AWS_REGION}-dev
aws s3 mb s3://yt-data-pipeline-gold-${AWS_REGION}-dev

# Create Glue databases
aws glue create-database --database-input '{"Name": "yt_pipeline_bronze_dev"}'
aws glue create-database --database-input '{"Name": "yt_pipeline_silver_dev"}'
aws glue create-database --database-input '{"Name": "yt_pipeline_gold_dev"}'

# Deploy Step Functions state machine
aws stepfunctions create-state-machine \
  --name yt-data-pipeline \
  --definition file://infrastructure/step-functions/pipeline-state-machine.json \
  --role-arn arn:aws:iam::${AWS_ACCOUNT_ID}:role/StepFunctionsRole
```

---

## Running the Pipeline

**Manual trigger:**
```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:${AWS_REGION}:${AWS_ACCOUNT_ID}:stateMachine:yt-data-pipeline
```

**Automated (EventBridge):**
```bash
aws events put-rule \
  --name yt-pipeline-schedule \
  --schedule-expression "rate(6 hours)"
```

**Pipeline execution order:**
```
1. Ingestion          → YouTube API → Bronze S3
2. Wait (10s)         → S3 consistency
3. Parallel           → Bronze→Silver Glue job + JSON→Parquet Lambda
4. Data Quality       → Validate Silver (blocks on failure)
5. Gold Aggregation   → Silver→Gold Glue job
6. Notification       → SNS success/failure alert
```

Each step retries up to 3 times with exponential backoff.

---

## Supported Regions

| Code | Country |
|---|---|
| US | United States |
| GB | United Kingdom |
| CA | Canada |
| DE | Germany |
| FR | France |
| IN | India |
| JP | Japan |
| KR | South Korea |
| MX | Mexico |
| RU | Russia |

---

## Monitoring

| Tool | What to check |
|---|---|
| Step Functions Console | Visual execution history, step-level status |
| CloudWatch Logs | Lambda and Glue job detailed logs |
| SNS Email | Pipeline success / failure alerts |
| Athena | Direct Gold table validation queries |
| QuickSight | Campaign dashboard — refresh status in datasets |

---

## Data Sources

- **YouTube Data API v3** — live trending video data (primary source)
- **Kaggle YouTube Trending Dataset** — historical backfill, auto-downloaded via `download_kaggle.py`