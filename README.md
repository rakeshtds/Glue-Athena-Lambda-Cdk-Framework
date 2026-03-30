# PSU Data Lake — Glue Table Framework

Schema-driven CDK framework that deploys AWS Glue tables from `.json` / `.yaml` schema files,
backed by a shared S3 bucket, with automatic partition refresh via Lambda + Athena MSCK REPAIR TABLE.

---

## Project structure

```
PSU_DATALAKE/
│
├── cdk.json                              CDK configuration
├── tsconfig.json                         TypeScript config
├── package.json                          npm dependencies + scripts
├── .env.example                          Environment variable template
│
└── infra/
    ├── bin/
    │   └── infra.ts                      Stack instantiation — cdk deploy runs this
    │
    ├── config/
    │   ├── dev.json                      Dev environment config
    │   ├── staging.json                  Staging environment config
    │   └── prod.json                     Production environment config
    │
    ├── schemas/
    │   ├── bank_transactions.json        CSV  table — daily partitions
    │   ├── customer_risk_events.yaml     JSON table — hourly partitions
    │   └── bank_transactions_avro.json   AVRO table — daily partitions
    │
    ├── lib/
    │   └── GLUE/
    │       ├── index.ts                  Single barrel export
    │       ├── PSUDLGlueTypes.ts         TypeScript interfaces
    │       ├── PSUDLGlueSchemaLoader.ts  Reads + validates schema files
    │       ├── PSUDLGlueSchemaValidator.ts CLI validator
    │       ├── PSUDLGlueTableConstruct.ts  S3 + Glue DB + Glue Table + IAM
    │       ├── PSUDLGluePartitionRefresher.ts  Lambda + S3 event + EventBridge
    │       ├── PSUDLGlueTableFramework.ts  Orchestrator construct
    │       └── PSUDLGlueTableStack.ts    Stack class — imported by infra.ts
    │
    └── test-data/
        └── s3-layout/raw/               Ready-to-upload test files
            ├── bank_transactions/year=2024/month=03/day=15/
            ├── customer_risk_events/year=2024/month=03/day=15/hour=12/
            ├── customer_risk_events/year=2024/month=03/day=15/hour=19/
            └── bank_transactions_avro/year=2024/month=03/day=15/
```

---

## AWS resources deployed (per table)

| Resource | Purpose |
|----------|---------|
| `AWS::S3::Bucket` | Shared data lake bucket (once per stack) |
| `AWS::Glue::Database` | Glue database (once per database name) |
| `AWS::Glue::Table` | EXTERNAL_TABLE with columns + partitions + SerDe |
| `AWS::Lambda::Function` | Runs MSCK REPAIR TABLE via Athena |
| S3 Event Notification | Fires Lambda on every file upload |
| `AWS::Events::Rule` | EventBridge cron (if strategy = schedule or both) |
| `AWS::IAM::Role` x2 | Lambda execution role + Glue reader role |

---

## Quick start

```bash
# 1. Install
npm install

# 2. Configure — update account ID in infra/config/dev.json
# Get your account ID:
aws sts get-caller-identity --query Account --output text

# 3. Update bucket names in schemas/ to match dev.json

# 4. Validate schemas
npm run validate-schemas

# 5. Bootstrap CDK (first time only per account/region)
npx cdk bootstrap aws://ACCOUNT_ID/us-east-1

# 6. Preview
npx cdk diff

# 7. Deploy
npm run deploy:dev

# 8. Upload test data
BUCKET="psudl-data-lake-dev-$(aws sts get-caller-identity --query Account --output text)"
aws s3 cp infra/test-data/s3-layout/raw/ s3://$BUCKET/raw/ --recursive

# 9. Verify in Athena
# SELECT * FROM finance_raw.bank_transactions
# WHERE year='2024' AND month='03' LIMIT 10;
```

---

## Deploy to other environments

```bash
npm run deploy:staging
npm run deploy:prod
```

Update `infra/config/staging.json` and `infra/config/prod.json` with the correct account IDs and bucket names.

---

## Adding a new table

1. Create a `.json` or `.yaml` file in `infra/schemas/`
2. Run `npm run validate-schemas`
3. Run `npm run deploy:dev`

No TypeScript changes needed.

---

## Schema reference

```yaml
database: finance_raw
tableName: my_table
format: CSV                      # CSV | JSON | PARQUET | AVRO | ORC
description: "My table"

columns:
  - name: id
    type: string
    comment: "Primary key"

partitions:
  columns:
    - name: year
      type: string
    - name: month
      type: string
  s3Pattern: "year={year}/month={month}"

s3Location:
  bucketName: psudl-data-lake-dev-123456789012
  prefix: raw/my_table/

refresh:
  strategy: both                  # event | schedule | both
  scheduleCron: "0/15 * * * ? *"  # every 15 mins (EventBridge 6-field format)

tags:
  Team: data-engineering
  Domain: finance
```

---

## Refresh strategies

| Strategy | Trigger | Best for |
|----------|---------|----------|
| `event` | S3 file upload → Lambda instantly | Streaming, frequent loads |
| `schedule` | EventBridge cron → Lambda | Batch ETL, predictable schedules |
| `both` | Event + schedule as backstop | Critical tables |

---

## Skip validation for local testing

```bash
# Via npm script
npm run validate-schemas:skip

# Via env var
SKIP_VALIDATION=true npx cdk deploy
```

---

## S3 file naming — critical rule

Files MUST use Hive partition format or MSCK REPAIR will not detect them:

```
# CORRECT
s3://bucket/raw/bank_transactions/year=2024/month=03/day=15/file.csv

# WRONG — partitions never detected
s3://bucket/raw/bank_transactions/2024/03/15/file.csv
```

---

## Teardown

```bash
npx cdk destroy
```

The S3 bucket has `RemovalPolicy.RETAIN` and will NOT be auto-deleted.
Delete manually from the AWS console only when data is no longer needed.





Here is a complete explanation of every file in the framework.

---

## The big picture first

```
You write schema files
        ↓
infra.ts calls PSUDLGlueTableStack
        ↓
Stack reads schemas via PSUDLGlueSchemaLoader
        ↓
Creates AWS resources via constructs
        ↓
AWS Lambda auto-refreshes partitions
```

---

## File by file explanation

---

### 1. `PSUDLGlueTypes.ts`
**What it is:** The contract file — defines all TypeScript interfaces.

**What it does:**
- Defines `GlueTableSchema` — the shape every schema file must follow
- Defines `GlueColumn` — what a column looks like (name, type, comment)
- Defines `PartitionConfig` — partition columns and S3 pattern
- Defines `RefreshConfig` — strategy (event/schedule/both) and cron
- Defines `ResolvedTableConfig` — enriched version after loading with s3Uri and serde filled in

**Think of it as:** A blueprint. Every other file imports from here to know what shape the data should be.

```typescript
// Every schema file must match this shape
interface GlueTableSchema {
  database:    string;
  tableName:   string;
  format:      "CSV" | "JSON" | "PARQUET" | "AVRO" | "ORC";
  columns:     GlueColumn[];
  partitions?: PartitionConfig;
  s3Location:  S3LocationConfig;
  refresh:     RefreshConfig;
}
```

---

### 2. `PSUDLGlueSchemaLoader.ts`
**What it is:** The file reader and validator.

**What it does:**
- Scans the `schemas/` folder for `.json` and `.yaml` files
- Parses each file into a TypeScript object
- Validates required fields — throws clear error if anything is missing
- Applies default SerDe config per format (CSV gets LazySimpleSerDe, JSON gets JsonSerDe etc.)
- Resolves the full `s3Uri` — combines bucket name and prefix
- Returns clean array of `ResolvedTableConfig` objects

**Think of it as:** The translator — turns your human-readable schema files into objects the CDK code can use.

```
bank_transactions.json  →  parse  →  validate  →  resolve  →  ResolvedTableConfig
customer_risk_events.yaml →  parse  →  validate  →  resolve  →  ResolvedTableConfig
bank_transactions_avro.json → parse → validate  →  resolve  →  ResolvedTableConfig
```

---

### 3. `PSUDLGlueTableConstruct.ts`
**What it is:** The AWS resource creator — one instance per table.

**What it does:**
- Creates or references the **S3 bucket** for this table
- Creates the **Glue Database** (idempotent — skips if already exists)
- Creates the **Glue Table** as EXTERNAL_TABLE with:
  - All your columns
  - Partition key definitions
  - SerDe configuration (how to read CSV/JSON/AVRO)
  - S3 location pointer
- Creates an **IAM reader role** scoped only to this table
- Applies tags to all resources

**Think of it as:** The builder — takes one schema and stamps out all the AWS resources for that table.

```
ResolvedTableConfig
        ↓
PSUDLGlueTableConstruct
        ↓
  ├── S3 Bucket
  ├── Glue Database
  ├── Glue Table (EXTERNAL_TABLE)
  └── IAM Reader Role
```

---

### 4. `PSUDLGluePartitionRefresher.ts`
**What it is:** The auto-refresh wiring — one instance per table.

**What it does:**
- Creates an **IAM role** for Lambda with Glue + Athena + S3 permissions
- Creates a **Lambda function** containing inline Node.js code that:
  - Receives S3 upload event
  - Calls Athena `StartQueryExecution` with `MSCK REPAIR TABLE`
  - Polls every 5 seconds until query succeeds or fails
- Wires **S3 Event Notification** → Lambda (fires on every file upload to this table's prefix)
- Creates **EventBridge Rule** → Lambda (fires on your cron schedule)
- Strategy from schema controls which triggers are created:
  - `event` → only S3 trigger
  - `schedule` → only cron trigger
  - `both` → both triggers

**Think of it as:** The auto-pilot — makes sure every time a file lands on S3, the Glue catalog is updated within seconds.

```
New file on S3
      ↓
S3 Event Notification
      ↓
Lambda fires
      ↓
Athena: MSCK REPAIR TABLE
      ↓
Glue catalog updated
      ↓
Data queryable in Athena
```

---

### 5. `PSUDLGlueTableFramework.ts`
**What it is:** The orchestrator — loops through all schemas.

**What it does:**
- Calls `PSUDLGlueSchemaLoader` to load all schema files
- Loops through every resolved schema
- For each schema creates one `PSUDLGlueTableConstruct`
- For each schema creates one `PSUDLGluePartitionRefresher`
- Exposes `tables` and `refreshers` maps so you can access individual constructs

**Think of it as:** The manager — reads all your schema files and delegates work to the constructs.

```
schemas/ folder
      ↓
PSUDLGlueSchemaLoader (reads all files)
      ↓
Loop through each schema
      ↓
  ├── PSUDLGlueTableConstruct   (per table)
  └── PSUDLGluePartitionRefresher (per table)
```

---

### 6. `PSUDLGlueTableStack.ts`
**What it is:** The CDK Stack — the only class `infra.ts` imports.

**What it does:**
- Receives props from `infra.ts` (environment, bucket name, schema directory)
- Applies stack-wide tags to every resource
- Creates or references the **shared S3 bucket**
- Instantiates `PSUDLGlueTableFramework`
- Exposes shortcuts to tables and refreshers
- Writes CloudFormation outputs (bucket name, database name, tables deployed)

**Think of it as:** The front door — everything starts here. `infra.ts` knocks on this door and the stack handles everything else.

```
infra.ts
    ↓
PSUDLGlueTableStack
    ├── Tags applied to everything
    ├── S3 bucket (shared)
    └── PSUDLGlueTableFramework
            ├── Table 1 construct + refresher
            ├── Table 2 construct + refresher
            └── Table 3 construct + refresher
```

---

### 7. `PSUDLGlueSchemaValidator.ts`
**What it is:** A CLI tool — runs before deployment.

**What it does:**
- Scans `schemas/` folder
- Validates every schema file
- Prints PASS or FAIL per file with details
- Exits with error code if any fail — stops bad deploys
- Supports `--skip-validation` flag for local testing

**Think of it as:** The safety check — catches mistakes in your schema files before they hit AWS.

```bash
npm run validate-schemas

  PASS  bank_transactions.json
  PASS  customer_risk_events.yaml
  PASS  bank_transactions_avro.json

  All schemas valid. Safe to run: npx cdk deploy
```

---

### 8. `index.ts`
**What it is:** The barrel export file.

**What it does:**
- Re-exports everything from all 6 files above
- Means `infra.ts` only needs one import line

**Think of it as:** The single door — instead of importing from 6 different files, you import everything from one place.

```typescript
// Without index.ts — 6 import lines needed
import { PSUDLGlueTableStack }      from "../lib/GLUE/PSUDLGlueTableStack";
import { PSUDLGlueTableFramework }  from "../lib/GLUE/PSUDLGlueTableFramework";
import { PSUDLGlueTypes }           from "../lib/GLUE/PSUDLGlueTypes";
// ... etc

// With index.ts — one import line
import { PSUDLGlueTableStack, PSUDLGlueTableFramework } from "../lib/GLUE";
```

---

### 9. `infra.ts`
**What it is:** The stack instantiation file — entry point for CDK.

**What it does:**
- Creates the CDK App
- Reads config from `dev.json`
- Instantiates `PSUDLGlueTableStack` with your config values
- Passes schema directory, bucket name, environment

**Think of it as:** The ignition key — this is what `cdk deploy` runs first.

```typescript
new PSUDLGlueTableStack(app, "PSUDLGlueStack-dev", {
  environment:        "dev",
  schemaDirectory:    "./schemas",
  existingBucketName: "psudl-data-lake-dev-454830470924",
});
```

---

### 10. Schema files (`schemas/`)
**What they are:** Your table definitions — the only files you write and maintain.

| File | Table | Format | Partitions |
|------|-------|--------|------------|
| `bank_transactions.json` | `finance_raw.bank_transactions` | CSV | year/month/day |
| `customer_risk_events.yaml` | `finance_raw.customer_risk_events` | JSON | year/month/day/hour |
| `bank_transactions_avro.json` | `finance_raw.bank_transactions_avro` | AVRO | year/month/day |

**Think of them as:** Your instructions — you describe what the table looks like and the framework does all the work.

---

## How all files work together — full sequence

```
npm run validate-schemas
        │
        ▼
PSUDLGlueSchemaValidator.ts  ← checks schemas/ folder
        │
        ▼
npx cdk deploy
        │
        ▼
infra.ts                     ← reads dev.json, creates App
        │
        ▼
PSUDLGlueTableStack.ts       ← creates S3 bucket, calls framework
        │
        ▼
PSUDLGlueTableFramework.ts   ← calls SchemaLoader, loops schemas
        │
        ▼
PSUDLGlueSchemaLoader.ts     ← reads bank_transactions.json etc.
        │
        ▼
PSUDLGlueTableConstruct.ts   ← creates Glue DB + Table + IAM
        │
        ▼
PSUDLGluePartitionRefresher.ts ← creates Lambda + S3 event + EventBridge
        │
        ▼
AWS CloudFormation deploys everything
        │
        ▼
File lands on S3
        │
        ▼
Lambda fires → MSCK REPAIR TABLE → Glue updated → Athena queryable ✅
```

---

## One line summary per file

| File | One line |
|------|----------|
| `PSUDLGlueTypes.ts` | Defines the shape of everything |
| `PSUDLGlueSchemaLoader.ts` | Reads and validates your schema files |
| `PSUDLGlueTableConstruct.ts` | Creates S3 + Glue + IAM per table |
| `PSUDLGluePartitionRefresher.ts` | Creates Lambda + triggers per table |
| `PSUDLGlueTableFramework.ts` | Loops all schemas and calls constructs |
| `PSUDLGlueTableStack.ts` | The Stack — entry point for infra.ts |
| `PSUDLGlueSchemaValidator.ts` | Validates schemas before deploy |
| `index.ts` | Single export point for all files |
| `infra.ts` | Instantiates the stack with your config |
| `schemas/*.json/yaml` | Your table definitions — only files you touch |
