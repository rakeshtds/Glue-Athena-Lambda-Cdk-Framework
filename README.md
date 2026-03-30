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
