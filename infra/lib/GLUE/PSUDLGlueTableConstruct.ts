// ============================================================
// PSUDLGlueTableConstruct.ts
// CDK Construct — creates one complete Glue table setup per schema:
//   S3 bucket + Glue Database + Glue Table + IAM reader role
// ============================================================

import { Construct } from "constructs";
import * as cdk  from "aws-cdk-lib";
import * as glue from "aws-cdk-lib/aws-glue";
import * as s3   from "aws-cdk-lib/aws-s3";
import * as iam  from "aws-cdk-lib/aws-iam";
import { ResolvedTableConfig, TableFormat } from "./PSUDLGlueTypes";

interface FormatDescriptor { inputFormat: string; outputFormat: string; }

const FORMAT_DESCRIPTORS: Record<TableFormat, FormatDescriptor> = {
  PARQUET: {
    inputFormat:  "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
    outputFormat: "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
  },
  CSV: {
    inputFormat:  "org.apache.hadoop.mapred.TextInputFormat",
    outputFormat: "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
  },
  JSON: {
    inputFormat:  "org.apache.hadoop.mapred.TextInputFormat",
    outputFormat: "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
  },
  ORC: {
    inputFormat:  "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
    outputFormat: "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
  },
  AVRO: {
    inputFormat:  "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat",
    outputFormat: "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat",
  },
};

export interface PSUDLGlueTableConstructProps {
  schema:        ResolvedTableConfig;
  sharedBucket?: s3.IBucket;
}

export class PSUDLGlueTableConstruct extends Construct {

  public readonly bucket:     s3.IBucket;
  public readonly table:      glue.CfnTable;
  public readonly readerRole: iam.Role;

  constructor(scope: Construct, id: string, props: PSUDLGlueTableConstructProps) {
    super(scope, id);
    const { schema } = props;

    // ── S3 BUCKET ────────────────────────────────────────────
    if (props.sharedBucket) {
      this.bucket = props.sharedBucket;
    } else if (schema.s3Location.bucketName) {
      this.bucket = s3.Bucket.fromBucketName(this, "ExistingBucket", schema.s3Location.bucketName);
    } else {
      this.bucket = new s3.Bucket(this, "DataBucket", {
        bucketName:        `${schema.database}-${schema.tableName}-${cdk.Stack.of(this).account}`,
        encryption:        s3.BucketEncryption.S3_MANAGED,
        blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
        lifecycleRules: [{
          transitions: [
            { storageClass: s3.StorageClass.INFREQUENT_ACCESS, transitionAfter: cdk.Duration.days(90)  },
            { storageClass: s3.StorageClass.GLACIER,           transitionAfter: cdk.Duration.days(365) },
          ],
        }],
        removalPolicy: cdk.RemovalPolicy.RETAIN,
      });
    }

    // ── GLUE DATABASE (idempotent) ────────────────────────────
	const dbId = `GlueDb-${schema.database}`;
	let database = scope.node.tryFindChild(dbId) as glue.CfnDatabase;
	if (!database) {
	database = new glue.CfnDatabase(scope, dbId, {
		catalogId:     cdk.Stack.of(this).account,
		databaseInput: { name: schema.database },
	});
	}

    // ── GLUE TABLE ───────────────────────────────────────────
    const toGlueCols = (cols: ResolvedTableConfig["columns"]) =>
      cols.map((c) => ({ name: c.name, type: c.type, comment: c.comment ?? "" }));

    const fmt = FORMAT_DESCRIPTORS[schema.format];

    this.table = new glue.CfnTable(this, "GlueTable", {
      catalogId:    cdk.Stack.of(this).account,
      databaseName: schema.database,
      tableInput: {
        name:        schema.tableName,
        description: schema.description ?? "",
        tableType:   "EXTERNAL_TABLE",
        parameters:  { classification: schema.format.toLowerCase(), EXTERNAL: "TRUE" },
        storageDescriptor: {
          location:               schema.s3Uri,
          columns:                toGlueCols(schema.columns),
          inputFormat:            fmt.inputFormat,
          outputFormat:           fmt.outputFormat,
          compressed:             schema.format === "PARQUET" || schema.format === "ORC",
          numberOfBuckets:        -1,
          serdeInfo: {
            serializationLibrary: schema.serde.serializationLibrary,
            parameters:           schema.serde.parameters ?? {},
          },
          storedAsSubDirectories: false,
        },
        partitionKeys: schema.partitions ? toGlueCols(schema.partitions.columns) : [],
      },
    });
	
	this.table.addDependency(database);

    // ── IAM READER ROLE ──────────────────────────────────────
    this.readerRole = new iam.Role(this, "ReaderRole", {
      roleName:    `psudl-glue-reader-${schema.database}-${schema.tableName}`,
      assumedBy:   new iam.ServicePrincipal("glue.amazonaws.com"),
      description: `Read access for ${schema.database}.${schema.tableName}`,
    });

    this.bucket.grantRead(this.readerRole, `${schema.s3Location.prefix}*`);

    this.readerRole.addToPolicy(new iam.PolicyStatement({
      actions:   ["glue:GetTable", "glue:GetPartitions", "glue:GetDatabase"],
      resources: [
        `arn:aws:glue:${cdk.Stack.of(this).region}:${cdk.Stack.of(this).account}:catalog`,
        `arn:aws:glue:${cdk.Stack.of(this).region}:${cdk.Stack.of(this).account}:database/${schema.database}`,
        `arn:aws:glue:${cdk.Stack.of(this).region}:${cdk.Stack.of(this).account}:table/${schema.database}/${schema.tableName}`,
      ],
    }));

    // ── TAGS ─────────────────────────────────────────────────
    cdk.Tags.of(this).add("GlueDatabase", schema.database);
    cdk.Tags.of(this).add("GlueTable",    schema.tableName);
    if (schema.tags) {
      Object.entries(schema.tags).forEach(([k, v]) => cdk.Tags.of(this).add(k, v));
    }
  }
}
