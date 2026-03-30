// ============================================================
// PSUDLGlueTableStack.ts
// The CDK Stack class imported and instantiated by infra.ts.
// No cdk.App here — app is owned by infra/bin/app.ts.
//
// Usage in infra.ts:
//   import { PSUDLGlueTableStack } from "../lib/GLUE/PSUDLGlueTableStack";
//
//   new PSUDLGlueTableStack(app, "PSUDLGlueStack-dev", {
//     env:             { account: config.account, region: config.region },
//     environment:     config.environment,
//     schemaDirectory: path.join(__dirname, "../schemas"),
//     bucketName:      config.dataLakeBucketName,
//   });
// ============================================================

import * as cdk  from "aws-cdk-lib";
import * as s3   from "aws-cdk-lib/aws-s3";
import * as path from "path";
import { Construct } from "constructs";
import { PSUDLGlueTableFramework }    from "./PSUDLGlueTableFramework";
import { PSUDLGlueTableConstruct }    from "./PSUDLGlueTableConstruct";
import { PSUDLGluePartitionRefresher } from "./PSUDLGluePartitionRefresher";
import { ResolvedTableConfig }         from "./PSUDLGlueTypes";

export interface PSUDLGlueTableStackProps extends cdk.StackProps {
  /** Path to folder containing .json / .yaml schema files */
  schemaDirectory?: string;
  /** Shared S3 bucket name — auto-named if not provided */
  bucketName?:      string;
  /** Pass an existing bucket — takes precedence over bucketName */
  existingBucketName?: string;
  /** Supply schemas programmatically instead of loading from files */
  schemas?:         ResolvedTableConfig[];
  /** Environment: dev | staging | prod */
  environment:      string;
}

export class PSUDLGlueTableStack extends cdk.Stack {

  public readonly dataLakeBucket:  s3.IBucket;
  public readonly glueFramework:   PSUDLGlueTableFramework;
  public readonly tables:          Map<string, PSUDLGlueTableConstruct>;
  public readonly refreshers:      Map<string, PSUDLGluePartitionRefresher>;
  public readonly resolvedSchemas: ResolvedTableConfig[];

  constructor(scope: Construct, id: string, props: PSUDLGlueTableStackProps) {
    super(scope, id, props);

    const { environment, schemaDirectory, bucketName, existingBucketName, schemas } = props;

    // ── STACK-WIDE TAGS ──────────────────────────────────────
    cdk.Tags.of(this).add("ManagedBy",   "PSUDLGlueTableStack");
    cdk.Tags.of(this).add("Environment", environment);
    cdk.Tags.of(this).add("Domain",      "finance");
    cdk.Tags.of(this).add("CostCenter",  "data-platform");

    // ── S3 DATA LAKE BUCKET ──────────────────────────────────
    // ── S3 DATA LAKE BUCKET ──────────────────────────────────
	if (props.existingBucketName) {
	// Reference existing bucket by name — inside Stack scope ✅
	this.dataLakeBucket = s3.Bucket.fromBucketName(
		this,                          // ← 'this' is the Stack — correct scope
		"DataLakeBucket",
		props.existingBucketName
	);
	} else {
	const resolvedName = props.bucketName
		?? `psudl-data-lake-${this.account}-${this.region}`;
	
	this.dataLakeBucket = new s3.Bucket(this, "DataLakeBucket", {
		bucketName:        resolvedName,
		encryption:        s3.BucketEncryption.S3_MANAGED,
		blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
		versioned:         false,
		lifecycleRules: [{
		transitions: [
			{ storageClass: s3.StorageClass.INFREQUENT_ACCESS, transitionAfter: cdk.Duration.days(90)  },
			{ storageClass: s3.StorageClass.GLACIER,           transitionAfter: cdk.Duration.days(365) },
		],
		}],
		removalPolicy: cdk.RemovalPolicy.RETAIN,
	});
	}

    // ── GLUE TABLE FRAMEWORK ─────────────────────────────────
    const resolvedSchemaDir = schemaDirectory
      ?? path.join(__dirname, "../../schemas");

    this.glueFramework = new PSUDLGlueTableFramework(this, "PSUDLGlueTableFramework", {
      schemaDirectory: schemas ? undefined : resolvedSchemaDir,
      schemas,
      sharedBucket:    this.dataLakeBucket,
    });

    // ── SHORTCUTS ────────────────────────────────────────────
    this.tables          = this.glueFramework.tables;
    this.refreshers      = this.glueFramework.refreshers;
    this.resolvedSchemas = this.glueFramework.resolvedSchemas;

    // ── CLOUDFORMATION OUTPUTS ───────────────────────────────
    new cdk.CfnOutput(this, "DataLakeBucketName", {
      value:       this.dataLakeBucket.bucketName,
      description: "Shared S3 data lake bucket",
      exportName:  `${id}-DataLakeBucket`,
    });
    new cdk.CfnOutput(this, "GlueDatabase", {
      value:       this.resolvedSchemas[0]?.database ?? "finance_raw",
      description: "Glue database name — use this in Athena",
      exportName:  `${id}-GlueDatabase`,
    });
    new cdk.CfnOutput(this, "TablesDeployed", {
      value:       Array.from(this.tables.keys()).join(", "),
      description: "All Glue tables deployed",
    });
  }
}
