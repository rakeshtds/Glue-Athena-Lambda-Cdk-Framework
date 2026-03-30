// ============================================================
// PSUDLGluePartitionRefresher.ts
// CDK Construct — wires partition auto-refresh per table:
//   Lambda (MSCK REPAIR via Athena)
//   S3 event notification  (strategy: event / both)
//   EventBridge cron rule  (strategy: schedule / both)
// ============================================================

import { Construct } from "constructs";
import * as cdk     from "aws-cdk-lib";
import * as lambda  from "aws-cdk-lib/aws-lambda";
import * as s3      from "aws-cdk-lib/aws-s3";
import * as s3n     from "aws-cdk-lib/aws-s3-notifications";
import * as events  from "aws-cdk-lib/aws-events";
import * as targets from "aws-cdk-lib/aws-events-targets";
import * as iam     from "aws-cdk-lib/aws-iam";
import * as logs    from "aws-cdk-lib/aws-logs";
import { ResolvedTableConfig } from "./PSUDLGlueTypes";

export interface PSUDLGluePartitionRefresherProps {
  schema: ResolvedTableConfig;
  bucket: s3.IBucket;
}

export class PSUDLGluePartitionRefresher extends Construct {

  public readonly refreshLambda: lambda.Function;

  constructor(scope: Construct, id: string, props: PSUDLGluePartitionRefresherProps) {
    super(scope, id);

    const { schema, bucket } = props;
    const strategy           = schema.refresh.strategy;

    // ── LAMBDA IAM ROLE ──────────────────────────────────────
    const lambdaRole = new iam.Role(this, "RefresherRole", {
      assumedBy:       new iam.ServicePrincipal("lambda.amazonaws.com"),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSLambdaBasicExecutionRole"),
      ],
    });

// ── GLUE PERMISSIONS ─────────────────────────────────────
	lambdaRole.addToPolicy(new iam.PolicyStatement({
	actions: [
		"glue:GetDatabase",
		"glue:GetDatabases",
		"glue:GetTable",
		"glue:GetTables",
		"glue:GetPartition",
		"glue:GetPartitions",
		"glue:BatchCreatePartition",
		"glue:CreatePartition",
		"glue:UpdatePartition",
	],
	resources: [
		`arn:aws:glue:${cdk.Stack.of(this).region}:${cdk.Stack.of(this).account}:catalog`,
		`arn:aws:glue:${cdk.Stack.of(this).region}:${cdk.Stack.of(this).account}:database/${schema.database}`,
		`arn:aws:glue:${cdk.Stack.of(this).region}:${cdk.Stack.of(this).account}:table/${schema.database}/*`,
	],
	}));
	
	// ── ATHENA PERMISSIONS ───────────────────────────────────  ← ADD THIS BLOCK HERE
	lambdaRole.addToPolicy(new iam.PolicyStatement({
	actions: [
		"athena:StartQueryExecution",
		"athena:GetQueryExecution",
		"athena:GetQueryResults",
		"athena:GetWorkGroup",
		"athena:ListWorkGroups",
	],
	resources: ["*"],
	}));

    bucket.grantRead(lambdaRole);
    bucket.grantWrite(lambdaRole, "athena-results/*");

    // ── LAMBDA FUNCTION ──────────────────────────────────────
    this.refreshLambda = new lambda.Function(this, "RefresherFn", {
      functionName: `psudl-glue-refresh-${schema.database}-${schema.tableName}`,
      runtime:      lambda.Runtime.NODEJS_20_X,
      handler:      "index.handler",
      code:         lambda.Code.fromInline(this.buildHandlerCode()),
      role:         lambdaRole,
      timeout:      cdk.Duration.minutes(5),
      memorySize:   256,
      environment: {
        DATABASE:        schema.database,
        TABLE:           schema.tableName,
        BUCKET:          bucket.bucketName,
        ATHENA_OUTPUT:   `s3://${bucket.bucketName}/athena-results/`,
        HAS_PARTITIONS:  schema.partitions ? "true" : "false",
      },
      logRetention: logs.RetentionDays.ONE_WEEK,
    });

    // ── S3 EVENT TRIGGER ─────────────────────────────────────
    if (strategy === "event" || strategy === "both") {
      bucket.addEventNotification(
        s3.EventType.OBJECT_CREATED,
        new s3n.LambdaDestination(this.refreshLambda),
        { prefix: schema.s3Location.prefix }
      );
    }

    // ── EVENTBRIDGE SCHEDULE ─────────────────────────────────
    if (strategy === "schedule" || strategy === "both") {
      if (!schema.refresh.scheduleCron) {
        throw new Error(`scheduleCron required for table ${schema.tableName} when strategy is '${strategy}'`);
      }
      const rule = new events.Rule(this, "ScheduleRule", {
        ruleName:    `psudl-glue-refresh-${schema.database}-${schema.tableName}`,
        schedule:    events.Schedule.expression(`cron(${schema.refresh.scheduleCron})`),
        description: `Partition refresh for ${schema.database}.${schema.tableName}`,
      });
      rule.addTarget(
        new targets.LambdaFunction(this.refreshLambda, {
          event:         events.RuleTargetInput.fromObject({ source: "scheduled" }),
          retryAttempts: 2,
        })
      );
    }
  }

  private buildHandlerCode(): string {
    return `
const { AthenaClient, StartQueryExecutionCommand, GetQueryExecutionCommand } =
  require('@aws-sdk/client-athena');

const athena         = new AthenaClient({});
const DATABASE       = process.env.DATABASE;
const TABLE          = process.env.TABLE;
const ATHENA_OUTPUT  = process.env.ATHENA_OUTPUT;
const HAS_PARTITIONS = process.env.HAS_PARTITIONS === 'true';

exports.handler = async (event) => {
  console.log(JSON.stringify({ source: event.source || 's3-event', database: DATABASE, table: TABLE }));

  if (!HAS_PARTITIONS) {
    console.log('No partitions defined — skipping.');
    return { status: 'no-op' };
  }

  const { QueryExecutionId } = await athena.send(new StartQueryExecutionCommand({
    QueryString:           \`MSCK REPAIR TABLE \\\`\${DATABASE}\\\`.\\\`\${TABLE}\\\`\`,
    QueryExecutionContext: { Database: DATABASE },
    ResultConfiguration:  { OutputLocation: ATHENA_OUTPUT },
    WorkGroup:            'primary',
  }));

  console.log('Athena query started:', QueryExecutionId);

  for (let i = 0; i < 48; i++) {
    await new Promise(r => setTimeout(r, 5000));
    const { QueryExecution } = await athena.send(
      new GetQueryExecutionCommand({ QueryExecutionId })
    );
    const state = QueryExecution?.Status?.State;
    console.log(\`Poll \${i + 1}: \${state}\`);
    if (state === 'SUCCEEDED') return { status: 'success', queryId: QueryExecutionId };
    if (state === 'FAILED' || state === 'CANCELLED') {
      throw new Error(\`MSCK REPAIR TABLE failed: \${QueryExecution?.Status?.StateChangeReason}\`);
    }
  }
  throw new Error('Timed out waiting for MSCK REPAIR TABLE');
};
`;
  }
}
