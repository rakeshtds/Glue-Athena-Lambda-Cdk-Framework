// ============================================================
// infra.ts  —  infra/bin/infra.ts
// ============================================================

import * as cdk  from "aws-cdk-lib";
import * as s3   from "aws-cdk-lib/aws-s3";
import * as path from "path";
import { PSUDLGlueTableStack } from "../lib/GLUE";
import config from "../config/dev.json";

const app = new cdk.App();

// ── DEPLOY GLUE TABLE STACK ──────────────────────────────────
const stack = new PSUDLGlueTableStack(app, `PSUDLGlueStack-${config.environment}`, {
  env: {
    account: config.account ?? process.env.CDK_DEFAULT_ACCOUNT,
    region:  config.region  ?? process.env.CDK_DEFAULT_REGION ?? "us-east-1",
  },
  environment:     config.environment,
  schemaDirectory: path.join(__dirname, "../schemas"),
  existingBucketName: config.dataLakeBucketName,  // ← pass name, not bucket object
});