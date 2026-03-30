// ============================================================
// PSUDLGlueTableFramework.ts
// Orchestrator CDK Construct.
// Reads all schema files → creates one PSUDLGlueTableConstruct
// + one PSUDLGluePartitionRefresher per table.
// ============================================================

import { Construct } from "constructs";
import * as s3 from "aws-cdk-lib/aws-s3";
import { PSUDLGlueSchemaLoader }      from "./PSUDLGlueSchemaLoader";
import { PSUDLGlueTableConstruct }    from "./PSUDLGlueTableConstruct";
import { PSUDLGluePartitionRefresher } from "./PSUDLGluePartitionRefresher";
import { ResolvedTableConfig }         from "./PSUDLGlueTypes";

export interface PSUDLGlueTableFrameworkProps {
  schemaDirectory?: string;
  schemas?:         ResolvedTableConfig[];
  sharedBucket?:    s3.IBucket;
}

export class PSUDLGlueTableFramework extends Construct {

  public readonly tables:          Map<string, PSUDLGlueTableConstruct>      = new Map();
  public readonly refreshers:      Map<string, PSUDLGluePartitionRefresher>  = new Map();
  public readonly resolvedSchemas: ResolvedTableConfig[];

  constructor(scope: Construct, id: string, props: PSUDLGlueTableFrameworkProps) {
    super(scope, id);

    this.resolvedSchemas = props.schemas
      ?? PSUDLGlueSchemaLoader.loadFromDirectory(props.schemaDirectory!);

    if (this.resolvedSchemas.length === 0) {
      throw new Error("PSUDLGlueTableFramework: No schemas loaded.");
    }

    console.log(
      `[PSUDLGlueTableFramework] Deploying ${this.resolvedSchemas.length} table(s): ` +
      this.resolvedSchemas.map((s) => `${s.database}.${s.tableName}`).join(", ")
    );

    for (const schema of this.resolvedSchemas) {
      const safeId = `${schema.database}-${schema.tableName}`;

      const tableConstruct = new PSUDLGlueTableConstruct(this, `Table-${safeId}`, {
        schema,
        sharedBucket: props.sharedBucket,
      });
      this.tables.set(schema.tableName, tableConstruct);

      const refresher = new PSUDLGluePartitionRefresher(this, `Refresher-${safeId}`, {
        schema,
        bucket: tableConstruct.bucket,
      });
      this.refreshers.set(schema.tableName, refresher);
    }
  }
}
