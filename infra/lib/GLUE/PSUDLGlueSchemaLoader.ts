// ============================================================
// PSUDLGlueSchemaLoader.ts
// Reads .json / .yaml schema files, validates them,
// applies SerDe defaults and resolves s3Uri.
// Runs at CDK synth time — not at runtime on AWS.
// ============================================================

import * as fs   from "fs";
import * as path from "path";
import * as yaml from "js-yaml";
import { GlueTableSchema, ResolvedTableConfig, SerdeConfig, TableFormat } from "./PSUDLGlueTypes";

const DEFAULT_SERDES: Record<TableFormat, SerdeConfig> = {
  PARQUET: {
    serializationLibrary: "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
    parameters: { "serialization.format": "1" },
  },
  CSV: {
    serializationLibrary: "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
    parameters: { "field.delim": ",", "line.delim": "\n", "serialization.format": "," },
  },
  JSON: {
    serializationLibrary: "org.openx.data.jsonserde.JsonSerDe",
    parameters: { "ignore.malformed.json": "TRUE" },
  },
  ORC: {
    serializationLibrary: "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
    parameters: {},
  },
  AVRO: {
    serializationLibrary: "org.apache.hadoop.hive.serde2.avro.AvroSerDe",
    parameters: {},
  },
};

export class PSUDLGlueSchemaLoader {

  static loadFromDirectory(schemaDir: string): ResolvedTableConfig[] {
    const absoluteDir = path.resolve(schemaDir);
    if (!fs.existsSync(absoluteDir)) {
      throw new Error(`Schema directory not found: ${absoluteDir}`);
    }
    const files = fs.readdirSync(absoluteDir).filter((f) => /\.(json|ya?ml)$/i.test(f));
    if (files.length === 0) {
      throw new Error(`No schema files found in: ${absoluteDir}`);
    }
    return files.map((file) => {
      const filePath = path.join(absoluteDir, file);
      const raw      = PSUDLGlueSchemaLoader.parse(filePath);
      PSUDLGlueSchemaLoader.validate(raw, filePath);
      return PSUDLGlueSchemaLoader.resolve(raw);
    });
  }

  static loadOne(filePath: string): ResolvedTableConfig {
    const raw = PSUDLGlueSchemaLoader.parse(filePath);
    PSUDLGlueSchemaLoader.validate(raw, filePath);
    return PSUDLGlueSchemaLoader.resolve(raw);
  }

  private static parse(filePath: string): GlueTableSchema {
    const content = fs.readFileSync(filePath, "utf-8");
    const ext     = path.extname(filePath).toLowerCase();
    try {
      return ext === ".json"
        ? (JSON.parse(content) as GlueTableSchema)
        : (yaml.load(content) as GlueTableSchema);
    } catch (err) {
      throw new Error(`Failed to parse ${filePath}: ${(err as Error).message}`);
    }
  }

  private static validate(schema: GlueTableSchema, filePath: string): void {
    const errors: string[] = [];
    if (!schema.database)    errors.push("'database' is required");
    if (!schema.tableName)   errors.push("'tableName' is required");
    if (!schema.format)      errors.push("'format' is required");
    if (!schema.columns || schema.columns.length === 0) errors.push("'columns' must have at least one entry");
    if (!schema.s3Location?.prefix) errors.push("'s3Location.prefix' is required");
    if (!schema.refresh?.strategy)  errors.push("'refresh.strategy' is required");
    if (
      (schema.refresh?.strategy === "schedule" || schema.refresh?.strategy === "both") &&
      !schema.refresh?.scheduleCron
    ) {
      errors.push("'refresh.scheduleCron' required when strategy is 'schedule' or 'both'");
    }
    const colNames = schema.columns?.map((c) => c.name) ?? [];
    const dupes    = colNames.filter((n, i) => colNames.indexOf(n) !== i);
    if (dupes.length > 0) errors.push(`Duplicate column names: ${dupes.join(", ")}`);
    if (errors.length > 0) {
      throw new Error(`Schema validation failed for ${path.basename(filePath)}:\n  - ${errors.join("\n  - ")}`);
    }
  }

  private static resolve(schema: GlueTableSchema): ResolvedTableConfig {
    const bucket = schema.s3Location.bucketName ?? "<<shared-bucket>>";
    const prefix = schema.s3Location.prefix.replace(/\/?$/, "/");
    return {
      ...schema,
      s3Uri:      `s3://${bucket}/${prefix}`,
      serde:      schema.serdeOverride ?? DEFAULT_SERDES[schema.format],
      s3Location: { ...schema.s3Location, prefix },
    };
  }
}
