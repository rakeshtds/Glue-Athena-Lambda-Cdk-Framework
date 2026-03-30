// ============================================================
// PSUDLGlueTypes.ts
// TypeScript interfaces for the schema-driven Glue table framework.
// All schema files (.json / .yaml) must conform to GlueTableSchema.
// ============================================================

export type GlueColumnType =
  | "string" | "int" | "bigint" | "double" | "float"
  | "boolean" | "timestamp" | "date" | "binary"
  | `array<${string}>` | `map<${string},${string}>` | `struct<${string}>`;

export interface GlueColumn {
  name:     string;
  type:     GlueColumnType | string;
  comment?: string;
}

export interface PartitionConfig {
  columns:   GlueColumn[];
  s3Pattern: string;
}

export interface S3LocationConfig {
  bucketName?: string;
  prefix:      string;
}

export interface SerdeConfig {
  serializationLibrary: string;
  parameters?:          Record<string, string>;
}

export type TableFormat = "PARQUET" | "CSV" | "JSON" | "ORC" | "AVRO";

export interface RefreshConfig {
  strategy:      "event" | "schedule" | "both";
  scheduleCron?: string;
}

export interface GlueTableSchema {
  database:       string;
  tableName:      string;
  description?:   string;
  format:         TableFormat;
  columns:        GlueColumn[];
  partitions?:    PartitionConfig;
  s3Location:     S3LocationConfig;
  serdeOverride?: SerdeConfig;
  refresh:        RefreshConfig;
  tags?:          Record<string, string>;
}

export interface ResolvedTableConfig extends GlueTableSchema {
  s3Uri: string;
  serde: SerdeConfig;
}
