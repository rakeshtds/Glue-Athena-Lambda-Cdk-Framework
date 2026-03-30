// ============================================================
// PSUDLGlueSchemaValidator.ts
// CLI tool — validates all schema files before cdk deploy.
//
// Usage:
//   npm run validate-schemas
//   npm run validate-schemas:skip         (skip for local test)
//   SKIP_VALIDATION=true npx cdk deploy   (skip via env var)
// ============================================================

import * as path from "path";
import * as fs   from "fs";
import { PSUDLGlueSchemaLoader } from "./PSUDLGlueSchemaLoader";

const green  = (s: string) => `\x1b[32m${s}\x1b[0m`;
const red    = (s: string) => `\x1b[31m${s}\x1b[0m`;
const yellow = (s: string) => `\x1b[33m${s}\x1b[0m`;
const bold   = (s: string) => `\x1b[1m${s}\x1b[0m`;
const dim    = (s: string) => `\x1b[2m${s}\x1b[0m`;

function main() {
  const args = process.argv.slice(2);

  // Skip flag — for local testing
  if (args.includes("--skip-validation") || process.env.SKIP_VALIDATION === "true") {
    console.log(yellow("\n  [PSUDLGlueSchemaValidator] Validation skipped.\n"));
    process.exit(0);
  }

  // Single file mode
  if (args.length > 0 && fs.existsSync(args[0])) {
    console.log(bold("\n PSUDLGlueSchemaValidator"));
    console.log(dim(`  File: ${args[0]}\n`));
    const result = validateFile(args[0]);
    console.log();
    process.exit(result.ok ? 0 : 1);
  }

  validateDirectory();
}

function validateDirectory() {
  const schemaDir = path.resolve(__dirname, "../../schemas");
  console.log(bold("\n PSUDLGlueSchemaValidator"));
  console.log(dim(`  Directory: ${schemaDir}\n`));

  if (!fs.existsSync(schemaDir)) {
    console.error(red(`  ERROR: schemas/ not found at ${schemaDir}`));
    process.exit(1);
  }

  const files = fs.readdirSync(schemaDir).filter((f) => /\.(json|ya?ml)$/i.test(f));

  if (files.length === 0) {
    console.warn(yellow("  WARNING: No schema files found."));
    process.exit(0);
  }

  let passed = 0;
  let failed = 0;

  for (const file of files) {
    const result = validateFile(path.join(schemaDir, file));
    result.ok ? passed++ : failed++;
  }

  console.log("\n" + "─".repeat(54));
  console.log(bold(`  Results: ${files.length} schema(s) checked`));
  if (passed > 0) console.log(green(`  ${passed} passed`));
  if (failed > 0) console.log(red(`  ${failed} failed`));
  console.log("─".repeat(54) + "\n");

  if (failed > 0) {
    console.error(red("  Fix errors above before running cdk deploy.\n"));
    process.exit(1);
  }

  console.log(green("  All schemas valid. Safe to run: npx cdk deploy\n"));
  process.exit(0);
}

function validateFile(filePath: string): { ok: boolean } {
  const fileName = path.basename(filePath);
  try {
    const resolved  = PSUDLGlueSchemaLoader.loadOne(filePath);
    const warnings: string[] = [];

    if (!resolved.description)
      warnings.push("no 'description' — consider adding for the Glue catalog");
    if (!resolved.tags || Object.keys(resolved.tags).length === 0)
      warnings.push("no 'tags' — recommended for cost tracking");
    if (
      (resolved.refresh.strategy === "schedule" || resolved.refresh.strategy === "both") &&
      resolved.refresh.scheduleCron
    ) {
      const fields = resolved.refresh.scheduleCron.trim().split(/\s+/);
      if (fields.length !== 6)
        warnings.push(`scheduleCron has ${fields.length} fields — EventBridge needs exactly 6`);
    }

    console.log(green(`  PASS`) + `  ${fileName}`);
    console.log(dim(`        → ${resolved.database}.${resolved.tableName}`));
    console.log(dim(`        → format: ${resolved.format}  |  columns: ${resolved.columns.length}  |  partitions: ${resolved.partitions?.columns.length ?? 0}  |  refresh: ${resolved.refresh.strategy}`));
    console.log(dim(`        → s3: ${resolved.s3Uri}`));
    warnings.forEach((w) => console.log(yellow(`  WARN    ${w}`)));
    console.log();
    return { ok: true };

  } catch (err) {
    console.log(red(`  FAIL`) + `  ${fileName}`);
    (err as Error).message.split("\n").forEach((l) => console.log(red(`        ${l}`)));
    console.log();
    return { ok: false };
  }
}

main();
