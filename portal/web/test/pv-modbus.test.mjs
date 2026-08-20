import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

test("raw Modbus browser call uses the fixed Portal route", async () => {
  const here = path.dirname(fileURLToPath(import.meta.url));
  const source = await readFile(path.resolve(here, "../src/app.js"), "utf8");
  assert.match(source, /api\/v1\/explorer\/modbus\/raw-read/);
  assert.match(source, /api\/v1\/semantic\/pv\/current/);
  assert.match(source, /setInterval\(\(\) => this\.refreshPV\(\), 5000\)/);
  assert.doesNotMatch(source, /portal-pv-m2m-url|m2m-graphql-listen/);
});
