import { createHash } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const webRoot = path.resolve(here, "..");
const srcRoot = path.resolve(webRoot, "src");
const outputRoot = path.resolve(webRoot, "..", "static", "assets");

const assets = [
  { name: "app.js", banner: "// Generated from portal/web/src/app.js. DO NOT EDIT.\n" },
  { name: "app.css", banner: "/* Generated from portal/web/src/app.css. DO NOT EDIT. */\n" }
];

async function build() {
  await mkdir(outputRoot, { recursive: true });

  const manifest = {
    generatedBy: "portal/web/scripts/build.mjs",
    assets: {}
  };

  for (const asset of assets) {
    const srcPath = path.join(srcRoot, asset.name);
    const dstPath = path.join(outputRoot, asset.name);
    const source = await readFile(srcPath, "utf8");
    const output = asset.banner + source;
    await writeFile(dstPath, output, "utf8");

    const sha = createHash("sha256").update(output, "utf8").digest("hex");
    manifest.assets[asset.name] = {
      bytes: Buffer.byteLength(output),
      sha256: sha
    };
  }

  const manifestPath = path.join(outputRoot, "manifest.json");
  await writeFile(manifestPath, JSON.stringify(manifest, null, 2) + "\n", "utf8");

  // Human-readable build summary for CI logs.
  console.log("portal assets built:");
  for (const [name, details] of Object.entries(manifest.assets)) {
    console.log(`- ${name}: ${details.bytes} bytes sha256=${details.sha256}`);
  }
}

build().catch((err) => {
  console.error("portal asset build failed:", err);
  process.exit(1);
});
