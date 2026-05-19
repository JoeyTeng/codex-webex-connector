#!/usr/bin/env node

const fs = require("node:fs");
const Module = require("node:module");
const path = require("node:path");

const root = __dirname;
const entrypoint = path.join(root, "index.cjs");
const source = fs.readFileSync(entrypoint, "utf8");
const packageJson = JSON.parse(fs.readFileSync(path.join(root, "package.json"), "utf8"));
const entrypointRequire = Module.createRequire(entrypoint);
const declaredDependencies = new Set(Object.keys(packageJson.dependencies || {}));
const runtimeSpecifiers = new Set();
const requirePattern = /\brequire\(\s*["']([^"']+)["']\s*\)/g;

for (const match of source.matchAll(requirePattern)) {
  const specifier = match[1];
  if (specifier.startsWith("node:") || specifier.startsWith(".") || path.isAbsolute(specifier)) {
    continue;
  }
  runtimeSpecifiers.add(specifier);
}

if (runtimeSpecifiers.size === 0) {
  throw new Error(`no runtime package imports found in ${path.basename(entrypoint)}`);
}

for (const specifier of runtimeSpecifiers) {
  entrypointRequire(specifier);
  const packageName = packageNameForSpecifier(specifier);
  if (!declaredDependencies.has(packageName)) {
    throw new Error(`${specifier} is required by index.cjs but ${packageName} is not declared`);
  }
}

const WebexCore = entrypointRequire("@webex/webex-core").default;
if (typeof WebexCore !== "function") {
  throw new Error("@webex/webex-core default export is not a constructor");
}

function packageNameForSpecifier(specifier) {
  const parts = specifier.split("/");
  if (specifier.startsWith("@")) {
    return parts.slice(0, 2).join("/");
  }
  return parts[0];
}
