const fs = require("node:fs");
const assert = require("node:assert/strict");
const path = require("node:path");

const root = path.resolve(__dirname, "..");
const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
const css = fs.readFileSync(path.join(root, "styles.css"), "utf8");
const app = fs.readFileSync(path.join(root, "app.js"), "utf8");
const live = html.match(/<section class="view is-active" id="live-view"[\s\S]*?<section class="view" id="history-view"/)?.[0] || "";
const cron = html.match(/<section class="view" id="cron-view"[\s\S]*?<\/section>\s*<\/main>/)?.[0] || "";

assert.equal((live.match(/class="kpi-card/g) || []).length, 4, "Live Desk uses four compact KPI cards");
assert.match(live, /class="dashboard-grid live-command-grid"/, "Live Desk has the L1 primary grid");
assert.match(live, /live-command-grid[\s\S]*positions-panel[\s\S]*chart-panel/, "positions precede the curve in L1");
assert.match(live, /class="dashboard-grid live-secondary-grid"/, "Live Desk has the L1 secondary grid");
assert.match(live, /live-secondary-grid[\s\S]*accounts-panel[\s\S]*signals-panel/, "accounts and signals share the secondary row");
for (const id of ["kpi-win-rate", "kpi-win-detail", "kpi-drawdown", "kpi-drawdown-detail"]) {
  assert.match(live, new RegExp(`id="${id}"`), `${id} remains available for compatibility`);
}

assert.equal((cron.match(/class="runtime-slot"/g) || []).length, 4, "Cron shows three OpenAI slots and one YouTube slot");
assert.match(cron, /id="youtube-key-form"/, "Cron exposes the one-way YouTube discovery key form");
assert.equal((cron.match(/class="cron-health-card"/g) || []).length, 4, "Cron shows four health metrics");
for (const preset of ["every-5", "every-10", "hourly", "custom"]) {
  assert.match(cron, new RegExp(`data-cron-preset="${preset}"`), `Cron includes ${preset} preset`);
}
assert.match(cron, /Bounded GET only\. Response bodies are discarded\./, "Cron shows the bounded GET safety contract");
assert.match(app, /function cronPresetExpression\(preset\)/, "Cron preset mapping exists");
assert.match(app, /function applyCronPreset\(preset\)/, "Cron preset interaction exists");
assert.match(app, /querySelectorAll\("\[data-cron-preset\]"\)/, "Cron preset buttons are bound");
assert.match(app, /function loadYouTubeVaultHealth/, "YouTube key health has an independent safe refresh");
assert.match(app, /setInterval\(\(\) => loadYouTubeVaultHealth\(\{ quiet: true \}\), 15_000\)/, "YouTube key health refreshes periodically on every view");
assert.match(app, /youtubeReady = youtubeLoaded === 1 && \/\^ready\$\/i/, "Only provider-verified READY is green");
assert.match(app, /youtubePending = youtubeLoaded === 1 && \/\^loaded\$\/i/, "Loaded but unverified YouTube keys are pending");
const loadCronStart = app.indexOf("async function loadCron");
const loadYouTubeStart = app.indexOf("async function loadYouTubeVaultHealth");
const nextFunctionStart = app.indexOf("function cronPresetExpression", loadYouTubeStart);
const loadCronBody = app.slice(loadCronStart, loadYouTubeStart);
const loadYouTubeBody = app.slice(loadYouTubeStart, nextFunctionStart);
assert.doesNotMatch(loadCronBody, /youtubeKeyHealth|youtubeVault/, "Cron or Neon failure cannot suppress YouTube key health");
assert.match(loadYouTubeBody, /API\.youtubeKeyHealth/, "The independent loader fetches safe YouTube key health");
assert.match(css, /\.runtime-slot\.is-pending/, "Loaded YouTube keys have an amber pending state");
assert.match(app, /pending = \[[^\]]*"not_yet_confirmed"/, "Indeterminate discovery is rendered as a warning, not a failure");
for (const healthyDiscoveryState of ["live_found", "fallback_live_found", "direct_stream_url"]) {
  assert.match(app, new RegExp(`statusPresentation[\\s\\S]*${healthyDiscoveryState}`), `${healthyDiscoveryState} is rendered as healthy`);
}
assert.match(css, /#live-view \.live-command-grid/, "L1 styling is scoped to Live Desk");
assert.match(css, /#cron-view \.cron-key-panel/, "K1 styling is scoped to Cron Jobs");

console.log("live-cron-layout.test.js PASS");
