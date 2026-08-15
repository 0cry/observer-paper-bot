const fs = require("node:fs");
const assert = require("node:assert/strict");
const path = require("node:path");

const root = path.resolve(__dirname, "..");
const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
const css = fs.readFileSync(path.join(root, "styles.css"), "utf8");
const app = fs.readFileSync(path.join(root, "app.js"), "utf8");

const positionsHeader = html.match(/<table class="data-table positions-table">[\s\S]*?<\/thead>/)?.[0] || "";
assert.equal((positionsHeader.match(/<th\b/g) || []).length, 7, "keeps the live-position table to seven columns");
for (const heading of ["T1 / T2", "Phase", "Opened"]) {
  assert.doesNotMatch(positionsHeader, new RegExp(heading), `removes ${heading} from live positions`);
}
assert.match(app, /emptyRow\(7, "No live positions"/, "uses the matching empty-state colspan");
assert.doesNotMatch(app, /const phase = cleanText\(pick\(position/, "does not render a phase column");
assert.match(css, /\.positions-panel \.table-scroll\s*\{\s*overflow-x:\s*hidden;/, "hides the live-position horizontal scrollbar");

console.log("positions-layout.test.js PASS");
