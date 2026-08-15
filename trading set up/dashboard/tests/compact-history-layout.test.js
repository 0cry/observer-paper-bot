const fs = require("node:fs");
const assert = require("node:assert/strict");
const path = require("node:path");

const root = path.resolve(__dirname, "..");
const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
const css = fs.readFileSync(path.join(root, "styles.css"), "utf8");
const app = fs.readFileSync(path.join(root, "app.js"), "utf8");

assert.match(html, /<h2 id="history-heading">Performance curve<\/h2>/, "uses the approved compact heading");
assert.match(html, /class="history-filters compact-history-filters"/, "uses compact filter layout");
assert.equal((html.match(/class="compact-filter"/g) || []).length, 4, "shows exactly four compact filters");
for (const id of ["history-account", "history-underlying", "history-to", "clear-filters"]) {
  assert.match(html, new RegExp(`(?:id="${id}"[^>]*hidden|hidden[^>]*id="${id}")`), `${id} remains available but hidden`);
}
assert.doesNotMatch(html, /history-ledger-header/, "removes the oversized ledger header");
assert.match(css, /#history-view \.history-table (?:th|th,)[^{]*:nth-child\(1\)/, "scopes compact ledger columns to Trade History");
assert.match(css, /#history-view \.history-metric-card small\s*\{[^}]*display:\s*none/, "hides verbose metric descriptions");
assert.match(css, /#history-view \.history-table\s*\{[^}]*min-width:\s*0/, "removes the old wide-ledger minimum width");
assert.match(html, />Max DD</, "uses the compact drawdown label");
assert.match(html, />Avg trade</, "uses the compact average label");
assert.match(html, /<input type="hidden" id="page-size" value="5">/, "keeps the ledger fixed at five rows");
assert.doesNotMatch(html, /<label>Rows\b/, "removes the row-count selector");
assert.match(app, /pageSize:\s*5,/, "defaults history pagination to five rows");
assert.match(app, /rawTrades\.slice\(0,\s*5\)\.map\(normalizeTrade\)/, "caps rendered API rows at five");
assert.match(app, /Math\.ceil\(app\.history\.total\s*\/\s*5\)/, "calculates pages in fixed groups of five");
assert.doesNotMatch(app, /\["page-size"\]\.addEventListener\("change"/, "does not expose mutable row sizing");

console.log("compact-history-layout.test.js PASS");
