(() => {
  "use strict";

  const API = Object.freeze({
    state: "/api/state",
    history: "/api/history",
    health: "/api/health",
    events: "/api/events",
    runtimeKeys: "/api/llm/keys",
    runtimeKeysClear: "/api/llm/keys/clear",
    runtimeKeyHealth: "/api/llm/keys/health",
    cronJobs: "/api/cron/jobs",
  });

  const IST = "Asia/Kolkata";
  const els = {};
  const app = {
    view: "live",
    mode: "llm_exit",
    state: null,
    health: null,
    apiOnline: false,
    sseOnline: false,
    lastApiSuccess: 0,
    lastStateAt: 0,
    consecutiveFailures: 0,
    eventSource: null,
    stateAbort: null,
    healthAbort: null,
    stateRefreshTimer: null,
    historyRefreshTimer: null,
    toastTimer: null,
    history: {
      trades: [],
      total: 0,
      page: 1,
      pageSize: 5,
      pages: 1,
      sort: "closed_at",
      order: "desc",
      summary: null,
      abort: null,
      requestId: 0,
      loading: false,
      analyticsTrades: [],
      analyticsPoints: [],
      analyticsAbort: null,
      analyticsRequestId: 0,
      analyticsKey: "",
      analyticsLoading: false,
      analyticsError: false,
      hoverIndex: null,
    },
    cron: { jobs: [], vault: null },
  };

  const INR = new Intl.NumberFormat("en-IN", {
    style: "currency",
    currency: "INR",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
  const NUMBER = new Intl.NumberFormat("en-IN", { maximumFractionDigits: 2 });
  const DATE_TIME = new Intl.DateTimeFormat("en-IN", {
    timeZone: IST,
    day: "2-digit",
    month: "short",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: true,
  });
  const TIME = new Intl.DateTimeFormat("en-IN", {
    timeZone: IST,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: true,
  });
  const DAY = new Intl.DateTimeFormat("en-IN", {
    timeZone: IST,
    weekday: "short",
    day: "2-digit",
    month: "short",
    year: "numeric",
  });

  function cacheElements() {
    document.querySelectorAll("[id]").forEach((node) => { els[node.id] = node; });
  }

  function firstDefined(...values) {
    return values.find((value) => value !== undefined && value !== null);
  }

  function path(object, dotted) {
    if (!object || typeof object !== "object") return undefined;
    return dotted.split(".").reduce((value, key) => value?.[key], object);
  }

  function pick(object, paths, fallback = undefined) {
    for (const candidate of paths) {
      const value = path(object, candidate);
      if (value !== undefined && value !== null) return value;
    }
    return fallback;
  }

  function list(value) {
    if (Array.isArray(value)) return value;
    if (value && typeof value === "object") return Object.values(value);
    return [];
  }

  function number(value, fallback = 0) {
    if (typeof value === "string") value = value.replace(/[₹,%\s]/g, "").replace(/,/g, "");
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : fallback;
  }

  function nullableNumber(value) {
    if (value === null || value === undefined || value === "") return null;
    const parsed = number(value, Number.NaN);
    return Number.isFinite(parsed) ? parsed : null;
  }

  function boolean(value, fallback = false) {
    if (typeof value === "boolean") return value;
    if (typeof value === "number") return value !== 0;
    if (typeof value === "string") {
      if (["true", "yes", "ok", "healthy", "connected", "online", "live", "ready", "fresh", "open"].includes(value.toLowerCase())) return true;
      if (["false", "no", "error", "failed", "offline", "disconnected", "stale", "closed"].includes(value.toLowerCase())) return false;
    }
    return fallback;
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#039;");
  }

  function cleanText(value, fallback = "—") {
    const text = String(value ?? "").trim();
    return text || fallback;
  }

  function unwrap(payload) {
    if (!payload || typeof payload !== "object") return {};
    if (payload.data && typeof payload.data === "object" && !Array.isArray(payload.data)) return { ...payload, ...payload.data };
    return payload;
  }

  function parseTime(value) {
    if (value === null || value === undefined || value === "") return null;
    if (typeof value === "number" || /^\d+$/.test(String(value))) {
      let epoch = Number(value);
      if (epoch < 100_000_000_000) epoch *= 1000;
      const date = new Date(epoch);
      return Number.isNaN(date.getTime()) ? null : date;
    }
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? null : date;
  }

  function formatDateTime(value, fallback = "—") {
    const date = parseTime(value);
    return date ? DATE_TIME.format(date).replace(",", "") : fallback;
  }

  function formatTime(value, fallback = "—") {
    const date = parseTime(value);
    return date ? TIME.format(date) : fallback;
  }

  function formatMoney(value, fallback = "₹—") {
    const parsed = nullableNumber(value);
    if (parsed === null) return fallback;
    return INR.format(parsed).replace("₹", "₹");
  }

  function formatNumber(value, fallback = "—") {
    const parsed = nullableNumber(value);
    return parsed === null ? fallback : NUMBER.format(parsed);
  }

  function formatPercent(value, fallback = "—%") {
    let parsed = nullableNumber(value);
    if (parsed === null) return fallback;
    if (Math.abs(parsed) <= 1 && parsed !== 0) parsed *= 100;
    return `${parsed.toFixed(1)}%`;
  }

  function formatDuration(seconds) {
    const total = Math.max(0, Math.round(number(seconds)));
    if (!total) return "—";
    const hours = Math.floor(total / 3600);
    const minutes = Math.floor((total % 3600) / 60);
    const secs = total % 60;
    if (hours) return `${hours}h ${minutes}m`;
    if (minutes) return `${minutes}m ${secs}s`;
    return `${secs}s`;
  }

  function relativeAge(value) {
    const date = parseTime(value);
    if (!date) return "—";
    const seconds = Math.max(0, Math.floor((Date.now() - date.getTime()) / 1000));
    if (seconds < 2) return "now";
    if (seconds < 60) return `${seconds}s ago`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
    return `${Math.floor(seconds / 3600)}h ago`;
  }

  function pnlClass(value) {
    const amount = number(value);
    return amount > 0 ? "pnl-positive" : amount < 0 ? "pnl-negative" : "pnl-flat";
  }

  function canonicalMode(value) {
    const mode = String(value ?? "").trim().toLowerCase().replace(/[\s-]+/g, "_");
    if (["llm", "ai", "llm_exit", "llmexit"].includes(mode)) return "llm_exit";
    if (["moving", "trail", "trailing", "moving_sl", "movingsl", "rule"].includes(mode)) return "moving_sl";
    return mode || "all";
  }

  function modeLabel(value) {
    const mode = canonicalMode(value);
    if (mode === "llm_exit") return "Approach 1";
    if (mode === "moving_sl") return "Approach 2";
    return cleanText(value, "Shared");
  }

  function modeBadge(value) {
    const mode = canonicalMode(value);
    const className = mode === "llm_exit" ? "llm" : mode === "moving_sl" ? "moving" : "";
    return `<span class="mode-badge ${className}">${escapeHtml(modeLabel(value))}</span>`;
  }

  function visibleForMode(item) {
    if (app.mode === "all") return true;
    const itemMode = canonicalMode(pick(item, ["mode", "exit_mode", "strategy", "portfolio_mode"], "all"));
    return itemMode === "all" || itemMode === app.mode;
  }

  function normalizePoint(point, index) {
    if (Array.isArray(point)) {
      return { time: point[0] ?? index, equity: nullableNumber(point[1]), pnl: nullableNumber(point[2]), mode: "all", accountId: null };
    }
    const explicitPnl = nullableNumber(pick(point, ["pnl", "net_pnl", "cumulative_pnl", "profit_loss"]));
    const realized = nullableNumber(pick(point, ["realized_pnl", "realised_pnl"]));
    const unrealized = nullableNumber(pick(point, ["unrealized_pnl", "unrealised_pnl"]));
    return {
      time: pick(point, ["timestamp", "time", "at", "ts", "recorded_at"], index),
      equity: nullableNumber(pick(point, ["equity", "total_equity", "value", "balance"])),
      pnl: explicitPnl ?? (realized !== null || unrealized !== null ? number(realized) + number(unrealized) : null),
      mode: canonicalMode(pick(point, ["mode", "exit_mode", "strategy"], "all")),
      accountId: pick(point, ["account_id", "accountId", "wallet_id"], null),
    };
  }

  function normalizeState(payload) {
    const root = unwrap(payload);
    const session = pick(root, ["session", "session_info"], {}) || {};
    const metrics = pick(root, ["metrics", "summary", "kpis", "portfolio"], {}) || {};
    const rawCurve = pick(root, ["equity_curve", "performance", "chart", "performance_curve", "series"], []);
    const curvePoints = Array.isArray(rawCurve)
      ? rawCurve
      : list(pick(rawCurve, ["points", "data", "equity"], []));
    return {
      generatedAt: firstDefined(
        pick(root, ["generated_at", "updated_at", "timestamp", "as_of"]),
        pick(session, ["updated_at", "last_update"]),
        Date.now(),
      ),
      session: {
        startedAt: pick(session, ["started_at", "start_time", "started", "created_at"], pick(root, ["session_started_at", "started_at"])),
        marketStatus: pick(session, ["market_status", "status"], pick(root, ["market_status"])),
        tickAt: pick(session, ["last_tick_at", "tick_at", "latest_tick_at"], pick(root, ["last_tick_at", "latest_tick_at"])),
        visualStatus: pick(session, ["visual_status"], "Waiting"),
      },
      metrics,
      accounts: list(pick(root, ["accounts", "wallets", "paper_accounts", "portfolios"], [])),
      positions: list(pick(root, ["positions", "open_positions", "live_positions"], [])),
      pending: list(pick(root, ["pending_orders", "orders.pending", "open_orders", "pending_entries"], [])),
      signals: list(pick(root, ["signals", "recent_signals", "llm_signals"], [])),
      curve: curvePoints.map(normalizePoint).filter((point) => point.equity !== null || point.pnl !== null),
      raw: root,
    };
  }

  function normalizeTrade(item, index = 0) {
    const pnl = nullableNumber(pick(item, ["realized_pnl", "net_pnl", "pnl", "profit_loss", "result"]));
    let status = String(pick(item, ["status", "outcome", "result_status"], "")).toLowerCase();
    if (!status || ["closed", "complete", "completed"].includes(status)) {
      status = pnl > 0 ? "won" : pnl < 0 ? "lost" : "breakeven";
    }
    const openedAt = pick(item, ["opened_at", "entry_at", "entry_time", "created_at"]);
    const closedAt = pick(item, ["closed_at", "exit_at", "exit_time", "updated_at"]);
    let duration = nullableNumber(pick(item, ["duration_seconds", "duration_secs", "duration"]));
    if (duration === null && parseTime(openedAt) && parseTime(closedAt)) duration = (parseTime(closedAt) - parseTime(openedAt)) / 1000;
    const contract = pick(item, ["contract", "symbol", "instrument", "trading_symbol", "option_name"], "Unknown contract");
    return {
      id: String(pick(item, ["id", "trade_id", "position_id", "setup_id"], index)),
      setupId: pick(item, ["setup_id", "signal_id", "setupId"], "—"),
      contract,
      underlying: String(pick(item, ["underlying"], contract)).toUpperCase().includes("SENSEX") ? "SENSEX" : String(pick(item, ["underlying"], contract)).toUpperCase().includes("NIFTY") ? "NIFTY" : cleanText(pick(item, ["underlying"]), "—"),
      account: pick(item, ["account", "account_name", "wallet", "account_id"], "—"),
      mode: canonicalMode(pick(item, ["mode", "exit_mode", "strategy", "portfolio_mode"], "all")),
      quantity: nullableNumber(pick(item, ["quantity", "qty", "units", "filled_quantity"])),
      entryPrice: nullableNumber(pick(item, ["entry_price", "entry", "fill_price", "average_entry"])),
      exitPrice: nullableNumber(pick(item, ["exit_price", "exit", "close_price", "average_exit"])),
      openedAt,
      closedAt,
      exitReason: pick(item, ["exit_reason", "reason", "close_reason"], "—"),
      pnl,
      charges: nullableNumber(pick(item, ["charges", "fees", "total_charges"])),
      status,
      duration,
      sl: nullableNumber(pick(item, ["stop_loss", "sl", "effective_sl"])),
      t1: nullableNumber(pick(item, ["t1", "target_1", "target1"])),
      t2: nullableNumber(pick(item, ["t2", "target_2", "target2"])),
      raw: item,
    };
  }

  function emptyRow(columns, title, detail) {
    return `<tr class="empty-row"><td colspan="${columns}"><span class="empty-state"><i aria-hidden="true">·</i><b>${escapeHtml(title)}</b><span>${escapeHtml(detail)}</span></span></td></tr>`;
  }

  function setPnlText(element, value, formatter = formatMoney) {
    element.textContent = formatter(value);
    element.classList.remove("pnl-positive", "pnl-negative", "pnl-flat");
    element.classList.add(pnlClass(value));
  }

  function modeData() {
    const state = app.state || normalizeState({});
    const accounts = state.accounts.filter(visibleForMode);
    const expectedAccountIds = accounts
      .map((account) => String(pick(account, ["account_id", "accountId", "id"], "")).trim())
      .filter(Boolean);
    return {
      ...state,
      accounts,
      positions: state.positions.filter(visibleForMode),
      pending: state.pending.filter(visibleForMode),
      signals: state.signals.filter(visibleForMode),
      curve: portfolioCurve(state.curve, app.mode, expectedAccountIds),
    };
  }

  function portfolioCurve(points, mode, expectedAccountIds = []) {
    if (mode === "all") {
      const combined = points.filter((point) => point.mode === "all" && !point.accountId);
      if (combined.length) return combined;
      return completeStrategyCurve(points.filter((point) => point.mode !== "all"), "all");
    }
    const exact = points.filter((point) => point.mode === mode);
    const aggregate = exact.filter((point) => !point.accountId);
    return aggregate.length ? aggregate : completeStrategyCurve(exact, mode, expectedAccountIds);
  }

  function completeStrategyCurve(points, mode, expectedAccountIds = []) {
    const expected = new Set(expectedAccountIds);
    const buckets = new Map();
    points.forEach((point, index) => {
      const key = String(point.time ?? index);
      const bucket = buckets.get(key) || { time: point.time ?? index, equity: 0, pnl: 0, equityCount: 0, pnlCount: 0, accountIds: new Set(), mode, accountId: null };
      if (Number.isFinite(point.equity)) { bucket.equity += point.equity; bucket.equityCount += 1; }
      if (Number.isFinite(point.pnl)) { bucket.pnl += point.pnl; bucket.pnlCount += 1; }
      if (point.accountId) bucket.accountIds.add(String(point.accountId));
      buckets.set(key, bucket);
    });
    return [...buckets.values()].filter((bucket) => expected.size === 0 || [...expected].every((accountId) => bucket.accountIds.has(accountId))).map((bucket) => ({
      time: bucket.time,
      equity: bucket.equityCount ? bucket.equity : null,
      pnl: bucket.pnlCount ? bucket.pnl : null,
      mode: bucket.mode,
      accountId: null,
    }));
  }

  function curveDrawdownPct(points) {
    let peak = null;
    let maximum = null;
    points.forEach((point) => {
      if (!Number.isFinite(point.equity)) return;
      peak = peak === null ? point.equity : Math.max(peak, point.equity);
      if (peak > 0) maximum = Math.max(maximum ?? 0, ((peak - point.equity) / peak) * 100);
    });
    return maximum;
  }

  function calculateMetrics(data) {
    const metricsRoot = data.metrics || {};
    const nestedMode = app.mode !== "all" ? pick(metricsRoot, [app.mode, `by_mode.${app.mode}`]) : null;
    const metrics = nestedMode && typeof nestedMode === "object" ? nestedMode : (app.mode === "all" ? metricsRoot : {});
    const accountSum = (keys) => data.accounts.reduce((sum, account) => sum + number(pick(account, keys)), 0);
    const equity = firstDefined(
      nullableNumber(pick(metrics, ["total_equity", "equity", "current_equity"])),
      data.accounts.length ? accountSum(["equity", "current_equity", "balance", "total_value"]) : null,
    ) ?? null;
    const realized = firstDefined(
      nullableNumber(pick(metrics, ["realized_pnl", "realised_pnl", "closed_pnl"])),
      data.accounts.length ? accountSum(["realized_pnl", "realised_pnl", "closed_pnl"]) : null,
    ) ?? null;
    const unrealized = firstDefined(
      nullableNumber(pick(metrics, ["unrealized_pnl", "unrealised_pnl", "open_pnl", "mtm"])),
      data.accounts.length ? accountSum(["unrealized_pnl", "unrealised_pnl", "open_pnl", "mtm"]) : null,
      data.positions.length ? data.positions.reduce((sum, position) => sum + number(pick(position, ["pnl", "unrealized_pnl", "mtm"])), 0) : null,
    ) ?? null;
    const wins = firstDefined(
      nullableNumber(pick(metrics, ["wins", "winning_trades"])),
      data.accounts.length ? accountSum(["wins", "winning_trades"]) : null,
    ) ?? null;
    const closed = firstDefined(
      nullableNumber(pick(metrics, ["closed_trades", "trade_count", "completed_trades"])),
      data.accounts.length ? accountSum(["trades", "closed_trades", "trade_count"]) : null,
    ) ?? null;
    const winRate = firstDefined(nullableNumber(pick(metrics, ["win_rate", "win_rate_pct"])), wins !== null && closed ? (wins / closed) * 100 : null) ?? null;
    return {
      equity,
      realized,
      unrealized,
      openPositions: nullableNumber(pick(metrics, ["open_positions", "position_count"])) ?? data.positions.length,
      winRate,
      wins,
      closed,
      drawdown: firstDefined(curveDrawdownPct(data.curve), nullableNumber(pick(metrics, ["max_drawdown_pct", "max_drawdown", "drawdown_pct"]))) ?? null,
    };
  }

  function renderKpis(data) {
    const metrics = calculateMetrics(data);
    els["kpi-equity"].textContent = formatMoney(metrics.equity);
    setPnlText(els["kpi-realized"], metrics.realized);
    setPnlText(els["kpi-unrealized"], metrics.unrealized);
    els["kpi-positions"].textContent = String(metrics.openPositions ?? "—");
    els["kpi-win-rate"].textContent = formatPercent(metrics.winRate);
    els["kpi-drawdown"].textContent = formatPercent(metrics.drawdown);
    els["kpi-equity-detail"].textContent = `${data.accounts.length} account${data.accounts.length === 1 ? "" : "s"} in view`;
    els["kpi-realized-detail"].textContent = metrics.closed === null ? "Closed trades" : `${metrics.closed} closed trade${metrics.closed === 1 ? "" : "s"}`;
    els["kpi-unrealized-detail"].textContent = data.positions.length ? `${data.positions.length} marked position${data.positions.length === 1 ? "" : "s"}` : "No open exposure";
    els["kpi-positions-detail"].textContent = `${data.pending.length} pending order${data.pending.length === 1 ? "" : "s"}`;
    els["kpi-win-detail"].textContent = metrics.wins === null || metrics.closed === null ? "No closed-trade record" : `${metrics.wins} wins from ${metrics.closed}`;
    els["kpi-drawdown-detail"].textContent = metrics.drawdown === null ? "Awaiting session curve" : "Session peak to trough";
  }

  function accountInitials(name, index) {
    const text = cleanText(name, `A${index + 1}`);
    const words = text.split(/\s+/).filter(Boolean);
    return (words.length > 1 ? words.slice(0, 2).map((word) => word[0]).join("") : text.slice(0, 2)).toUpperCase();
  }

  function renderAccounts(data) {
    els["account-count"].textContent = `${data.accounts.length} account${data.accounts.length === 1 ? "" : "s"}`;
    if (!data.accounts.length) {
      els["accounts-body"].innerHTML = emptyRow(5, "No account snapshots", "Wallet data will appear when the paper manager starts.");
      updateAccountFilter([]);
      return;
    }
    els["accounts-body"].innerHTML = data.accounts.map((account, index) => {
      const name = pick(account, ["name", "account_name", "label", "id"], `Account ${index + 1}`);
      const starting = number(pick(account, ["starting_capital", "initial_capital", "capital"]));
      const equity = nullableNumber(pick(account, ["equity", "current_equity", "balance", "total_value"]));
      const free = nullableNumber(pick(account, ["free_cash", "available_cash", "cash", "available_balance"]));
      const reserved = nullableNumber(pick(account, ["reserved_cash", "used_capital", "margin_used", "invested"]));
      const pnl = firstDefined(nullableNumber(pick(account, ["total_pnl", "pnl", "net_pnl"])), equity !== null && starting ? equity - starting : null);
      const denominator = starting || ((free || 0) + (reserved || 0));
      const usage = denominator ? Math.min(100, Math.max(0, (number(reserved) / denominator) * 100)) : 0;
      return `<tr>
        <td><span class="account-name"><i class="account-avatar">${escapeHtml(accountInitials(name, index))}</i><span><strong>${escapeHtml(name)}</strong><small class="secondary">Start ${formatMoney(starting)}</small></span></span></td>
        <td class="numeric"><strong>${formatMoney(equity)}</strong></td>
        <td class="numeric">${formatMoney(free)}</td>
        <td class="numeric ${pnlClass(pnl)}"><strong>${formatMoney(pnl)}</strong></td>
        <td class="numeric"><span class="usage-bar" title="${usage.toFixed(1)}% capital used"><i style="width:${usage.toFixed(1)}%"></i></span> ${usage.toFixed(0)}%</td>
      </tr>`;
    }).join("");
    updateAccountFilter(data.accounts);
  }

  function renderPositions(data) {
    els["position-count"].textContent = `${data.positions.length} open`;
    if (!data.positions.length) {
      els["positions-body"].innerHTML = emptyRow(10, "No live positions", "Accepted entries will appear here and update tick by tick.");
      return;
    }
    els["positions-body"].innerHTML = data.positions.map((position) => {
      const contract = pick(position, ["contract", "symbol", "instrument", "trading_symbol", "option_name"], "Unknown contract");
      const account = pick(position, ["account", "account_name", "wallet", "account_id"], "—");
      const entry = nullableNumber(pick(position, ["entry_price", "entry", "fill_price", "average_entry"]));
      const ltp = nullableNumber(pick(position, ["ltp", "current_price", "last_price", "mark_price"]));
      const sl = nullableNumber(pick(position, ["effective_sl", "stop_loss", "sl", "trailing_sl"]));
      const t1 = nullableNumber(pick(position, ["t1", "target_1", "target1"]));
      const t2 = nullableNumber(pick(position, ["t2", "target_2", "target2"]));
      const qty = nullableNumber(pick(position, ["quantity", "qty", "filled_quantity", "units"]));
      const pnl = firstDefined(nullableNumber(pick(position, ["pnl", "unrealized_pnl", "mtm"])), entry !== null && ltp !== null && qty !== null ? (ltp - entry) * qty : null);
      const explicitFresh = pick(position, ["tick_fresh", "fresh", "is_fresh"]);
      const tickAge = nullableNumber(pick(position, ["tick_age_ms", "market_tick_age_ms"]));
      const fresh = explicitFresh === undefined
        ? (tickAge !== null ? tickAge <= 5_000 : Boolean(pick(position, ["last_tick_at"])))
        : boolean(explicitFresh, false);
      const phase = cleanText(pick(position, ["phase", "trailing_phase", "risk_phase"]), "Phase 0");
      const opened = pick(position, ["opened_at", "entry_at", "entry_time", "created_at"]);
      return `<tr>
        <td><strong>${escapeHtml(contract)}</strong><span class="secondary">${escapeHtml(account)}</span></td>
        <td>${modeBadge(pick(position, ["mode", "exit_mode", "strategy"]))}</td>
        <td class="numeric">${formatNumber(qty)}</td>
        <td class="numeric">${formatNumber(entry)}</td>
        <td class="numeric"><strong class="ltp-fresh ${fresh ? "" : "ltp-stale"}" title="${fresh ? "Fresh market tick" : "Stale or missing market tick"}">${formatNumber(ltp)}</strong></td>
        <td class="numeric">${formatNumber(sl)}</td>
        <td class="numeric">${formatNumber(t1)} <span class="secondary">${t2 === null ? "No T2" : formatNumber(t2)}</span></td>
        <td><span class="phase-badge">${escapeHtml(phase)}</span></td>
        <td class="numeric ${pnlClass(pnl)}"><strong>${formatMoney(pnl)}</strong></td>
        <td><time title="${escapeHtml(formatDateTime(opened))}">${escapeHtml(relativeAge(opened))}</time></td>
      </tr>`;
    }).join("");
  }

  function renderPending(data) {
    els["pending-count"].textContent = `${data.pending.length} pending`;
    if (!data.pending.length) {
      els["pending-body"].innerHTML = emptyRow(5, "No pending entries", "Buffered limit orders will be shown here.");
      return;
    }
    els["pending-body"].innerHTML = data.pending.map((order) => {
      const contract = pick(order, ["contract", "symbol", "instrument", "trading_symbol"], "Unknown contract");
      const entry = nullableNumber(pick(order, ["entry_cap", "limit_price", "entry_price", "entry"]));
      const buffer = nullableNumber(pick(order, ["buffer", "entry_buffer"]));
      const ltp = nullableNumber(pick(order, ["ltp", "current_price", "last_price"]));
      const accounts = pick(order, ["accounts", "account_names", "account"], []);
      const accountText = Array.isArray(accounts) ? accounts.join(", ") : String(accounts || "—");
      const created = pick(order, ["created_at", "placed_at", "signal_at", "timestamp"]);
      return `<tr>
        <td><strong>${escapeHtml(contract)}</strong><span class="secondary">${escapeHtml(modeLabel(pick(order, ["mode", "strategy"])))}</span></td>
        <td class="numeric"><strong>${formatNumber(entry)}</strong><span class="secondary">${buffer === null ? "" : `+${formatNumber(buffer)} buffer`}</span></td>
        <td class="numeric">${formatNumber(ltp)}</td>
        <td title="${escapeHtml(accountText)}">${escapeHtml(accountText)}</td>
        <td><time title="${escapeHtml(formatDateTime(created))}">${escapeHtml(relativeAge(created))}</time></td>
      </tr>`;
    }).join("");
  }

  function signalAction(value) {
    return String(value ?? "UNKNOWN").trim().toUpperCase().replace(/[\s-]+/g, "_");
  }

  function renderSignals(data) {
    const signals = data.signals.slice(-12).reverse();
    els["signal-count"].textContent = `${data.signals.length} signal${data.signals.length === 1 ? "" : "s"}`;
    if (!signals.length) {
      els["signals-list"].innerHTML = `<li class="empty-row"><span class="empty-state"><i aria-hidden="true">·</i><b>No Luna decisions</b><span>The next analyzed clip will appear here.</span></span></li>`;
      return;
    }
    els["signals-list"].innerHTML = signals.map((signal) => {
      const action = signalAction(pick(signal, ["action", "decision", "type"], "UNKNOWN"));
      const contract = pick(signal, ["contract", "symbol", "instrument", "option_name"], "No contract");
      const accepted = boolean(pick(signal, ["accepted", "is_accepted"]), false);
      const reason = pick(signal, ["decision_reason", "reason", "evidence", "summary", "rationale"], accepted ? "Accepted by the paper runtime." : "Signal observed; no trade action accepted.");
      const timestamp = pick(signal, ["timestamp", "created_at", "signal_at", "at"]);
      const actionClass = action.includes("ENTRY") ? "entry" : action.includes("EXIT") ? "exit" : action.includes("WATCH") ? "watch" : "";
      const shortAction = action.replace("PLACE_", "").replace("UPDATE_", "UPD ").slice(0, 6);
      return `<li class="signal-item">
        <span class="signal-action ${actionClass}" title="${escapeHtml(action)}">${escapeHtml(shortAction)}</span>
        <div class="signal-main">
          <div class="signal-title"><strong>${escapeHtml(contract)}</strong><time title="${escapeHtml(formatDateTime(timestamp))}">${escapeHtml(formatTime(timestamp))}</time></div>
          <p>${escapeHtml(reason)}</p>
          <div class="signal-meta"><span class="status-badge ${accepted ? "accepted" : "rejected"}">${accepted ? "Applied" : "Observed"}</span>${modeBadge(pick(signal, ["mode", "strategy"], "all"))}</div>
        </div>
      </li>`;
    }).join("");
  }

  function renderSession(data) {
    const market = app.apiOnline ? "Online" : "Offline";
    const open = app.apiOnline;
    els["market-pill"].textContent = market;
    els["market-pill"].classList.toggle("is-open", open);
    els["market-pill"].classList.toggle("is-closed", !open && market !== "—");
  }

  function renderLive() {
    const data = modeData();
    renderSession(data);
    renderKpis(data);
    renderAccounts(data);
    renderPositions(data);
    renderPending(data);
    renderSignals(data);
    drawChart(data.curve);
  }

  function marketStatusByClock() {
    const parts = new Intl.DateTimeFormat("en-GB", { timeZone: IST, weekday: "short", hour: "2-digit", minute: "2-digit", hour12: false }).formatToParts(new Date());
    const values = Object.fromEntries(parts.map((part) => [part.type, part.value]));
    const weekday = values.weekday;
    const minutes = number(values.hour) * 60 + number(values.minute);
    return ["Sat", "Sun"].includes(weekday) || minutes < 555 || minutes >= 930 ? "Closed" : "Open";
  }

  function drawChart(points) {
    const canvas = els["performance-chart"];
    const empty = els["chart-empty"];
    if (!canvas) return;
    if (!points.length) {
      empty.classList.add("is-visible");
      const context = canvas.getContext("2d");
      context.clearRect(0, 0, canvas.width, canvas.height);
      els["chart-summary"].textContent = "No session performance data is available.";
      return;
    }
    empty.classList.remove("is-visible");
    const rect = canvas.getBoundingClientRect();
    const dpr = Math.min(window.devicePixelRatio || 1, 2);
    const width = Math.max(280, Math.floor(rect.width));
    const height = Math.max(190, Math.floor(rect.height));
    canvas.width = width * dpr;
    canvas.height = height * dpr;
    const ctx = canvas.getContext("2d");
    ctx.scale(dpr, dpr);
    ctx.clearRect(0, 0, width, height);

    const padding = { top: 15, right: 56, bottom: 27, left: 60 };
    const plotW = width - padding.left - padding.right;
    const plotH = height - padding.top - padding.bottom;
    const styles = getComputedStyle(document.documentElement);
    const grid = styles.getPropertyValue("--border").trim() || "#242c3d";
    const muted = styles.getPropertyValue("--text-muted").trim() || "#727d92";
    const blue = styles.getPropertyValue("--blue").trim() || "#5c8dff";
    const green = styles.getPropertyValue("--green").trim() || "#27d59b";
    const equityValues = points.map((point) => point.equity).filter(Number.isFinite);
    const pnlValues = points.map((point) => point.pnl).filter(Number.isFinite);
    const equityRange = paddedRange(equityValues);
    const pnlRange = paddedRange(pnlValues.length ? [...pnlValues, 0] : [0]);

    ctx.lineWidth = 1;
    ctx.font = "9px Cascadia Code, Consolas, monospace";
    ctx.textBaseline = "middle";
    for (let row = 0; row <= 4; row += 1) {
      const y = padding.top + (plotH * row) / 4;
      ctx.strokeStyle = grid;
      ctx.globalAlpha = .65;
      ctx.beginPath();
      ctx.moveTo(padding.left, y + .5);
      ctx.lineTo(width - padding.right, y + .5);
      ctx.stroke();
      ctx.globalAlpha = 1;
      const equityLabel = equityRange.max - ((equityRange.max - equityRange.min) * row) / 4;
      const pnlLabel = pnlRange.max - ((pnlRange.max - pnlRange.min) * row) / 4;
      ctx.fillStyle = muted;
      ctx.textAlign = "right";
      ctx.fillText(compactMoney(equityLabel), padding.left - 7, y);
      ctx.textAlign = "left";
      ctx.fillText(signedCompact(pnlLabel), width - padding.right + 7, y);
    }

    const indexes = [...new Set([0, Math.floor((points.length - 1) / 2), points.length - 1])];
    indexes.forEach((index) => {
      const x = points.length === 1 ? padding.left : padding.left + (plotW * index) / (points.length - 1);
      ctx.fillStyle = muted;
      ctx.textAlign = index === 0 ? "left" : index === points.length - 1 ? "right" : "center";
      ctx.fillText(chartTime(points[index].time), x, height - 9);
    });

    const xFor = (index) => points.length === 1 ? padding.left + plotW / 2 : padding.left + (plotW * index) / (points.length - 1);
    const yFor = (value, range) => padding.top + ((range.max - value) / (range.max - range.min || 1)) * plotH;

    if (equityValues.length) {
      const gradient = ctx.createLinearGradient(0, padding.top, 0, padding.top + plotH);
      gradient.addColorStop(0, "rgba(92, 141, 255, .18)");
      gradient.addColorStop(1, "rgba(92, 141, 255, 0)");
      ctx.beginPath();
      let started = false;
      points.forEach((point, index) => {
        if (point.equity === null) return;
        const x = xFor(index);
        const y = yFor(point.equity, equityRange);
        if (!started) { ctx.moveTo(x, y); started = true; } else ctx.lineTo(x, y);
      });
      const lastIndex = findValueIndex(points, "equity", false);
      const firstIndex = findValueIndex(points, "equity", true);
      if (lastIndex >= 0 && firstIndex >= 0) {
        ctx.lineTo(xFor(lastIndex), padding.top + plotH);
        ctx.lineTo(xFor(firstIndex), padding.top + plotH);
        ctx.closePath();
        ctx.fillStyle = gradient;
        ctx.fill();
      }
      drawSeries(ctx, points, "equity", equityRange, blue, xFor, yFor);
    }
    if (pnlValues.length) drawSeries(ctx, points, "pnl", pnlRange, green, xFor, yFor);

    const first = points[0];
    const last = points[points.length - 1];
    els["chart-summary"].textContent = `Session performance has ${points.length} snapshots. Latest equity ${formatMoney(last.equity)} and cumulative profit and loss ${formatMoney(last.pnl)}.`;
  }

  function paddedRange(values) {
    if (!values.length) return { min: 0, max: 1 };
    let min = Math.min(...values);
    let max = Math.max(...values);
    let span = max - min;
    if (!span) span = Math.max(Math.abs(max) * .02, 1);
    const padding = span * .14;
    return { min: min - padding, max: max + padding };
  }

  function findValueIndex(points, key, first) {
    if (first) return points.findIndex((point) => point[key] !== null);
    for (let index = points.length - 1; index >= 0; index -= 1) if (points[index][key] !== null) return index;
    return -1;
  }

  function drawSeries(ctx, points, key, range, color, xFor, yFor) {
    ctx.save();
    ctx.strokeStyle = color;
    ctx.lineWidth = 1.75;
    ctx.lineJoin = "round";
    ctx.lineCap = "round";
    ctx.beginPath();
    let started = false;
    points.forEach((point, index) => {
      if (point[key] === null) return;
      const x = xFor(index);
      const y = yFor(point[key], range);
      if (!started) { ctx.moveTo(x, y); started = true; } else ctx.lineTo(x, y);
    });
    ctx.stroke();
    const lastIndex = findValueIndex(points, key, false);
    if (lastIndex >= 0) {
      const x = xFor(lastIndex);
      const y = yFor(points[lastIndex][key], range);
      ctx.fillStyle = color;
      ctx.beginPath();
      ctx.arc(x, y, 3, 0, Math.PI * 2);
      ctx.fill();
      ctx.globalAlpha = .2;
      ctx.beginPath();
      ctx.arc(x, y, 7, 0, Math.PI * 2);
      ctx.fill();
    }
    ctx.restore();
  }

  function compactMoney(value) {
    const abs = Math.abs(value);
    if (abs >= 100_000) return `₹${(value / 100_000).toFixed(1)}L`;
    if (abs >= 1_000) return `₹${(value / 1_000).toFixed(1)}k`;
    return `₹${Math.round(value)}`;
  }

  function signedCompact(value) {
    const sign = value > 0 ? "+" : "";
    const abs = Math.abs(value);
    if (abs >= 1_000) return `${sign}${(value / 1_000).toFixed(1)}k`;
    return `${sign}${Math.round(value)}`;
  }

  function chartTime(value) {
    const parsed = parseTime(value);
    return parsed ? new Intl.DateTimeFormat("en-IN", { timeZone: IST, hour: "2-digit", minute: "2-digit", hour12: true }).format(parsed) : String(value ?? "");
  }

  function updateAccountFilter(accounts) {
    const select = els["history-account"];
    const current = select.value;
    const fromAccounts = accounts.map((account, index) => cleanText(pick(account, ["name", "account_name", "label", "id"]), `Account ${index + 1}`));
    const fromTrades = app.history.trades.map((trade) => cleanText(trade.account, "")).filter(Boolean);
    const options = [...new Set([...fromAccounts, ...fromTrades])].sort((a, b) => a.localeCompare(b));
    select.innerHTML = `<option value="">All accounts</option>${options.map((name) => `<option value="${escapeHtml(name)}">${escapeHtml(name)}</option>`).join("")}`;
    if (options.includes(current)) select.value = current;
  }

  function historyParams({ exportAll = false } = {}) {
    const form = new FormData(els["history-filters"]);
    const params = new URLSearchParams();
    for (const [key, value] of form.entries()) if (String(value).trim()) params.set(key, String(value).trim());
    params.set("sort", app.history.sort);
    params.set("order", app.history.order);
    params.set("page", exportAll ? "1" : String(app.history.page));
    params.set("page_size", exportAll ? "10000" : String(app.history.pageSize));
    return params;
  }

  function historyFilterKey() {
    const form = new FormData(els["history-filters"]);
    return JSON.stringify([...form.entries()].map(([key, value]) => [key, String(value).trim()]));
  }

  async function requestJson(url, { signal, timeout = 6500, method = "GET", body } = {}) {
    const timeoutController = new AbortController();
    const timer = window.setTimeout(() => timeoutController.abort(), timeout);
    const combined = signal && typeof AbortSignal.any === "function" ? AbortSignal.any([signal, timeoutController.signal]) : timeoutController.signal;
    try {
      const headers = { Accept: "application/json" };
      if (body !== undefined) headers["Content-Type"] = "application/json";
      const response = await fetch(url, { method, headers, body: body === undefined ? undefined : JSON.stringify(body), cache: "no-store", signal: combined });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      if (response.status === 204) return null;
      return await response.json();
    } finally {
      window.clearTimeout(timer);
    }
  }

  async function loadCron({ quiet = false } = {}) {
    try {
      const [jobs, vault] = await Promise.all([
        requestJson(API.cronJobs, { timeout: 8500 }),
        requestJson(API.runtimeKeyHealth, { timeout: 6500 }),
      ]);
      app.cron.jobs = list(jobs);
      app.cron.vault = unwrap(vault);
      markApiSuccess();
      renderCron();
    } catch (error) {
      if (!quiet) showToast("Cron or runtime-key storage is unavailable.");
      renderCron();
    }
  }

  function cronPresetExpression(preset) {
    return { "every-5": "*/5 * * * *", "every-10": "*/10 * * * *", hourly: "0 * * * *" }[preset] || "";
  }

  function applyCronPreset(preset) {
    document.querySelectorAll("[data-cron-preset]").forEach((button) => {
      button.classList.toggle("is-active", button.dataset.cronPreset === preset);
    });
    const descriptions = {
      "every-5": "Every 5 minutes · IST",
      "every-10": "Every 10 minutes · IST",
      hourly: "At the start of every hour · IST",
      custom: "Enter a custom five-field cron expression evaluated in IST.",
    };
    els["cron-expression-help"].textContent = descriptions[preset] || descriptions.custom;
    if (preset === "custom") {
      els["cron-expression"].focus();
      return;
    }
    els["cron-expression"].value = cronPresetExpression(preset);
    els["cron-expression"].dispatchEvent(new Event("input", { bubbles: true }));
  }

  function renderCron() {
    const vault = app.cron.vault || {};
    const loaded = number(vault.loaded_slots, 0);
    const state = cleanText(vault.state, "KEYS_REQUIRED").replaceAll("_", " ");
    els["runtime-key-state"].textContent = `${state} · ${loaded}/3 slots`;
    const slots = list(vault.slots);
    els["runtime-key-health"].textContent = slots.length
      ? slots.map((slot) => `Slot ${number(slot.slot)}: ${cleanText(slot.state, "UNKNOWN")}${slot.last_failure ? ` (${cleanText(slot.last_failure)})` : ""}`).join(" · ")
      : "No runtime keys loaded.";
    for (let slotNumber = 1; slotNumber <= 3; slotNumber += 1) {
      const slot = slots.find((item) => number(item.slot) === slotNumber);
      const slotState = cleanText(slot?.state, "Empty").replaceAll("_", " ");
      els[`runtime-slot-${slotNumber}`].textContent = slotState;
      els[`runtime-slot-${slotNumber}`].closest(".runtime-slot")?.classList.toggle("is-ready", /ready|healthy|available/i.test(slotState));
    }
    const jobs = app.cron.jobs || [];
    const activeJobs = jobs.filter((job) => Boolean(job.enabled));
    const failedJobs = activeJobs.filter((job) => Boolean(job.last_error) || number(job.last_http_status) >= 400 || /fail|error/i.test(cleanText(job.last_status, "")));
    const nextRun = activeJobs
      .map((job) => parseTime(job.next_run_at))
      .filter(Boolean)
      .sort((left, right) => left.getTime() - right.getTime())[0];
    els["cron-active-count"].textContent = String(activeJobs.length);
    els["cron-healthy-count"].textContent = String(Math.max(0, activeJobs.length - failedJobs.length));
    els["cron-failed-count"].textContent = String(failedJobs.length);
    els["cron-next-run"].textContent = nextRun ? chartTime(nextRun) : "—";
    if (!jobs.length) {
      els["cron-jobs-body"].innerHTML = emptyRow(6, "No cron jobs", "Create a public HTTPS GET job above. Render URLs are supported.");
      return;
    }
    els["cron-jobs-body"].innerHTML = jobs.map((job) => {
      const enabled = Boolean(job.enabled);
      const last = cleanText(job.last_status, "Never run");
      const detail = [job.last_http_status ? `HTTP ${job.last_http_status}` : "", job.last_duration_ms ? `${job.last_duration_ms}ms` : "", cleanText(job.last_error, "")].filter(Boolean).join(" · ");
      return `<tr>
        <td><strong>${escapeHtml(cleanText(job.label, "Untitled"))}</strong><span class="secondary">${enabled ? "Enabled" : "Disabled"}</span></td>
        <td><code>${escapeHtml(cleanText(job.expression, ""))}</code></td>
        <td><span class="secondary">${escapeHtml(cleanText(job.target_url, ""))}</span></td>
        <td>${escapeHtml(formatDateTime(job.next_run_at))}</td>
        <td><strong>${escapeHtml(last)}</strong><span class="secondary">${escapeHtml(detail)}</span></td>
        <td><button class="quiet-button" type="button" data-cron-toggle="${number(job.id)}" data-cron-enabled="${enabled ? "true" : "false"}">${enabled ? "Disable" : "Enable"}</button><button class="quiet-button" type="button" data-cron-delete="${number(job.id)}">Delete</button></td>
      </tr>`;
    }).join("");
  }

  async function submitRuntimeKeys(event) {
    event.preventDefault();
    const keys = String(els["runtime-key-input"].value || "").split(/\r?\n/).map((value) => value.trim()).filter(Boolean);
    if (!keys.length || keys.length > 3) { showToast("Paste one to three keys."); return; }
    try {
      await requestJson(API.runtimeKeys, { method: "POST", body: { keys } });
      els["runtime-key-input"].value = "";
      showToast("Keys loaded into RAM only.");
      await loadCron({ quiet: true });
    } catch { showToast("Keys were not accepted."); }
  }

  async function clearRuntimeKeys() {
    try {
      await requestJson(API.runtimeKeysClear, { method: "POST", body: {} });
      els["runtime-key-input"].value = "";
      showToast("All runtime key slots cleared.");
      await loadCron({ quiet: true });
    } catch { showToast("Unable to clear runtime keys."); }
  }

  async function submitCronJob(event) {
    event.preventDefault();
    const body = { label: els["cron-label"].value.trim(), expression: els["cron-expression"].value.trim(), target_url: els["cron-target-url"].value.trim() };
    try {
      await requestJson(API.cronJobs, { method: "POST", body, timeout: 8500 });
      event.currentTarget.reset();
      showToast("Cron job created.");
      await loadCron({ quiet: true });
    } catch { showToast("Cron job was rejected. Use five-field cron and a public HTTPS URL."); }
  }

  async function handleCronControls(event) {
    const toggle = event.target.closest("[data-cron-toggle]");
    const remove = event.target.closest("[data-cron-delete]");
    try {
      if (toggle) {
        await requestJson(`${API.cronJobs}/${number(toggle.dataset.cronToggle)}`, { method: "PATCH", body: { enabled: toggle.dataset.cronEnabled !== "true" } });
      } else if (remove) {
        await requestJson(`${API.cronJobs}/${number(remove.dataset.cronDelete)}`, { method: "DELETE" });
      } else return;
      await loadCron({ quiet: true });
    } catch { showToast("Cron job update failed."); }
  }

  async function loadState({ quiet = false } = {}) {
    app.stateAbort?.abort();
    app.stateAbort = new AbortController();
    try {
      const payload = await requestJson(API.state, { signal: app.stateAbort.signal });
      app.state = normalizeState(payload);
      app.lastStateAt = Date.now();
      markApiSuccess();
      renderLive();
      return true;
    } catch (error) {
      if (error.name === "AbortError") return false;
      markApiFailure();
      if (!quiet && !app.state) showToast("State API is unavailable. Waiting to reconnect.");
      renderConnection();
      return false;
    }
  }

  async function loadHealth({ quiet = false } = {}) {
    app.healthAbort?.abort();
    app.healthAbort = new AbortController();
    try {
      const payload = await requestJson(API.health, { signal: app.healthAbort.signal, timeout: 4500 });
      app.health = unwrap(payload);
      markApiSuccess();
      renderHealth();
      return true;
    } catch (error) {
      if (error.name === "AbortError") return false;
      markApiFailure();
      if (!quiet && !app.health) showToast("Health endpoint did not respond.");
      renderHealth();
      return false;
    }
  }

  function markApiSuccess() {
    app.apiOnline = true;
    app.lastApiSuccess = Date.now();
    app.consecutiveFailures = 0;
    renderConnection();
  }

  function markApiFailure() {
    app.consecutiveFailures += 1;
    if (!app.lastApiSuccess || Date.now() - app.lastApiSuccess > 12_000) app.apiOnline = false;
  }

  function componentStatus(name, aliases = []) {
    const health = app.health || {};
    const value = firstDefined(
      pick(health, [`components.${name}`, `services.${name}`, name, `${name}_status`, `${name}_connected`]),
      ...aliases.map((alias) => pick(health, [`components.${alias}`, `services.${alias}`, alias, `${alias}_status`, `${alias}_connected`])),
    );
    if (value && typeof value === "object") {
      const state = firstDefined(value.status, value.state, value.healthy, value.connected, value.ok);
      return statusPresentation(state, value.detail || value.message);
    }
    return statusPresentation(value);
  }

  function statusPresentation(value, detail) {
    if (value === undefined || value === null) return { className: "is-muted", label: "Unknown" };
    const lower = String(value).toLowerCase();
    if (boolean(value, false) || ["running", "active", "success", "synced"].includes(lower)) {
      return { className: "is-good", label: detail || (lower === "true" ? "Online" : cleanText(value, "Online")) };
    }
    if (["starting", "connecting", "degraded", "stale", "waiting", "idle", "warning"].some((word) => lower.includes(word))) return { className: "is-warn", label: detail || cleanText(value, "Waiting") };
    return { className: "is-bad", label: detail || (lower === "false" ? "Offline" : cleanText(value, "Offline")) };
  }

  function setSourceStatus(prefix, status) {
    const dot = els[`${prefix}-dot`];
    const label = els[`${prefix}-label`];
    dot.className = `status-dot ${status.className}`;
    label.textContent = status.label;
    label.title = status.label;
  }

  function renderHealth() {
    setSourceStatus("api", app.apiOnline ? { className: "is-good", label: "Online" } : { className: "is-bad", label: "Offline" });
    setSourceStatus("feed", componentStatus("feed", ["market", "market_feed", "indstocks"]));
    setSourceStatus("stream", componentStatus("stream", ["video", "youtube"]));
    setSourceStatus("stt", componentStatus("stt", ["transcription", "elevenlabs"]));
    setSourceStatus("luna", componentStatus("analysis", ["llm", "ai"]));
    if (els["visual-label"]) {
      els["visual-label"].textContent = cleanText(app.state?.session?.visualStatus, "Waiting");
      els["visual-label"].title = els["visual-label"].textContent;
    }
    renderKeyHealth();
    renderConnection();
  }

  function renderKeyHealth() {
    const host = els["key-health-list"];
    if (!host) return;
    const slots = list(pick(app.health || {}, ["components.api_keys", "api_keys"], []));
    host.replaceChildren();
    if (!slots.length) {
      const empty = document.createElement("li");
      empty.className = "key-health-empty";
      empty.textContent = "Waiting for runtime";
      host.append(empty);
      return;
    }
    slots.forEach((slot) => {
      const status = String(firstDefined(slot.status, slot.state, "UNKNOWN")).toUpperCase();
      const row = document.createElement("li");
      const label = document.createElement("span");
      const dot = document.createElement("i");
      dot.className = `status-dot ${status === "READY" ? "is-good" : status === "COOLDOWN" ? "is-warn" : "is-bad"}`;
      label.append(dot, `${cleanText(slot.provider, "API")} Key ${number(slot.slot, 0)}`);
      const value = document.createElement("b");
      const rpm = nullableNumber(slot.request_remaining);
      const rpmLimit = nullableNumber(slot.request_limit);
      const tpm = nullableNumber(slot.token_remaining);
      const tpmLimit = nullableNumber(slot.token_limit);
      const todayRequests = number(slot.observed_daily_requests, 0);
      const todayTokens = number(slot.observed_daily_total_tokens, 0);
      const retryAfter = nullableNumber(slot.retry_after_ms);
      const rateSummary = slot.provider === "OpenAI / Luna"
        ? `server headers: RPM ${rpm === null ? "—" : formatNumber(rpm)}${rpmLimit === null ? "" : `/${formatNumber(rpmLimit)}`} · TPM ${tpm === null ? "—" : formatNumber(tpm)}${tpmLimit === null ? "" : `/${formatNumber(tpmLimit)}`} · local IST observed Today ${formatNumber(todayRequests)} req / ${formatNumber(todayTokens)} tok${retryAfter === null ? "" : ` · retry ${Math.ceil(retryAfter / 1000)}s`}`
        : "";
      value.textContent = status === "COOLDOWN"
        ? `Retry ${Math.ceil(number(slot.cooldown_remaining_ms, 0) / 1000)}s`
        : rateSummary || status;
      value.title = slot.last_failure
        ? `Last failure: ${cleanText(slot.last_failure)}`
        : (rateSummary || status);
      row.append(label, value);
      host.append(row);
    });
  }

  function renderConnection() {
    const pill = els["connection-pill"];
    pill.classList.remove("is-live", "is-connecting", "is-offline");
    const text = pill.querySelector("span");
    if (app.apiOnline && app.sseOnline) {
      pill.classList.add("is-live");
      text.textContent = "Live sync";
    } else if (app.apiOnline) {
      pill.classList.add("is-connecting");
      text.textContent = "Polling";
    } else if (!app.lastApiSuccess && app.consecutiveFailures < 1) {
      pill.classList.add("is-connecting");
      text.textContent = "Connecting";
    } else {
      pill.classList.add("is-offline");
      text.textContent = "Offline";
    }
    els["offline-banner"].hidden = app.apiOnline || (!app.lastApiSuccess && app.consecutiveFailures < 1);
    setSourceStatus("api", app.apiOnline ? { className: "is-good", label: "Online" } : { className: "is-bad", label: "Offline" });
    if (app.state) renderSession(modeData());
  }

  function connectEvents() {
    if (!("EventSource" in window)) {
      app.sseOnline = false;
      renderConnection();
      return;
    }
    app.eventSource?.close();
    const events = new EventSource(API.events);
    app.eventSource = events;
    events.onopen = () => {
      app.sseOnline = true;
      markApiSuccess();
    };
    events.onerror = () => {
      app.sseOnline = false;
      renderConnection();
    };
    events.onmessage = handleEvent;
    [
      "ready", "resync_required", "state", "snapshot", "snapshot_replaced", "session", "metrics",
      "tick", "position", "position_closed", "order", "pending_order", "pending_order_removed",
      "signal", "equity", "trade", "trade_closed", "account", "health",
    ].forEach((name) => events.addEventListener(name, handleEvent));
  }

  function handleEvent(event) {
    markApiSuccess();
    let payload;
    try { payload = event.data ? JSON.parse(event.data) : null; } catch { payload = null; }
    const eventType = String(firstDefined(payload?.type, payload?.event, event.type, "message")).toLowerCase();
    const embeddedState = firstDefined(payload?.state, payload?.snapshot, eventType === "state" || eventType === "snapshot" ? payload?.data : undefined);
    if (embeddedState && typeof embeddedState === "object") {
      app.state = normalizeState(embeddedState);
      app.lastStateAt = Date.now();
      renderLive();
    } else if (eventType === "health" && payload) {
      app.health = unwrap(payload);
      renderHealth();
    } else {
      scheduleStateRefresh();
    }
    if (["trade", "position_closed", "history", "fill", "exit"].some((type) => eventType.includes(type)) && app.view === "history") scheduleHistoryRefresh();
  }

  function scheduleStateRefresh() {
    if (app.stateRefreshTimer) return;
    app.stateRefreshTimer = window.setTimeout(() => {
      app.stateRefreshTimer = null;
      loadState({ quiet: true });
    }, 120);
  }

  function scheduleHistoryRefresh() {
    if (app.historyRefreshTimer) return;
    app.historyRefreshTimer = window.setTimeout(() => {
      app.historyRefreshTimer = null;
      Promise.allSettled([
        loadHistory({ quiet: true }),
        loadHistoryAnalytics({ quiet: true, force: true }),
      ]);
    }, 250);
  }

  async function loadHistory({ quiet = false } = {}) {
    app.history.abort?.abort();
    app.history.abort = new AbortController();
    const requestId = ++app.history.requestId;
    app.history.loading = true;
    renderHistoryLoading();
    try {
      const payload = await requestJson(`${API.history}?${historyParams()}`, { signal: app.history.abort.signal, timeout: 8500 });
      if (requestId !== app.history.requestId) return;
      const root = Array.isArray(payload) ? { trades: payload } : unwrap(payload);
      const rawTrades = list(pick(root, ["trades", "items", "history", "results", "rows"], []));
      const trades = rawTrades.slice(0, 5).map(normalizeTrade);
      app.history.trades = sortTrades(trades, app.history.sort, app.history.order);
      app.history.total = number(pick(root, ["total", "total_count", "pagination.total"], trades.length));
      app.history.page = Math.max(1, number(pick(root, ["page", "pagination.page"], app.history.page), app.history.page));
      app.history.pageSize = 5;
      app.history.pages = Math.max(1, Math.ceil(app.history.total / 5));
      app.history.summary = pick(root, ["summary", "metrics", "stats"], null);
      markApiSuccess();
      renderHistory();
      updateAccountFilter((app.state || { accounts: [] }).accounts);
    } catch (error) {
      if (error.name === "AbortError") return;
      markApiFailure();
      app.history.trades = [];
      app.history.total = 0;
      app.history.pages = 1;
      renderHistory();
      if (!quiet) showToast("Trade history is unavailable.");
    } finally {
      if (requestId === app.history.requestId) app.history.loading = false;
    }
  }

  async function loadHistoryAnalytics({ quiet = false, force = false } = {}) {
    const key = historyFilterKey();
    if (!force && app.history.analyticsKey === key && !app.history.analyticsError) {
      renderHistoryAnalytics();
      return true;
    }
    app.history.analyticsAbort?.abort();
    app.history.analyticsAbort = new AbortController();
    const requestId = ++app.history.analyticsRequestId;
    app.history.analyticsKey = key;
    app.history.analyticsLoading = true;
    app.history.analyticsError = false;
    renderHistoryAnalytics();
    try {
      const payload = await requestJson(`${API.history}?${historyParams({ exportAll: true })}`, {
        signal: app.history.analyticsAbort.signal,
        timeout: 15000,
      });
      if (requestId !== app.history.analyticsRequestId) return false;
      const root = Array.isArray(payload) ? { trades: payload } : unwrap(payload);
      const trades = list(pick(root, ["trades", "items", "history", "results", "rows"], [])).map(normalizeTrade);
      app.history.analyticsTrades = trades;
      app.history.analyticsPoints = buildHistoryCurve(trades);
      markApiSuccess();
      return true;
    } catch (error) {
      if (error.name === "AbortError") return false;
      if (requestId !== app.history.analyticsRequestId) return false;
      app.history.analyticsTrades = [];
      app.history.analyticsPoints = [];
      app.history.analyticsError = true;
      if (!quiet) showToast("Trade analytics are unavailable.");
      return false;
    } finally {
      if (requestId === app.history.analyticsRequestId) {
        app.history.analyticsLoading = false;
        app.history.hoverIndex = null;
        renderHistoryAnalytics();
      }
    }
  }

  function buildHistoryCurve(trades) {
    let cumulative = 0;
    return [...trades]
      .filter((trade) => trade.closedAt && Number.isFinite(trade.pnl))
      .sort((left, right) => (parseTime(left.closedAt)?.getTime() || 0) - (parseTime(right.closedAt)?.getTime() || 0))
      .map((trade) => ({
        trade,
        time: trade.closedAt,
        pnl: trade.pnl,
        cumulative: (cumulative += trade.pnl),
      }));
  }

  function calculateHistoryAnalytics(trades, points = buildHistoryCurve(trades)) {
    const eligible = points.map((point) => point.trade);
    const net = points.at(-1)?.cumulative ?? 0;
    const wins = eligible.filter((trade) => number(trade.pnl) > 0).length;
    let peak = 0;
    let maxDrawdown = 0;
    points.forEach((point) => {
      peak = Math.max(peak, point.cumulative);
      maxDrawdown = Math.max(maxDrawdown, peak - point.cumulative);
    });
    return {
      net,
      average: eligible.length ? net / eligible.length : 0,
      wins,
      winRate: eligible.length ? (wins / eligible.length) * 100 : 0,
      maxDrawdown,
      closed: eligible.length,
    };
  }

  function renderHistoryAnalytics() {
    const points = app.history.analyticsPoints;
    const metrics = calculateHistoryAnalytics(app.history.analyticsTrades, points);
    const empty = els["history-chart-empty"];
    const tooltip = els["history-chart-tooltip"];
    setPnlText(els["history-net"], metrics.net);
    setPnlText(els["history-average"], metrics.average);
    setPnlText(els["history-drawdown"], metrics.maxDrawdown ? -metrics.maxDrawdown : 0);
    els["history-win-rate"].textContent = formatPercent(metrics.winRate);
    els["history-win-rate"].classList.remove("pnl-positive", "pnl-negative", "pnl-flat");
    els["history-win-rate"].classList.add(metrics.closed ? (metrics.winRate >= 50 ? "pnl-positive" : "pnl-negative") : "pnl-flat");
    els["history-net-detail"].textContent = `${metrics.closed} filtered closed trade${metrics.closed === 1 ? "" : "s"}`;
    els["history-average-detail"].textContent = metrics.closed ? `${formatNumber(metrics.closed)} trades in calculation` : "No closed trades";
    els["history-win-detail"].textContent = `${metrics.wins} win${metrics.wins === 1 ? "" : "s"} from ${metrics.closed} trade${metrics.closed === 1 ? "" : "s"}`;
    els["history-chart-range"].textContent = metrics.closed ? `${metrics.closed} filtered trade${metrics.closed === 1 ? "" : "s"}` : "Filtered trades";
    tooltip.hidden = true;

    if (app.history.analyticsLoading && !points.length) {
      empty.textContent = "Building filtered performance curve…";
      empty.classList.add("is-visible");
    } else if (app.history.analyticsError) {
      empty.textContent = "Analytics are unavailable. The trade ledger remains usable.";
      empty.classList.add("is-visible");
    } else if (!points.length) {
      empty.textContent = "No closed trades match the current filters.";
      empty.classList.add("is-visible");
    } else {
      empty.classList.remove("is-visible");
    }

    els["history-chart-summary"].textContent = points.length
      ? `${metrics.closed} filtered closed trades. Net realized profit and loss ${formatMoney(metrics.net)}. Maximum drawdown ${formatMoney(metrics.maxDrawdown)}. Win rate ${formatPercent(metrics.winRate)}.`
      : "No filtered trade analytics are available.";
    drawHistoryChart(points);
  }

  function drawHistoryChart(points) {
    const canvas = els["history-performance-chart"];
    if (!canvas) return;
    const rect = canvas.getBoundingClientRect();
    if (rect.width < 40 || rect.height < 40) return;
    const dpr = Math.min(window.devicePixelRatio || 1, 2);
    canvas.width = Math.round(rect.width * dpr);
    canvas.height = Math.round(rect.height * dpr);
    const ctx = canvas.getContext("2d");
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, rect.width, rect.height);
    if (!points.length) return;

    const padding = { top: 22, right: 20, bottom: 35, left: 62 };
    const plotWidth = Math.max(1, rect.width - padding.left - padding.right);
    const plotHeight = Math.max(1, rect.height - padding.top - padding.bottom);
    const values = [0, ...points.map((point) => point.cumulative)];
    let min = Math.min(...values);
    let max = Math.max(...values);
    const spread = Math.max(1, max - min, Math.abs(max) * 0.08, Math.abs(min) * 0.08);
    min -= spread * 0.14;
    max += spread * 0.14;
    if (min === max) { min -= 1; max += 1; }
    const xFor = (index) => padding.left + (points.length === 1 ? plotWidth / 2 : (index / (points.length - 1)) * plotWidth);
    const yFor = (value) => padding.top + ((max - value) / (max - min)) * plotHeight;

    ctx.font = '9px "Cascadia Code", Consolas, monospace';
    ctx.textAlign = "right";
    ctx.textBaseline = "middle";
    for (let row = 0; row <= 4; row += 1) {
      const value = max - ((max - min) * row) / 4;
      const y = padding.top + (plotHeight * row) / 4;
      ctx.beginPath();
      ctx.strokeStyle = "rgba(74, 87, 112, .22)";
      ctx.lineWidth = 1;
      ctx.moveTo(padding.left, y);
      ctx.lineTo(rect.width - padding.right, y);
      ctx.stroke();
      ctx.fillStyle = "#778298";
      ctx.fillText(signedCompact(value), padding.left - 9, y);
    }

    if (min <= 0 && max >= 0) {
      const zeroY = yFor(0);
      ctx.beginPath();
      ctx.setLineDash([4, 4]);
      ctx.strokeStyle = "rgba(165, 174, 192, .42)";
      ctx.moveTo(padding.left, zeroY);
      ctx.lineTo(rect.width - padding.right, zeroY);
      ctx.stroke();
      ctx.setLineDash([]);
    }

    const finalValue = points.at(-1).cumulative;
    const color = finalValue >= 0 ? "#27d59b" : "#ff647c";
    const fill = ctx.createLinearGradient(0, padding.top, 0, padding.top + plotHeight);
    fill.addColorStop(0, finalValue >= 0 ? "rgba(39, 213, 155, .28)" : "rgba(255, 100, 124, .24)");
    fill.addColorStop(1, "rgba(8, 11, 18, 0)");

    ctx.beginPath();
    points.forEach((point, index) => {
      const x = xFor(index);
      const y = yFor(point.cumulative);
      if (!index) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    ctx.lineTo(xFor(points.length - 1), padding.top + plotHeight);
    ctx.lineTo(xFor(0), padding.top + plotHeight);
    ctx.closePath();
    ctx.fillStyle = fill;
    ctx.fill();

    ctx.beginPath();
    points.forEach((point, index) => {
      const x = xFor(index);
      const y = yFor(point.cumulative);
      if (!index) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    ctx.strokeStyle = color;
    ctx.lineWidth = 2;
    ctx.lineJoin = "round";
    ctx.lineCap = "round";
    ctx.stroke();

    const labelIndexes = [...new Set([0, Math.floor((points.length - 1) / 2), points.length - 1])];
    ctx.textBaseline = "top";
    labelIndexes.forEach((index) => {
      ctx.textAlign = index === 0 ? "left" : index === points.length - 1 ? "right" : "center";
      ctx.fillStyle = "#778298";
      ctx.fillText(chartTime(points[index].time), xFor(index), rect.height - padding.bottom + 11);
    });

    const hoverIndex = Math.min(points.length - 1, Math.max(0, number(app.history.hoverIndex, -1)));
    if (app.history.hoverIndex !== null && hoverIndex >= 0) {
      const point = points[hoverIndex];
      const x = xFor(hoverIndex);
      const y = yFor(point.cumulative);
      ctx.beginPath();
      ctx.strokeStyle = "rgba(165, 174, 192, .3)";
      ctx.lineWidth = 1;
      ctx.moveTo(x, padding.top);
      ctx.lineTo(x, padding.top + plotHeight);
      ctx.stroke();
      ctx.beginPath();
      ctx.fillStyle = "#0e131e";
      ctx.strokeStyle = color;
      ctx.lineWidth = 2;
      ctx.arc(x, y, 4.5, 0, Math.PI * 2);
      ctx.fill();
      ctx.stroke();
    }
  }

  function renderHistoryChartTooltip(index, clientX, clientY) {
    const points = app.history.analyticsPoints;
    const point = points[index];
    const tooltip = els["history-chart-tooltip"];
    if (!point) { tooltip.hidden = true; return; }
    const wrap = tooltip.parentElement.getBoundingClientRect();
    tooltip.innerHTML = `<strong>${escapeHtml(point.trade.contract)}</strong><span>${escapeHtml(formatDateTime(point.time))}</span><span>Trade <b class="${pnlClass(point.pnl)}">${escapeHtml(formatMoney(point.pnl))}</b></span><span>Cumulative <b class="${pnlClass(point.cumulative)}">${escapeHtml(formatMoney(point.cumulative))}</b></span>`;
    tooltip.hidden = false;
    const width = tooltip.offsetWidth || 180;
    const height = tooltip.offsetHeight || 86;
    tooltip.style.left = `${Math.max(8, Math.min(wrap.width - width - 8, clientX - wrap.left + 13))}px`;
    tooltip.style.top = `${Math.max(8, Math.min(wrap.height - height - 8, clientY - wrap.top - height / 2))}px`;
  }

  function renderHistoryLoading() {
    els["history-body"].innerHTML = emptyRow(12, "Loading trade history", "Fetching the latest paper-trade ledger…");
  }

  function sortTrades(trades, key, order) {
    const properties = {
      closed_at: "closedAt", contract: "contract", account: "account", mode: "mode", quantity: "quantity",
      entry_price: "entryPrice", exit_price: "exitPrice", exit_reason: "exitReason", realized_pnl: "pnl", duration_seconds: "duration",
    };
    const property = properties[key] || "closedAt";
    return [...trades].sort((a, b) => {
      let left = a[property];
      let right = b[property];
      if (property.endsWith("At")) { left = parseTime(left)?.getTime() || 0; right = parseTime(right)?.getTime() || 0; }
      const result = typeof left === "number" || typeof right === "number" ? number(left) - number(right) : String(left ?? "").localeCompare(String(right ?? ""));
      return order === "asc" ? result : -result;
    });
  }

  function renderHistory() {
    const trades = app.history.trades;
    els["history-body"].closest("table")?.classList.toggle("is-empty", !trades.length);
    if (!trades.length) {
      els["history-body"].innerHTML = emptyRow(12, "No matching trades", "Completed paper trades will appear here. Try clearing the filters.");
    } else {
      els["history-body"].innerHTML = trades.map((trade, index) => {
        const status = ["won", "lost", "open"].includes(trade.status) ? trade.status : "";
        return `<tr>
          <td><time title="${escapeHtml(formatDateTime(trade.closedAt))}">${escapeHtml(formatDateTime(trade.closedAt))}</time><span class="secondary">Entered ${escapeHtml(formatDateTime(trade.openedAt))}</span></td>
          <td><strong>${escapeHtml(trade.contract)}</strong><span class="secondary">${escapeHtml(trade.underlying)}</span></td>
          <td>${escapeHtml(trade.account)}</td>
          <td>${modeBadge(trade.mode)}</td>
          <td class="numeric">${formatNumber(trade.quantity)}</td>
          <td class="numeric">${formatNumber(trade.entryPrice)}</td>
          <td class="numeric">${formatNumber(trade.exitPrice)}</td>
          <td>${escapeHtml(trade.exitReason)}</td>
          <td class="numeric ${pnlClass(trade.pnl)}"><strong>${formatMoney(trade.pnl)}</strong><span class="secondary">Fees ${formatMoney(trade.charges, "—")}</span></td>
          <td>${escapeHtml(formatDuration(trade.duration))}</td>
          <td><span class="status-badge ${status}">${escapeHtml(cleanText(trade.status, "Unknown"))}</span></td>
          <td><button class="details-button" type="button" data-trade-index="${index}" aria-label="View details for ${escapeHtml(trade.contract)}">›</button></td>
        </tr>`;
      }).join("");
    }
    renderHistorySummary();
    renderPagination();
    renderSortIndicators();
  }

  function renderHistorySummary() {
    els["history-total"].textContent = String(app.history.total);
    els["history-ledger-count"].textContent = `${app.history.total} result${app.history.total === 1 ? "" : "s"}`;
    renderHistoryAnalytics();
  }

  function renderPagination() {
    const start = app.history.total ? (app.history.page - 1) * app.history.pageSize + 1 : 0;
    const end = Math.min(app.history.total, start + app.history.trades.length - 1);
    els["page-summary"].textContent = app.history.total ? `Showing ${start}–${end} of ${app.history.total}` : "No trades";
    els["page-number"].textContent = `Page ${app.history.page} of ${app.history.pages}`;
    els["page-size"].value = String(app.history.pageSize);
    els["previous-page"].disabled = app.history.page <= 1;
    els["next-page"].disabled = app.history.page >= app.history.pages;
  }

  function renderSortIndicators() {
    document.querySelectorAll("[data-sort]").forEach((button) => {
      const active = button.dataset.sort === app.history.sort;
      button.classList.toggle("is-sorted", active);
      button.querySelector("span").textContent = active ? (app.history.order === "asc" ? "↑" : "↓") : "↕";
      button.closest("th").setAttribute("aria-sort", active ? (app.history.order === "asc" ? "ascending" : "descending") : "none");
    });
  }

  async function exportHistory() {
    const button = els["export-button"];
    button.disabled = true;
    const original = button.textContent;
    button.textContent = "Preparing CSV…";
    let trades = app.history.trades;
    try {
      const payload = await requestJson(`${API.history}?${historyParams({ exportAll: true })}`, { timeout: 15000 });
      const root = Array.isArray(payload) ? { trades: payload } : unwrap(payload);
      const fetched = list(pick(root, ["trades", "items", "history", "results", "rows"], [])).map(normalizeTrade);
      if (fetched.length) trades = fetched;
    } catch {
      showToast("Full export was unavailable; exporting the visible page.");
    }
    if (!trades.length) {
      showToast("There are no trades to export.");
      button.disabled = false;
      button.textContent = original;
      return;
    }
    const rows = [
      ["Trade ID", "Setup ID", "Contract", "Underlying", "Account", "Mode", "Quantity", "Entry price", "Exit price", "Opened at", "Closed at", "Exit reason", "Realized P&L", "Charges", "Duration seconds", "Status"],
      ...trades.map((trade) => [trade.id, trade.setupId, trade.contract, trade.underlying, trade.account, trade.mode, trade.quantity, trade.entryPrice, trade.exitPrice, trade.openedAt, trade.closedAt, trade.exitReason, trade.pnl, trade.charges, trade.duration, trade.status]),
    ];
    const csv = `\ufeff${rows.map((row) => row.map(csvCell).join(",")).join("\r\n")}`;
    const url = URL.createObjectURL(new Blob([csv], { type: "text/csv;charset=utf-8" }));
    const link = document.createElement("a");
    link.href = url;
    link.download = `observer-trades-${new Date().toISOString().slice(0, 10)}.csv`;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
    showToast(`Exported ${trades.length} trade${trades.length === 1 ? "" : "s"}.`);
    button.disabled = false;
    button.textContent = original;
  }

  function csvCell(value) {
    const text = value === null || value === undefined ? "" : String(value);
    return `"${text.replaceAll('"', '""')}"`;
  }

  function showTradeDialog(index) {
    const trade = app.history.trades[index];
    if (!trade) return;
    els["trade-dialog-title"].textContent = trade.contract;
    const details = [
      ["Trade ID", trade.id], ["Setup ID", trade.setupId], ["Account", trade.account],
      ["Strategy", modeLabel(trade.mode)], ["Status", trade.status], ["Quantity", formatNumber(trade.quantity)],
      ["Entry price", formatNumber(trade.entryPrice)], ["Exit price", formatNumber(trade.exitPrice)], ["Net P&L", formatMoney(trade.pnl)],
      ["Charges", formatMoney(trade.charges)], ["Stop loss", formatNumber(trade.sl)], ["T1 / T2", `${formatNumber(trade.t1)} / ${formatNumber(trade.t2)}`],
      ["Opened", formatDateTime(trade.openedAt)], ["Closed", formatDateTime(trade.closedAt)], ["Duration", formatDuration(trade.duration)],
      ["Exit reason", trade.exitReason], ["Underlying", trade.underlying],
    ];
    els["trade-detail-grid"].innerHTML = details.map(([label, value]) => `<div><span>${escapeHtml(label)}</span><strong>${escapeHtml(value)}</strong></div>`).join("");
    if (typeof els["trade-dialog"].showModal === "function") els["trade-dialog"].showModal();
  }

  function showView(name) {
    if (!['live', 'history', 'cron'].includes(name)) return;
    app.view = name;
    window.scrollTo({ top: 0, left: 0, behavior: "auto" });
    document.querySelectorAll("[data-view]").forEach((view) => {
      const active = view.dataset.view === name;
      view.classList.toggle("is-active", active);
      view.hidden = !active;
    });
    document.querySelectorAll("[data-view-target]").forEach((button) => {
      const active = button.dataset.viewTarget === name;
      button.classList.toggle("is-active", active);
      if (active) button.setAttribute("aria-current", "page"); else button.removeAttribute("aria-current");
    });
    els["page-title"].textContent = name === "live" ? "Live paper desk" : name === "history" ? "Trade history" : "Cron jobs";
    closeSidebar();
    if (name === "history") Promise.allSettled([
      loadHistory({ quiet: Boolean(app.history.trades.length) }),
      loadHistoryAnalytics({ quiet: Boolean(app.history.analyticsPoints.length) }),
    ]);
    else if (name === "cron") loadCron({ quiet: Boolean(app.cron.vault) });
    else window.requestAnimationFrame(() => drawChart(modeData().curve));
  }

  function openSidebar() {
    els.sidebar.classList.add("is-open");
    document.body.classList.add("sidebar-open");
    els["menu-toggle"].setAttribute("aria-expanded", "true");
  }

  function closeSidebar() {
    els.sidebar.classList.remove("is-open");
    document.body.classList.remove("sidebar-open");
    els["menu-toggle"].setAttribute("aria-expanded", "false");
  }

  function showToast(message) {
    els.toast.textContent = message;
    els.toast.classList.add("is-visible");
    window.clearTimeout(app.toastTimer);
    app.toastTimer = window.setTimeout(() => els.toast.classList.remove("is-visible"), 3300);
  }

  function refreshAll() {
    els["refresh-button"].disabled = true;
    const tasks = [loadState(), loadHealth({ quiet: true })];
    if (app.view === "history") tasks.push(loadHistory({ quiet: true }), loadHistoryAnalytics({ quiet: true, force: true }));
    else if (app.view === "cron") tasks.push(loadCron({ quiet: true }));
    Promise.allSettled(tasks)
      .finally(() => { els["refresh-button"].disabled = false; });
  }

  function setupEvents() {
    document.querySelectorAll("[data-view-target]").forEach((button) => button.addEventListener("click", () => showView(button.dataset.viewTarget)));
    document.querySelectorAll('input[name="mode"]').forEach((radio) => radio.addEventListener("change", () => {
      app.mode = radio.value;
      renderLive();
    }));
    els["menu-toggle"].addEventListener("click", () => els.sidebar.classList.contains("is-open") ? closeSidebar() : openSidebar());
    document.addEventListener("click", (event) => {
      if (document.body.classList.contains("sidebar-open") && !els.sidebar.contains(event.target) && !els["menu-toggle"].contains(event.target)) closeSidebar();
    });
    document.addEventListener("keydown", (event) => { if (event.key === "Escape") closeSidebar(); });
    els["refresh-button"].addEventListener("click", refreshAll);
    els["retry-button"].addEventListener("click", refreshAll);
    els["export-button"].addEventListener("click", exportHistory);
    els["previous-page"].addEventListener("click", () => { if (app.history.page > 1) { app.history.page -= 1; loadHistory(); } });
    els["next-page"].addEventListener("click", () => { if (app.history.page < app.history.pages) { app.history.page += 1; loadHistory(); } });
    document.querySelectorAll("[data-sort]").forEach((button) => button.addEventListener("click", () => {
      const key = button.dataset.sort;
      if (app.history.sort === key) app.history.order = app.history.order === "asc" ? "desc" : "asc";
      else { app.history.sort = key; app.history.order = "desc"; }
      app.history.page = 1;
      loadHistory();
    }));
    els["history-body"].addEventListener("click", (event) => {
      const button = event.target.closest("[data-trade-index]");
      if (button) showTradeDialog(number(button.dataset.tradeIndex));
    });
    const refreshHistoryFilters = () => {
      app.history.page = 1;
      Promise.allSettled([
        loadHistory({ quiet: true }),
        loadHistoryAnalytics({ quiet: true, force: true }),
      ]);
    };
    const debouncedFilter = debounce(refreshHistoryFilters, 280);
    els["history-search"].addEventListener("input", debouncedFilter);
    els["history-filters"].addEventListener("change", refreshHistoryFilters);
    els["history-filters"].addEventListener("reset", () => window.setTimeout(refreshHistoryFilters, 0));
    els["runtime-key-form"].addEventListener("submit", submitRuntimeKeys);
    els["runtime-key-clear"].addEventListener("click", clearRuntimeKeys);
    document.querySelectorAll("[data-cron-preset]").forEach((button) => button.addEventListener("click", () => applyCronPreset(button.dataset.cronPreset)));
    els["cron-job-form"].addEventListener("submit", submitCronJob);
    els["cron-jobs-body"].addEventListener("click", handleCronControls);
    window.addEventListener("online", refreshAll);
    window.addEventListener("offline", () => { app.apiOnline = false; renderConnection(); });
    window.addEventListener("beforeunload", () => { app.eventSource?.close(); app.stateAbort?.abort(); app.healthAbort?.abort(); app.history.abort?.abort(); app.history.analyticsAbort?.abort(); });
    const chartPointer = (event) => {
      const points = app.history.analyticsPoints;
      if (!points.length) return;
      const rect = els["history-performance-chart"].getBoundingClientRect();
      const left = 62;
      const right = 20;
      const width = Math.max(1, rect.width - left - right);
      const ratio = Math.max(0, Math.min(1, (event.clientX - rect.left - left) / width));
      const index = points.length === 1 ? 0 : Math.round(ratio * (points.length - 1));
      app.history.hoverIndex = index;
      drawHistoryChart(points);
      renderHistoryChartTooltip(index, event.clientX, event.clientY);
    };
    els["history-performance-chart"].addEventListener("pointermove", chartPointer);
    els["history-performance-chart"].addEventListener("pointerdown", chartPointer);
    els["history-performance-chart"].addEventListener("pointerleave", () => {
      app.history.hoverIndex = null;
      els["history-chart-tooltip"].hidden = true;
      drawHistoryChart(app.history.analyticsPoints);
    });
    if ("ResizeObserver" in window) {
      const observer = new ResizeObserver(debounce(() => {
        if (app.view === "live") drawChart(modeData().curve);
        if (app.view === "history") drawHistoryChart(app.history.analyticsPoints);
      }, 80));
      observer.observe(els["performance-chart"].parentElement);
      observer.observe(els["history-performance-chart"].parentElement);
    } else window.addEventListener("resize", debounce(() => {
      if (app.view === "live") drawChart(modeData().curve);
      if (app.view === "history") drawHistoryChart(app.history.analyticsPoints);
    }, 100));
  }

  function debounce(fn, wait) {
    let timer;
    return (...args) => {
      window.clearTimeout(timer);
      timer = window.setTimeout(() => fn(...args), wait);
    };
  }

  function tickClock() {
    const now = new Date();
    els["live-clock"].innerHTML = `${TIME.format(now)} <small>IST</small>`;
    els["session-date"].textContent = DAY.format(now);
    if (app.state) {
      renderSession(modeData());
    }
  }

  function initializeEmptyState() {
    app.state = normalizeState({ generated_at: Date.now() });
    renderLive();
    renderHistory();
    renderHealth();
  }

  async function init() {
    cacheElements();
    initializeEmptyState();
    setupEvents();
    tickClock();
    window.setInterval(tickClock, 1000);
    connectEvents();
    await Promise.allSettled([loadState(), loadHealth({ quiet: true }), loadCron({ quiet: true })]);
    window.setInterval(() => loadState({ quiet: true }), 4000);
    window.setInterval(() => loadHealth({ quiet: true }), 12000);
  }

  document.addEventListener("DOMContentLoaded", init);
})();
