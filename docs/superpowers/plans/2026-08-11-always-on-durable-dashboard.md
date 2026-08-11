# Always-On Durable Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Populate the dashboard with ten durable strategy-wallet rows, history, last-known positions, and P&L before livestream discovery, and preserve that desk through every idle scheduler state.

**Architecture:** Add a read-only idle snapshot builder beside the paper runtime so broker restoration and dashboard conversion reuse production types. Initialize the daemon from that snapshot, then mutate only scheduler session/health fields while idle; the live runtime remains the only broker writer.

**Tech Stack:** Rust 2024, Tokio, Axum dashboard state, Serde, Neon PostgreSQL, existing PaperBroker and DashboardHandle APIs.

---

### Task 1: Build an idle paper-desk snapshot

**Files:**
- Modify: trading set up/src/paper_runtime.rs
- Test: trading set up/src/paper_runtime.rs tests module

- [ ] **Step 1: Write the failing configured-wallet test**

Add a test calling the wished-for builder with five AccountSpec values and no durable state:

~~~rust
#[test]
fn idle_dashboard_without_durable_state_has_ten_strategy_wallets() {
    let accounts = [5_000, 10_000, 2_000, 15_000, 20_000]
        .into_iter()
        .enumerate()
        .map(|(index, rupees)| AccountSpec {
            account_id: format!("account_{}", index + 1),
            display_name: format!("Account {}", index + 1),
            starting_capital_paise: rupees * 100,
        })
        .collect::<Vec<_>>();
    let state = idle_dashboard_from_parts(
        PaperBrokerConfig::default(),
        accounts,
        None,
        20.0,
    )
    .unwrap();
    assert_eq!(state.accounts.len(), 10);
    assert_eq!(state.accounts.iter().filter(|a| a.strategy == "LLM Exit").count(), 5);
    assert_eq!(state.accounts.iter().filter(|a| a.strategy == "Moving SL").count(), 5);
    assert_eq!(state.metrics.starting_capital, 104_000.0);
}
~~~

- [ ] **Step 2: Run RED**

Run from trading set up:

~~~powershell
cargo test paper_runtime::tests::idle_dashboard_without_durable_state_has_ten_strategy_wallets -- --exact
~~~

Expected: compilation fails because idle_dashboard_from_parts does not exist.

- [ ] **Step 3: Implement the shared builder**

Create broker_config and account_specs helpers used by both live and idle paths. Add idle_dashboard_from_parts, which restores a persisted PaperBroker with strict current configuration or creates one with PaperBroker::with_accounts, then calls the existing dashboard_state converter with the durable history and equity curve.

- [ ] **Step 4: Persist equity history backward-compatibly**

Extend DurablePaperState:

~~~rust
#[serde(default)]
equity_curve: Vec<EquityPoint>,
~~~

Pass the active equity curve to every save_neon_runtime call. Existing Neon JSON without this field must deserialize to an empty vector.

- [ ] **Step 5: Add the async daemon preload**

~~~rust
pub async fn load_idle_dashboard_state(
    config: &AppConfig,
    store: Option<&NeonStore>,
) -> Result<DashboardState> {
    let durable = match store {
        Some(store) => store
            .load_runtime_state::<DurablePaperState>("paper-primary")
            .await?,
        None => None,
    };
    idle_dashboard_from_parts(
        broker_config(config)?,
        account_specs(config)?,
        durable,
        config.trading.charge_per_fill_rupees,
    )
}
~~~

- [ ] **Step 6: Test durable and legacy state**

Add tests proving one durable HistoryTrade and one EquityPoint survive idle conversion, and JSON without equity_curve remains readable.

- [ ] **Step 7: Run GREEN**

~~~powershell
cargo test paper_runtime::tests -- --test-threads=1
~~~

Expected: all paper-runtime tests pass.

### Task 2: Preserve desk data through scheduler states

**Files:**
- Modify: trading set up/src/scheduler.rs
- Modify: trading set up/src/paper_runtime.rs shared-dashboard startup
- Test: trading set up/src/scheduler.rs tests module

- [ ] **Step 1: Write the failing preservation test**

Create a DashboardState fixture containing sentinel accounts, positions, pending orders, signals, equity points, history, and logs. Call apply_waiting_status and assert every desk collection remains equal while session.status and health change.

- [ ] **Step 2: Run RED**

~~~powershell
cargo test scheduler::tests::waiting_status_overlay_preserves_durable_desk_data -- --exact
~~~

Expected: compilation fails because apply_waiting_status does not exist.

- [ ] **Step 3: Implement status-only mutation**

~~~rust
fn apply_waiting_status(
    state: &mut DashboardState,
    status: &str,
    message: &str,
    channel_url: &str,
    config: &AppConfig,
    persistence_degraded: bool,
) {
    state.session = waiting_session(status, channel_url);
    state.health = waiting_health(status, message, config);
    if persistence_degraded {
        state.health.persistence = component_health(
            "DEGRADED",
            "durable paper state is unavailable; configured fallback wallets are displayed",
        );
        state.health.overall = "DEGRADED".to_owned();
    }
}
~~~

Add publish_waiting_status, which uses DashboardHandle::update. Replace every scheduler DashboardState replacement with that mutation.

- [ ] **Step 4: Preload before binding the dashboard**

Reuse the startup NeonStore to call load_idle_dashboard_state. On Neon/load failure, build configured fallback wallets with store None, preserve the error as degraded health, and never write the fallback into Neon. Give RuntimeEventLogger a clone of the same store.

- [ ] **Step 5: Preserve desk during live-runtime startup**

When run_with_dashboard receives a shared handle, call DashboardHandle::update to set only session and health to STARTING. Do not replace the shared dashboard with DashboardState::empty.

- [ ] **Step 6: Add degraded and live-start tests**

Prove fallback wallets remain visible with persistence DEGRADED and live STARTING status does not clear desk collections.

- [ ] **Step 7: Run focused GREEN tests**

~~~powershell
cargo test scheduler::tests -- --test-threads=1
cargo test dashboard::tests -- --test-threads=1
~~~

Expected: all focused tests pass.

### Task 3: Verify locally without GitHub or Render mutation

**Files:**
- Check: trading set up/src/paper_runtime.rs
- Check: trading set up/src/scheduler.rs
- Do not mutate: GitHub remote or Render service

- [ ] **Step 1: Format and inspect**

~~~powershell
cargo fmt --all -- --check
git diff --check
git status --short
~~~

Expected: checks pass and only the design, plan, and intended Rust files differ locally.

- [ ] **Step 2: Run the complete suite**

~~~powershell
cargo test --all -- --test-threads=1
~~~

Expected: zero failed tests; the existing production smoke test remains ignored.

- [ ] **Step 3: Build production**

~~~powershell
cargo build --release --locked
~~~

Expected: exit code 0.

- [ ] **Step 4: Run a finite local daemon smoke**

Start the local release daemon with the separate external environment, query /api/state, and assert an idle session contains ten accounts: five LLM Exit and five Moving SL. Stop the local daemon afterward.

- [ ] **Step 5: Report local-only evidence**

Report changed files, test/build results, and local API account evidence. State explicitly that GitHub and Render remain unchanged, then wait for deployment authorization.
