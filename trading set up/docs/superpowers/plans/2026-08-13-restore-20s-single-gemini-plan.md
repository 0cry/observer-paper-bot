# Restore 20-Second Single-Gemini Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore synchronized 20-second media windows, use only Gemini key slot 1 without an application analysis deadline, and persist measured Gemini reasoning latency.

**Architecture:** Capture emits four exact 5-second segments and packages them into one 720p MP4. STT produces four aligned chunks. Gemini receives the complete window through one configured credential and may run until provider completion or runtime shutdown; the runtime records elapsed milliseconds on both success and failure.

**Tech Stack:** Rust, Tokio, Reqwest, FFmpeg, ElevenLabs Scribe v2, Gemini Interactions API.

---

### Task 1: Restore the media contract

**Files:** `src/capture.rs`, `src/stt.rs`, `src/gemini.rs`, `src/paper_runtime.rs`

- [ ] Change focused tests to require 5-second segments, four chunks, and a 20-second window.
- [ ] Run focused tests and verify they fail against the 12-second implementation.
- [ ] Update constants, fixed-size arrays, prompt wording, fixtures, and validation messages.
- [ ] Run capture, STT, Gemini, and paper-runtime focused tests.

### Task 2: Remove Gemini application deadlines and select key 1

**Files:** `src/gemini.rs`, `src/config.rs`

- [ ] Add failing tests asserting no default request/window deadline and one configured Gemini credential.
- [ ] Run the tests and verify the old timeout/multi-key behavior fails.
- [ ] Make Gemini deadlines optional with production defaults disabled; retain optional short deadlines for deterministic tests.
- [ ] Load only `GEMINI_API_KEY_1` or the legacy single key, and make the vault default select only slot 1.
- [ ] Run focused Gemini and configuration tests.

### Task 3: Persist reasoning latency

**Files:** `src/paper_runtime.rs`

- [ ] Add a failing serialization test for `latency_ms` on Gemini pipeline audit events.
- [ ] Add an optional latency field and write it for every Gemini success or failure.
- [ ] Run paper-runtime focused tests.

### Task 4: Document and verify

**Files:** `README.md`

- [ ] Replace 12-second/three-chunk/fallback descriptions with the 20-second/four-chunk/single-key contract.
- [ ] Run formatting, the complete test suite, and a release build.
- [ ] Run a finite isolated public-livestream smoke test, inspect media metadata and audit latency, and verify no process remains.

