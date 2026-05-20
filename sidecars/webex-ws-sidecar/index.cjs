#!/usr/bin/env node

const fs = require("node:fs/promises");
const crypto = require("node:crypto");
const net = require("node:net");
const path = require("node:path");
const process = require("node:process");

require("@webex/internal-plugin-device");
require("@webex/plugin-logger");
require("@webex/plugin-rooms");
require("@webex/plugin-people");
require("@webex/plugin-messages");
require("@webex/plugin-attachment-actions");

const WebexCore = require("@webex/webex-core").default;

const token = process.env.WEBEX_BOT_TOKEN;
const socketPath = process.env.WXCD_SOCKET_PATH || "/tmp/wxcd.sock";
const botEmail = (process.env.WEBEX_BOT_EMAIL || "").toLowerCase();
const ingressRetryDelayMs = Number.parseInt(process.env.WXCD_INGRESS_RETRY_DELAY_MS || "1000", 10);
const deferredIngressPersistTimeoutMs = Number.parseInt(
  process.env.WXCD_DEFERRED_INGRESS_PERSIST_TIMEOUT_MS || "30000",
  10
);
const deferredIngressReplayMaxAgeMs = Number.parseInt(
  process.env.WXCD_DEFERRED_INGRESS_REPLAY_MAX_AGE_MS || "86400000",
  10
);
const sidecarDrainStateHeartbeatMs = 30000;
const pluginHome = process.env.WXCD_PLUGIN_HOME || process.env.CBTH_PLUGIN_HOME || "";
const pluginInstanceId = process.env.WXCD_PLUGIN_INSTANCE_ID || "standalone";
const pluginReleaseId = process.env.WXCD_PLUGIN_RELEASE_ID || process.env.CBTH_PLUGIN_RELEASE_ID || "unknown";
const deferredIngressDir = pluginHome ? path.join(pluginHome, "webex-sidecar-deferred-ingress") : "";
const deferredIngressQuarantineDir = pluginHome
  ? path.join(pluginHome, "webex-sidecar-deferred-ingress-quarantine")
  : "";
const sidecarDrainStatePath = pluginHome
  ? path.join(
      pluginHome,
      "webex-sidecar-drain-state",
      `scope-${stableFnv1aHex(`${pluginInstanceId}\n${pluginReleaseId}`)}--${process.pid}.json`
    )
  : "";

if (!token) {
  throw new Error("WEBEX_BOT_TOKEN is required");
}

const webex = new WebexCore({
  credentials: {
    access_token: token,
  },
});

let exitingForRestart = false;
let shuttingDown = false;
let listenersActive = false;
let messagesListenerActive = false;
let attachmentActionsListenerActive = false;
let sidecarInFlightCount = 0;
let sidecarDrainStateWrite = Promise.resolve();
let sidecarDrainStateHeartbeat = null;
let replayDeferredIngressTask = null;
let workerInactiveObservedAt = null;
const SEND_DEFERRED = "deferred";

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function drainStateComponent(value) {
  const normalized = String(value || "unknown").replace(/[^A-Za-z0-9_.-]/g, "_");
  return normalized.slice(0, 128) || "unknown";
}

function stableFnv1aHex(value) {
  let hash = 0xcbf29ce484222325n;
  for (const byte of Buffer.from(String(value), "utf8")) {
    hash ^= BigInt(byte);
    hash = (hash * 0x100000001b3n) & 0xffffffffffffffffn;
  }
  return hash.toString(16).padStart(16, "0");
}

function finitePositiveDurationMs(value, fallback) {
  return Number.isFinite(value) && value > 0 ? value : fallback;
}

async function deferredIngressCountForDrainState() {
  if (!deferredIngressDir) {
    return 0;
  }
  let entries;
  try {
    entries = await fs.readdir(deferredIngressDir);
  } catch (error) {
    if (error?.code === "ENOENT") {
      return 0;
    }
    console.error("failed to read deferred Webex ingress dir for drain state", error);
    return 1;
  }

  let count = 0;
  for (const entry of entries.filter((name) => name.endsWith(".json"))) {
    const recordPath = path.join(deferredIngressDir, entry);
    let record;
    try {
      record = JSON.parse(await fs.readFile(recordPath, "utf8"));
    } catch (error) {
      console.error("failed to parse deferred Webex ingress for drain state", {
        path: recordPath,
        error,
      });
      count += 1;
      continue;
    }
    if (deferredIngressRecordMatchesCurrentScope(record)) {
      count += 1;
    }
  }
  return count;
}

function queueSidecarDrainStateWrite() {
  if (!sidecarDrainStatePath) {
    return Promise.resolve();
  }
  sidecarDrainStateWrite = sidecarDrainStateWrite
    .catch(() => {})
    .then(async () => {
      const deferredIngressCount = await deferredIngressCountForDrainState();
      const snapshot = {
        plugin_instance_id: pluginInstanceId,
        plugin_release_id: pluginReleaseId,
        pid: process.pid,
        in_flight_count: sidecarInFlightCount,
        deferred_ingress_count: deferredIngressCount,
        worker_inactive_observed_at: workerInactiveObservedAt,
        updated_at: new Date().toISOString(),
      };
      await fs.mkdir(path.dirname(sidecarDrainStatePath), { recursive: true });
      const tmpPath = `${sidecarDrainStatePath}.${process.pid}.${Date.now()}.tmp`;
      await fs.writeFile(tmpPath, `${JSON.stringify(snapshot, null, 2)}\n`, "utf8");
      await fs.rename(tmpPath, sidecarDrainStatePath);
    });
  return sidecarDrainStateWrite;
}

function startSidecarDrainStateHeartbeat() {
  if (!sidecarDrainStatePath || sidecarDrainStateHeartbeat) {
    return;
  }
  sidecarDrainStateHeartbeat = setInterval(() => {
    queueSidecarDrainStateWrite().catch((error) => {
      exitForSupervisorRestart("sidecar_drain_state_heartbeat_failed", {
        message: error?.message || String(error),
        code: error?.code,
      });
    });
  }, sidecarDrainStateHeartbeatMs);
  if (typeof sidecarDrainStateHeartbeat.unref === "function") {
    sidecarDrainStateHeartbeat.unref();
  }
}

function stopSidecarDrainStateHeartbeat() {
  if (!sidecarDrainStateHeartbeat) {
    return;
  }
  clearInterval(sidecarDrainStateHeartbeat);
  sidecarDrainStateHeartbeat = null;
}

async function clearSidecarDrainState() {
  if (!sidecarDrainStatePath) {
    return;
  }
  sidecarInFlightCount = 0;
  workerInactiveObservedAt = null;
  try {
    await queueSidecarDrainStateWrite();
  } catch (error) {
    console.error("failed to persist cleared sidecar drain state", error);
  }
  try {
    await fs.rm(sidecarDrainStatePath, { force: true });
  } catch (error) {
    console.error("failed to remove sidecar drain state", error);
  }
}

async function clearWorkerInactiveObservation() {
  if (workerInactiveObservedAt === null) {
    return;
  }
  workerInactiveObservedAt = null;
  await queueSidecarDrainStateWrite();
}

async function recordWorkerInactiveObservation() {
  workerInactiveObservedAt = new Date().toISOString();
  await queueSidecarDrainStateWrite();
}

async function withSidecarDrainTracking(callback) {
  sidecarInFlightCount += 1;
  try {
    await queueSidecarDrainStateWrite();
  } catch (error) {
    sidecarInFlightCount = Math.max(0, sidecarInFlightCount - 1);
    console.error("failed to persist sidecar drain state before processing ingress", error);
    exitForSupervisorRestart("sidecar_drain_state_before_ingress_failed", {
      message: error?.message || String(error),
      code: error?.code,
    });
    throw error;
  }
  try {
    return await callback();
  } finally {
    sidecarInFlightCount = Math.max(0, sidecarInFlightCount - 1);
    try {
      await queueSidecarDrainStateWrite();
    } catch (error) {
      console.error("failed to persist sidecar drain state after processing ingress", error);
      exitForSupervisorRestart("sidecar_drain_state_after_ingress_failed", {
        message: error?.message || String(error),
        code: error?.code,
      });
      throw error;
    }
  }
}

async function fetchJson(url) {
  const response = await fetch(url, {
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
  });
  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Webex API ${response.status}: ${body}`);
  }
  return await response.json();
}

function isRetryableWorkerAck(ack) {
  const detail = String(ack?.detail || "");
  return ack?.healthy === false || isLifecycleBackpressureDetail(detail);
}

function isLifecycleBackpressureDetail(detail) {
  return (
    /quiescing|shutting down|not accepting new Webex work/i.test(detail) ||
    isReplayAckTimeoutDetail(detail)
  );
}

function isReplayAckTimeoutDetail(detail) {
  return /timed out after .*processing async notification/i.test(detail);
}

function deferredIngressPath(envelope) {
  const eventId = String(envelope?.event_id || `${Date.now()}-${Math.random()}`);
  const digest = crypto.createHash("sha256").update(eventId).digest("hex").slice(0, 16);
  const stem = drainStateComponent(eventId).slice(0, 80);
  return path.join(deferredIngressDir, `${stem}--${digest}.json`);
}

async function persistDeferredIngress(envelope, error) {
  if (!deferredIngressDir) {
    throw error;
  }
  await fs.mkdir(deferredIngressDir, { recursive: true });
  const targetPath = deferredIngressPath(envelope);
  const tmpPath = `${targetPath}.${process.pid}.${Date.now()}.tmp`;
  const record = {
    plugin_instance_id: pluginInstanceId,
    plugin_release_id: pluginReleaseId,
    event_id: envelope?.event_id || null,
    deferred_at: new Date().toISOString(),
    reason: error?.message || "worker rejected ingress during lifecycle transition",
    envelope,
  };
  await fs.writeFile(tmpPath, `${JSON.stringify(record, null, 2)}\n`, "utf8");
  await fs.rename(tmpPath, targetPath);
}

async function persistDeferredIngressUntilStored(envelope, error) {
  const startedAt = Date.now();
  const timeoutMs = finitePositiveDurationMs(deferredIngressPersistTimeoutMs, 30000);
  const retryDelayMs = finitePositiveDurationMs(ingressRetryDelayMs, 1000);
  let nextLogAt = 0;
  let attempts = 0;
  let lastPersistError = error;
  while (Date.now() - startedAt < timeoutMs) {
    try {
      await persistDeferredIngress(envelope, error);
      return;
    } catch (persistError) {
      lastPersistError = persistError;
      attempts += 1;
      const now = Date.now();
      const elapsedMs = now - startedAt;
      if (now >= nextLogAt) {
        console.error(
          "failed to persist deferred ingress; retrying before releasing sidecar drain",
          persistError
        );
        nextLogAt = now + 10000;
      }
      const remainingMs = timeoutMs - elapsedMs;
      if (remainingMs <= 0) {
        break;
      }
      await sleep(Math.min(retryDelayMs, remainingMs));
    }
  }
  const elapsedMs = Date.now() - startedAt;
  exitForSupervisorRestart("deferred_ingress_persist_failed", {
    message: lastPersistError?.message || String(lastPersistError),
    code: lastPersistError?.code,
    event_id: envelope?.event_id || null,
    attempts,
    elapsed_ms: elapsedMs,
    timeout_ms: timeoutMs,
  });
  throw lastPersistError;
}

function refreshReplayEnvelope(envelope) {
  if (
    envelope?.kind === "message_created" ||
    envelope?.kind === "attachment_action_created"
  ) {
    return {
      ...envelope,
      sidecar_received_at: new Date().toISOString(),
      processing_ack: true,
    };
  }
  return envelope;
}

function replayRecordSortTime(record) {
  const candidates = [
    record?.envelope?.created,
    record?.envelope?.created_at,
    record?.deferred_at,
  ];
  for (const candidate of candidates) {
    const parsed = Date.parse(candidate);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return 0;
}

function deferredIngressReplayEligibility(record) {
  if (record?.plugin_instance_id !== pluginInstanceId) {
    return { eligible: false, reason: "foreign_instance" };
  }
  const releaseId = record?.plugin_release_id;
  if (!releaseId) {
    return { eligible: false, reason: "missing_plugin_release_id" };
  }
  if (releaseId === pluginReleaseId) {
    return { eligible: true, reason: "current_release" };
  }
  const maxAgeMs = finitePositiveDurationMs(deferredIngressReplayMaxAgeMs, 86400000);
  const timestamp = Date.parse(record?.deferred_at || record?.envelope?.created_at || record?.envelope?.created);
  if (!Number.isFinite(timestamp)) {
    return { eligible: false, reason: "missing_replay_timestamp" };
  }
  const ageMs = Date.now() - timestamp;
  if (ageMs <= maxAgeMs) {
    return { eligible: true, reason: "recent_previous_release", age_ms: ageMs };
  }
  return { eligible: false, reason: "stale_previous_release", age_ms: ageMs, max_age_ms: maxAgeMs };
}

function deferredIngressRecordMatchesCurrentScope(record) {
  return deferredIngressReplayEligibility(record).eligible;
}

function quarantineFileName(recordPath, reason) {
  const safeReason = drainStateComponent(reason).slice(0, 48);
  const safeBaseName = drainStateComponent(path.basename(recordPath)).slice(0, 120);
  return `${Date.now()}--${safeReason}--${safeBaseName}`;
}

async function quarantineDeferredIngressRecord(recordPath, reason, details = {}) {
  if (!deferredIngressQuarantineDir) {
    await fs.rm(recordPath, { force: true });
    return;
  }
  await fs.mkdir(deferredIngressQuarantineDir, { recursive: true });
  const targetPath = path.join(
    deferredIngressQuarantineDir,
    quarantineFileName(recordPath, reason)
  );
  try {
    await fs.rename(recordPath, targetPath);
  } catch (error) {
    if (error?.code !== "EXDEV") {
      throw error;
    }
    await fs.copyFile(recordPath, targetPath);
    await fs.rm(recordPath, { force: true });
  }
  console.error("quarantined deferred Webex ingress record", {
    path: recordPath,
    quarantine_path: targetPath,
    reason,
    ...details,
  });
}

async function quarantineMalformedDeferredIngressRecord(recordPath, error) {
  await quarantineDeferredIngressRecord(recordPath, "malformed_json", {
    message: error?.message || String(error),
  });
}

async function replayDeferredIngress() {
  if (!deferredIngressDir) {
    return;
  }
  if (replayDeferredIngressTask) {
    return replayDeferredIngressTask;
  }
  replayDeferredIngressTask = (async () => {
    let entries;
    try {
      entries = await fs.readdir(deferredIngressDir);
    } catch (error) {
      if (error?.code === "ENOENT") {
        return;
      }
      throw error;
    }

    const records = [];
    for (const entry of entries.filter((name) => name.endsWith(".json")).sort()) {
      const recordPath = path.join(deferredIngressDir, entry);
      let record;
      try {
        record = JSON.parse(await fs.readFile(recordPath, "utf8"));
      } catch (error) {
        await quarantineMalformedDeferredIngressRecord(recordPath, error);
        continue;
      }
      const eligibility = deferredIngressReplayEligibility(record);
      if (!eligibility.eligible) {
        await quarantineDeferredIngressRecord(recordPath, eligibility.reason, {
          plugin_instance_id: record?.plugin_instance_id,
          plugin_release_id: record?.plugin_release_id,
          current_plugin_instance_id: pluginInstanceId,
          current_plugin_release_id: pluginReleaseId,
          age_ms: eligibility.age_ms,
          max_age_ms: eligibility.max_age_ms,
        });
        continue;
      }
      if (!record?.envelope || typeof record.envelope !== "object") {
        await quarantineDeferredIngressRecord(recordPath, "missing_envelope", {
          plugin_instance_id: record?.plugin_instance_id,
          plugin_release_id: record?.plugin_release_id,
        });
        continue;
      }
      records.push({
        entry,
        record,
        recordPath,
        sortTime: replayRecordSortTime(record),
      });
    }
    records.sort((left, right) => {
      const timeOrder = left.sortTime - right.sortTime;
      if (timeOrder !== 0) {
        return timeOrder;
      }
      return left.entry.localeCompare(right.entry);
    });

    for (const { record, recordPath } of records) {
      const replayEnvelope = refreshReplayEnvelope(record.envelope);
      let replayResult;
      try {
        replayResult = await withSidecarDrainTracking(() =>
          sendEnvelope(replayEnvelope, {
            retryUnavailable: true,
            retryLifecycleRejection: true,
            deferOnLifecycleRejection: true,
          })
        );
      } catch (error) {
        if (shuttingDown || exitingForRestart || error?.retryable) {
          throw error;
        }
        await quarantineDeferredIngressRecord(recordPath, "worker_rejected_replay", {
          message: error?.message || String(error),
          ack: error?.ack,
        });
        await queueSidecarDrainStateWrite();
        continue;
      }
      if (replayResult === SEND_DEFERRED) {
        return SEND_DEFERRED;
      }
      await fs.rm(recordPath, { force: true });
      await queueSidecarDrainStateWrite();
    }
  })();
  try {
    return await replayDeferredIngressTask;
  } finally {
    replayDeferredIngressTask = null;
  }
}

function workerAckError(ack, options = {}) {
  const error = new Error(`worker rejected ingress: ${ack?.detail || "negative ack"}`);
  error.lifecycleRejected = isRetryableWorkerAck(ack);
  error.retryable = options.retryLifecycleRejection === true && error.lifecycleRejected;
  error.ack = ack;
  return error;
}

function decodeWorkerAck(line, options = {}) {
  const ack = JSON.parse(line);
  if (ack?.ok === true) {
    return ack;
  }
  if (ack?.ok === false) {
    throw workerAckError(ack, options);
  }
  throw new Error(`invalid worker ingress ack: ${line}`);
}

function markRetryableSocketError(error) {
  if (["ECONNREFUSED", "ECONNRESET", "ENOENT", "EPIPE"].includes(error?.code)) {
    error.retryable = true;
  }
  return error;
}

function retryableSocketCloseError(message) {
  const error = new Error(message);
  error.retryable = true;
  return error;
}

function sendEnvelopeOnce(envelope, options = {}) {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection(socketPath);
    let settled = false;
    let buffer = "";

    function finish(callback, value) {
      if (settled) {
        return;
      }
      settled = true;
      callback(value);
    }

    socket.on("connect", () => {
      socket.write(JSON.stringify(envelope));
      socket.write("\n");
    });
    socket.on("data", (chunk) => {
      buffer += chunk.toString("utf8");
      if (buffer.includes("\n")) {
        const line = buffer.slice(0, buffer.indexOf("\n")).trim();
        try {
          finish(resolve, decodeWorkerAck(line, options));
        } catch (error) {
          finish(reject, error);
        }
        socket.end();
      }
    });
    socket.on("error", (error) => finish(reject, markRetryableSocketError(error)));
    socket.on("end", () => {
      if (!settled) {
        finish(reject, retryableSocketCloseError("worker closed ingress socket before ack"));
      }
    });
  });
}

async function sendEnvelope(envelope, options = {}) {
  let nextLogAt = 0;
  for (;;) {
    try {
      await sendEnvelopeOnce(envelope, options);
      return;
    } catch (error) {
      if (options.deferOnLifecycleRejection === true && error?.lifecycleRejected) {
        await persistDeferredIngressUntilStored(envelope, error);
        await stopWebexListeners();
        return SEND_DEFERRED;
      }
      if (options.retryUnavailable !== true || !error?.retryable || shuttingDown) {
        throw error;
      }
      const now = Date.now();
      if (now >= nextLogAt) {
        console.error("worker ingress temporarily unavailable; retrying", {
          message: error.message,
          code: error.code,
          ack: error.ack,
        });
        nextLogAt = now + 10000;
      }
      await queueSidecarDrainStateWrite();
      await sleep(Number.isFinite(ingressRetryDelayMs) ? ingressRetryDelayMs : 1000);
    }
  }
}

async function waitForActiveWorker() {
  let nextLogAt = 0;
  for (;;) {
    try {
      await sendEnvelopeOnce(
        { kind: "active_check" },
        { retryLifecycleRejection: true }
      );
      await clearWorkerInactiveObservation();
      return;
    } catch (error) {
      if (!error?.retryable || shuttingDown) {
        throw error;
      }
      const now = Date.now();
      if (now >= nextLogAt) {
        console.error("worker is not active yet; delaying Webex listener startup", {
          message: error.message,
          code: error.code,
          ack: error.ack,
        });
        nextLogAt = now + 10000;
      }
      await recordWorkerInactiveObservation();
      await sleep(Number.isFinite(ingressRetryDelayMs) ? ingressRetryDelayMs : 1000);
    }
  }
}

async function startWebexListeners() {
  if (listenersActive) {
    return;
  }
  try {
    await webex.messages.listen();
    messagesListenerActive = true;
    await webex.attachmentActions.listen();
    attachmentActionsListenerActive = true;
    listenersActive = true;
    console.log("webex-ws-sidecar is listening for Webex events");
  } catch (error) {
    try {
      await stopWebexListeners();
    } catch (stopError) {
      console.error("failed to stop Webex listeners after startup failure", stopError);
    }
    throw error;
  }
}

async function stopWebexListeners() {
  const failures = [];

  if (messagesListenerActive) {
    if (typeof webex.messages.stopListening !== "function") {
      failures.push(new Error("messages stopListening is unavailable"));
    } else {
      try {
        await webex.messages.stopListening();
        messagesListenerActive = false;
      } catch (error) {
        failures.push(error);
        console.error("failed to stop messages listener", error);
      }
    }
  }

  if (attachmentActionsListenerActive) {
    if (typeof webex.attachmentActions.stopListening !== "function") {
      failures.push(new Error("attachment actions stopListening is unavailable"));
    } else {
      try {
        await webex.attachmentActions.stopListening();
        attachmentActionsListenerActive = false;
      } catch (error) {
        failures.push(error);
        console.error("failed to stop attachment actions listener", error);
      }
    }
  }

  listenersActive = messagesListenerActive || attachmentActionsListenerActive;
  if (failures.length > 0) {
    throw new Error(
      `failed to stop Webex listeners: ${failures
        .map((error) => error?.message || String(error))
        .join("; ")}`
    );
  }
}

async function monitorWorkerActive() {
  for (;;) {
    if (shuttingDown) {
      return;
    }
    try {
      await sendEnvelopeOnce(
        { kind: "active_check" },
        { retryLifecycleRejection: true }
      );
      await clearWorkerInactiveObservation();
      if ((await replayDeferredIngress()) === SEND_DEFERRED) {
        await stopWebexListeners();
        await recordWorkerInactiveObservation();
        await sleep(Number.isFinite(ingressRetryDelayMs) ? ingressRetryDelayMs : 1000);
        continue;
      }
      await startWebexListeners();
    } catch (error) {
      if (!error?.retryable) {
        throw error;
      }
      if (listenersActive) {
        console.error("worker is no longer active; stopping Webex listeners", {
          message: error.message,
          code: error.code,
          ack: error.ack,
        });
        await stopWebexListeners();
      }
      await recordWorkerInactiveObservation();
    }
    await sleep(Number.isFinite(ingressRetryDelayMs) ? ingressRetryDelayMs : 1000);
  }
}

function ingressEventId(payload) {
  return (
    payload?.id ||
    payload?.data?.id ||
    payload?.data?.messageId ||
    payload?.event ||
    `${Date.now()}-${Math.random().toString(16).slice(2)}`
  );
}

function exitForSupervisorRestart(label, details) {
  if (exitingForRestart) {
    return;
  }
  exitingForRestart = true;
  console.error(`mercury watchdog triggered: ${label}`, details);
  shutdown()
    .catch((error) => {
      console.error("failed to cleanly shutdown mercury watchdog exit", error);
    })
    .finally(() => process.exit(1));
}

function installMercuryWatchdog() {
  const mercury = webex.internal?.mercury;
  if (!mercury || typeof mercury.on !== "function") {
    console.error("mercury watchdog could not attach: mercury plugin unavailable");
    return;
  }

  mercury.on("connection_failed", (reason, context) => {
    exitForSupervisorRestart("connection_failed", {
      message: reason?.message || String(reason),
      code: reason?.code,
      url: context?.url,
      newWSUrl: context?.newWSUrl,
      retries: context?.retries,
    });
  });

  mercury.on("offline.permanent", (event) => {
    exitForSupervisorRestart("offline.permanent", {
      code: event?.code,
      reason: event?.reason,
    });
  });
}

async function forwardMessage(payload) {
  const sidecarReceivedAt = new Date().toISOString();
  await withSidecarDrainTracking(async () => {
    const message = await fetchJson(`https://webexapis.com/v1/messages/${payload.data.id}`);
    const personEmail = (message.personEmail || payload.data.personEmail || "").toLowerCase();
    if (!message.text || personEmail === botEmail) {
      return;
    }
    await sendEnvelope(
      {
        kind: "message_created",
        event_id: ingressEventId(payload),
        room_id: message.roomId,
        message_id: message.id,
        person_email: personEmail,
        text: message.text,
        created: message.created || new Date().toISOString(),
        sidecar_received_at: sidecarReceivedAt,
      },
      {
        retryUnavailable: true,
        retryLifecycleRejection: true,
        deferOnLifecycleRejection: true,
      }
    );
  });
}

async function forwardAttachmentAction(payload) {
  const sidecarReceivedAt = new Date().toISOString();
  await withSidecarDrainTracking(async () => {
    const action = await fetchJson(`https://webexapis.com/v1/attachment/actions/${payload.data.id}`);
    const personEmail = (action.personEmail || payload.data.personEmail || "").toLowerCase();
    if (personEmail === botEmail) {
      return;
    }
    await sendEnvelope(
      {
        kind: "attachment_action_created",
        event_id: ingressEventId(payload),
        room_id: action.roomId,
        attachment_action_id: action.id,
        person_email: personEmail,
        message_id: action.messageId || null,
        inputs: action.inputs || {},
        created: action.created || new Date().toISOString(),
        sidecar_received_at: sidecarReceivedAt,
      },
      {
        retryUnavailable: true,
        retryLifecycleRejection: true,
        deferOnLifecycleRejection: true,
      }
    );
  });
}

async function main() {
  startSidecarDrainStateHeartbeat();
  await queueSidecarDrainStateWrite();
  for (;;) {
    await waitForActiveWorker();
    if ((await replayDeferredIngress()) !== SEND_DEFERRED) {
      break;
    }
  }
  await webex.people.get("me");
  installMercuryWatchdog();

  webex.messages.on("created", async (payload) => {
    try {
      await forwardMessage(payload);
    } catch (error) {
      console.error("failed to forward message event", error);
    }
  });

  webex.attachmentActions.on("created", async (payload) => {
    try {
      await forwardAttachmentAction(payload);
    } catch (error) {
      console.error("failed to forward attachment action", error);
    }
  });

  await startWebexListeners();
  monitorWorkerActive().catch((error) => {
    exitForSupervisorRestart("active_check_failed", {
      message: error?.message || String(error),
      code: error?.code,
    });
  });
}

async function shutdown() {
  shuttingDown = true;
  stopSidecarDrainStateHeartbeat();
  try {
    await stopWebexListeners();
  } catch (error) {
    console.error("failed to stop Webex listeners during shutdown", error);
  }

  try {
    if (webex.internal?.mercury?.connected) {
      await webex.internal.mercury.disconnect();
    }
  } catch (error) {
    console.error("failed to disconnect mercury", error);
  }
  await clearSidecarDrainState();
}

process.on("SIGINT", () => {
  shutdown()
    .catch((error) => {
      console.error(error);
    })
    .finally(() => process.exit(0));
});

process.on("SIGTERM", () => {
  shutdown()
    .catch((error) => {
      console.error(error);
    })
    .finally(() => process.exit(0));
});

process.on("unhandledRejection", (error) => {
  exitForSupervisorRestart("unhandledRejection", {
    message: error?.message || String(error),
    stack: error?.stack,
  });
});

process.on("uncaughtException", (error) => {
  exitForSupervisorRestart("uncaughtException", {
    message: error?.message || String(error),
    stack: error?.stack,
  });
});

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
