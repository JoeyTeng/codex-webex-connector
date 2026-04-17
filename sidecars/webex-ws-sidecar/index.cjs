#!/usr/bin/env node

const net = require("node:net");
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

if (!token) {
  throw new Error("WEBEX_BOT_TOKEN is required");
}

const webex = new WebexCore({
  credentials: {
    access_token: token,
  },
});

let exitingForRestart = false;

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

function sendEnvelope(envelope) {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection(socketPath);
    let buffer = "";
    socket.on("connect", () => {
      socket.write(JSON.stringify(envelope));
      socket.write("\n");
    });
    socket.on("data", (chunk) => {
      buffer += chunk.toString("utf8");
      if (buffer.includes("\n")) {
        resolve();
        socket.end();
      }
    });
    socket.on("error", reject);
    socket.on("end", resolve);
  });
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
  const message = await fetchJson(`https://webexapis.com/v1/messages/${payload.data.id}`);
  const personEmail = (message.personEmail || payload.data.personEmail || "").toLowerCase();
  if (!message.text || personEmail === botEmail) {
    return;
  }
  await sendEnvelope({
    kind: "message_created",
    event_id: payload.event || payload.id || payload.data.id,
    room_id: message.roomId,
    message_id: message.id,
    person_email: personEmail,
    text: message.text,
    created: message.created || new Date().toISOString(),
  });
}

async function forwardAttachmentAction(payload) {
  const action = await fetchJson(`https://webexapis.com/v1/attachment/actions/${payload.data.id}`);
  const personEmail = (action.personEmail || payload.data.personEmail || "").toLowerCase();
  if (personEmail === botEmail) {
    return;
  }
  await sendEnvelope({
    kind: "attachment_action_created",
    event_id: payload.event || payload.id || payload.data.id,
    room_id: action.roomId,
    attachment_action_id: action.id,
    person_email: personEmail,
    message_id: action.messageId || null,
    inputs: action.inputs || {},
    created: action.created || new Date().toISOString(),
  });
}

async function main() {
  await webex.people.get("me");
  installMercuryWatchdog();
  await webex.messages.listen();
  await webex.attachmentActions.listen();

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

  console.log("webex-ws-sidecar is listening for Webex events");
}

async function shutdown() {
  try {
    if (typeof webex.messages.stopListening === "function") {
      await webex.messages.stopListening();
    }
  } catch (error) {
    console.error("failed to stop messages listener", error);
  }

  try {
    if (typeof webex.attachmentActions.stopListening === "function") {
      await webex.attachmentActions.stopListening();
    }
  } catch (error) {
    console.error("failed to stop attachment actions listener", error);
  }

  try {
    if (webex.internal?.mercury?.connected) {
      await webex.internal.mercury.disconnect();
    }
  } catch (error) {
    console.error("failed to disconnect mercury", error);
  }
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
