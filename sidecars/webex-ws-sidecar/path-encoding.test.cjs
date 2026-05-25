#!/usr/bin/env node

const assert = require("node:assert/strict");

process.env.WEBEX_BOT_TOKEN = "eyJhbGciOiJub25lIn0.eyJzdWIiOiJ0ZXN0In0.signature";
process.env.WEBEX_BOT_EMAIL = "bot@example.com";

const { webexPathSegment, webexResourceUrl } = require("./index.cjs");

assert.equal(webexPathSegment("message/with space+="), "message%2Fwith%20space%2B%3D");
assert.equal(
  webexResourceUrl("messages", "message/with space+="),
  "https://webexapis.com/v1/messages/message%2Fwith%20space%2B%3D"
);
assert.equal(
  webexResourceUrl("attachment/actions", "action/with space+="),
  "https://webexapis.com/v1/attachment/actions/action%2Fwith%20space%2B%3D"
);
