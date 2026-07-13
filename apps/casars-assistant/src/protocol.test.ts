// SPDX-License-Identifier: LGPL-3.0-or-later

import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { createInterface } from "node:readline";
import test from "node:test";

interface Harness {
  send(value: unknown): void;
  next(): Promise<Record<string, unknown>>;
  close(): Promise<void>;
}

function startFixture(): Harness {
  const child = spawn(process.execPath, [new URL("./main.js", import.meta.url).pathname], {
    env: { PATH: process.env["PATH"] ?? "", CASARS_ASSISTANT_FAKE: "1" },
    stdio: ["pipe", "pipe", "pipe"],
  });
  const lines = createInterface({ input: child.stdout });
  const iterator = lines[Symbol.asyncIterator]();
  return {
    send(value) {
      child.stdin.write(`${JSON.stringify(value)}\n`);
    },
    async next() {
      const line = await iterator.next();
      assert.equal(line.done, false);
      return JSON.parse(line.value) as Record<string, unknown>;
    },
    async close() {
      child.stdin.end();
      await once(child, "exit");
      assert.equal(child.exitCode, 0);
    },
  };
}

const policy = {
  provider_network_only: true,
  project_filesystem: false,
  shell: false,
  python: false,
  direct_host_tools: false,
};

test("fixture adapter handshakes and advertises the constrained policy", async () => {
  const harness = startFixture();
  harness.send({ command: "hello", request_id: "hello-1", protocol_version: 1, policy });
  const ready = await harness.next();
  assert.equal(ready["event"], "ready");
  assert.equal(ready["adapter"], "fixture");
  assert.deepEqual(ready["policy"], policy);
  harness.send({ command: "catalog", request_id: "catalog-1" });
  const catalog = await harness.next();
  assert.equal(catalog["event"], "catalog");
  harness.send({ command: "shutdown", request_id: "shutdown-1" });
  await harness.close();
});

test("authentication cancellation is explicit and does not fabricate a credential", async () => {
  const harness = startFixture();
  harness.send({ command: "cancel_authentication", request_id: "auth-cancelled" });
  const cancelled = await harness.next();
  assert.equal(cancelled["event"], "authentication_cancelled");
  assert.equal(cancelled["request_id"], "auth-cancelled");
  harness.send({ command: "shutdown", request_id: "shutdown-auth-cancelled" });
  await harness.close();
});

test("fixture requests corpus search through the host and never opens SQLite", async () => {
  const harness = startFixture();
  harness.send({ command: "hello", request_id: "hello-2", protocol_version: 1, policy });
  await harness.next();
  harness.send({
    command: "turn",
    request_id: "turn-1",
    conversation_id: "01900000-0000-7000-8000-000000000000",
    provider: "fixture",
    model: "fixture-v1",
    messages: [
      {
        id: "01900000-0000-7000-8000-000000000001",
        role: "user",
        content: "Please search the corpus for calibration advice",
        created_at: 1,
      },
    ],
    egress: {
      provider: "fixture",
      model: "fixture-v1",
      destination: "fixture",
      items: [],
      estimated_bytes: 0,
    },
    tools: [
      {
        name: "corpus.search",
        description: "Search the host-owned scientific corpus",
        input_schema: {
          type: "object",
          properties: { query: { type: "string" }, limit: { type: "integer" } },
          required: ["query"],
        },
        read_only: true,
      },
    ],
  });
  assert.equal((await harness.next())["event"], "turn_started");
  const toolCall = await harness.next();
  assert.equal(toolCall["event"], "tool_call");
  assert.equal(toolCall["name"], "corpus.search");
  harness.send({
    command: "tool_result",
    request_id: "turn-1",
    call_id: toolCall["call_id"],
    result: [{ text: "Use a nearby gain calibrator.", citation: { locator: "p. 4" } }],
    is_error: false,
  });
  assert.equal((await harness.next())["event"], "text_delta");
  assert.equal((await harness.next())["event"], "turn_complete");
  harness.send({ command: "shutdown", request_id: "shutdown-2" });
  await harness.close();
});

test("fixture fails closed when a mutating tool is offered", async () => {
  const harness = startFixture();
  harness.send({
    command: "turn",
    request_id: "turn-denied",
    conversation_id: "01900000-0000-7000-8000-000000000000",
    provider: "fixture",
    model: "fixture-v1",
    messages: [],
    egress: {
      provider: "fixture",
      model: "fixture-v1",
      destination: "fixture",
      items: [],
      estimated_bytes: 0,
    },
    tools: [
      {
        name: "task.execute",
        description: "Must not be exposed to Pi",
        input_schema: { type: "object" },
        read_only: false,
      },
    ],
  });
  const error = await harness.next();
  assert.equal(error["event"], "error");
  assert.match(JSON.stringify(error), /only read-only tools and proposal-only tools/);
  harness.send({ command: "shutdown", request_id: "shutdown-3" });
  await harness.close();
});

test("fixture can request a proposal but cannot insert or execute it", async () => {
  const harness = startFixture();
  harness.send({
    command: "turn",
    request_id: "turn-proposal",
    conversation_id: "01900000-0000-7000-8000-000000000000",
    provider: "fixture",
    model: "fixture-v1",
    messages: [{
      id: "01900000-0000-7000-8000-000000000002",
      role: "user",
      content: "Please propose a note",
      created_at: 1,
    }],
    egress: {
      provider: "fixture",
      model: "fixture-v1",
      destination: "fixture",
      items: [],
      estimated_bytes: 0,
    },
    tools: [{
      name: "proposal.note",
      description: "Create a pending host proposal only",
      input_schema: { type: "object" },
      read_only: false,
    }],
  });
  assert.equal((await harness.next())["event"], "turn_started");
  const call = await harness.next();
  assert.equal(call["name"], "proposal.note");
  harness.send({
    command: "tool_result",
    request_id: "turn-proposal",
    call_id: call["call_id"],
    result: { proposal_id: "proposal-1", status: "pending_user_review" },
    is_error: false,
  });
  assert.equal((await harness.next())["event"], "turn_complete");
  harness.send({ command: "shutdown", request_id: "shutdown-proposal" });
  await harness.close();
});

test("fixture Python requests remain pending host proposals", async () => {
  const harness = startFixture();
  harness.send({
    command: "turn",
    request_id: "turn-python-proposal",
    conversation_id: "01900000-0000-7000-8000-000000000000",
    provider: "fixture",
    model: "fixture-v1",
    messages: [{
      id: "01900000-0000-7000-8000-000000000003",
      role: "user",
      content: "Please propose Python",
      created_at: 1,
    }],
    egress: {
      provider: "fixture",
      model: "fixture-v1",
      destination: "fixture",
      items: [],
      estimated_bytes: 0,
    },
    tools: [{
      name: "proposal.python",
      description: "Create a pending isolated Python proposal",
      input_schema: { type: "object" },
      read_only: false,
    }],
  });
  assert.equal((await harness.next())["event"], "turn_started");
  const call = await harness.next();
  assert.equal(call["name"], "proposal.python");
  assert.match(JSON.stringify(call["arguments"]), /CASARS_ARTIFACT_STAGING/);
  harness.send({
    command: "tool_result",
    request_id: "turn-python-proposal",
    call_id: call["call_id"],
    result: { proposal_id: "proposal-python", status: "pending_user_review" },
    is_error: false,
  });
  assert.equal((await harness.next())["event"], "turn_complete");
  harness.send({ command: "shutdown", request_id: "shutdown-python-proposal" });
  await harness.close();
});
