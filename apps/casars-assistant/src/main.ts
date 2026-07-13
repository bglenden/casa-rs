// SPDX-License-Identifier: LGPL-3.0-or-later

import { createInterface } from "node:readline";

import {
  FakeAdapter,
  PiAdapter,
  type AdapterHost,
  type AssistantAdapter,
  type TurnToolResult,
} from "./adapter.js";
import {
  CONSTRAINED_POLICY,
  PROTOCOL_VERSION,
  isConstrainedPolicy,
  parseRequest,
  type ProtocolEvent,
} from "./protocol.js";

interface PendingToolBatch {
  expected: Set<string>;
  results: Map<string, TurnToolResult>;
  resolve(results: TurnToolResult[]): void;
}

class JsonLineHost implements AdapterHost {
  private readonly toolBatches = new Map<string, PendingToolBatch>();
  private readonly promptResolvers = new Map<
    string,
    { resolve(value: string): void; reject(error: Error): void }
  >();

  emit(event: ProtocolEvent): void {
    process.stdout.write(`${JSON.stringify(event)}\n`);
  }

  waitForToolResults(requestId: string, callIds: string[]): Promise<TurnToolResult[]> {
    return new Promise((resolve) => {
      this.toolBatches.set(requestId, {
        expected: new Set(callIds),
        results: new Map(),
        resolve,
      });
    });
  }

  acceptToolResult(requestId: string, result: TurnToolResult): void {
    const batch = this.toolBatches.get(requestId);
    if (batch === undefined || !batch.expected.has(result.callId)) {
      throw new Error(`unexpected tool result ${result.callId}`);
    }
    batch.results.set(result.callId, result);
    if (batch.results.size === batch.expected.size) {
      this.toolBatches.delete(requestId);
      batch.resolve([...batch.expected].map((id) => batch.results.get(id)!));
    }
  }

  waitForPrompt(requestId: string, promptId: string, signal?: AbortSignal): Promise<string> {
    return new Promise((resolve, reject) => {
      const key = `${requestId}:${promptId}`;
      this.promptResolvers.set(key, { resolve, reject });
      signal?.addEventListener(
        "abort",
        () => {
          this.promptResolvers.delete(key);
          reject(new Error("authentication prompt cancelled"));
        },
        { once: true },
      );
    });
  }

  acceptPrompt(requestId: string, promptId: string, value: string): void {
    const key = `${requestId}:${promptId}`;
    const pending = this.promptResolvers.get(key);
    if (pending === undefined) throw new Error(`unexpected authentication response ${promptId}`);
    this.promptResolvers.delete(key);
    pending.resolve(value);
  }

  cancelPrompts(requestId: string): void {
    for (const [key, pending] of this.promptResolvers) {
      if (!key.startsWith(`${requestId}:`)) continue;
      this.promptResolvers.delete(key);
      pending.reject(new Error("authentication cancelled by user"));
    }
  }
}

const host = new JsonLineHost();
const adapter: AssistantAdapter = process.env["CASARS_ASSISTANT_FAKE"] === "1"
  ? new FakeAdapter(host)
  : new PiAdapter(host);
const activeTurns = new Map<string, AbortController>();
const cancelledAuthentications = new Set<string>();

function fail(requestId: string, error: unknown, retryable = false): void {
  host.emit({
    event: "error",
    request_id: requestId,
    error: {
      code: "assistant_adapter_error",
      message: error instanceof Error ? error.message : String(error),
      retryable,
    },
  });
}

async function dispatch(line: string): Promise<boolean> {
  let request;
  try {
    request = parseRequest(line);
  } catch (error) {
    fail("unknown", error);
    return true;
  }
  try {
    switch (request.command) {
      case "hello":
        if (request.protocol_version !== PROTOCOL_VERSION) {
          throw new Error(`unsupported protocol version ${request.protocol_version}`);
        }
        if (!isConstrainedPolicy(request.policy)) {
          throw new Error("CASA-RS sidecar policy must deny filesystem, shell, Python, and direct host tools");
        }
        host.emit({
          event: "ready",
          request_id: request.request_id,
          protocol_version: PROTOCOL_VERSION,
          adapter: process.env["CASARS_ASSISTANT_FAKE"] === "1" ? "fixture" : "pi-ai",
          adapter_version: "0.80.2",
          policy: CONSTRAINED_POLICY,
        });
        break;
      case "catalog":
        await adapter.catalog(request.request_id);
        break;
      case "authenticate":
        void adapter.authenticate(request.request_id, request.provider).catch((error: unknown) => {
          if (!cancelledAuthentications.delete(request.request_id)) fail(request.request_id, error);
        });
        break;
      case "authentication_response":
        host.acceptPrompt(request.request_id, request.prompt_id, request.value);
        break;
      case "cancel_authentication":
        cancelledAuthentications.add(request.request_id);
        host.cancelPrompts(request.request_id);
        host.emit({ event: "authentication_cancelled", request_id: request.request_id });
        break;
      case "turn": {
        if (request.egress.provider !== request.provider || request.egress.model !== request.model) {
          throw new Error("egress manifest provider/model does not match the turn destination");
        }
        if (request.tools.some((tool) => !tool.read_only && !tool.name.startsWith("proposal."))) {
          throw new Error("the Pi sidecar accepts only read-only tools and proposal-only tools");
        }
        const controller = new AbortController();
        activeTurns.set(request.request_id, controller);
        void adapter
          .turn(
            {
              requestId: request.request_id,
              conversationId: request.conversation_id,
              provider: request.provider,
              model: request.model,
              messages: request.messages,
              egress: request.egress,
              tools: request.tools,
              ...(request.credential === undefined ? {} : { credential: request.credential }),
            },
            controller.signal,
          )
          .catch((error: unknown) => fail(request.request_id, error, true))
          .finally(() => activeTurns.delete(request.request_id));
        break;
      }
      case "tool_result":
        host.acceptToolResult(request.request_id, {
          callId: request.call_id,
          result: request.result,
          isError: request.is_error,
        });
        break;
      case "cancel":
        activeTurns.get(request.request_id)?.abort();
        activeTurns.delete(request.request_id);
        host.emit({ event: "cancelled", request_id: request.request_id });
        break;
      case "shutdown":
        for (const controller of activeTurns.values()) controller.abort();
        return false;
    }
  } catch (error) {
    fail(request.request_id, error);
  }
  return true;
}

const lines = createInterface({ input: process.stdin, crlfDelay: Infinity });
for await (const line of lines) {
  if (line.trim() === "") continue;
  if (!(await dispatch(line))) break;
}
