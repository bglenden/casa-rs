// SPDX-License-Identifier: LGPL-3.0-or-later

export const PROTOCOL_VERSION = 1;

export interface SidecarPolicy {
  provider_network_only: boolean;
  project_filesystem: boolean;
  shell: boolean;
  python: boolean;
  direct_host_tools: boolean;
}

export const CONSTRAINED_POLICY: SidecarPolicy = Object.freeze({
  provider_network_only: true,
  project_filesystem: false,
  shell: false,
  python: false,
  direct_host_tools: false,
});

export interface CredentialLease {
  provider: string;
  credential_type: string;
  secret: string;
  expires_at?: number;
}

export interface VisibleMessage {
  id: string;
  role: "user" | "assistant" | "tool";
  content: string;
  created_at: number;
  provider?: string;
  model?: string;
  citations?: unknown[];
  proposals?: unknown[];
  pins?: unknown[];
}

export interface EgressItem {
  id: string;
  kind: string;
  label: string;
  summary: string;
  excerpt: string;
  byte_count: number;
  content_sha256: string;
  provider_visible: boolean;
  untrusted_evidence: boolean;
}

export interface EgressManifest {
  provider: string;
  model: string;
  destination: string;
  items: EgressItem[];
  estimated_bytes: number;
}

export interface ToolDefinition {
  name: string;
  description: string;
  input_schema: Record<string, unknown>;
  read_only: boolean;
}

export type ProtocolRequest =
  | { command: "hello"; request_id: string; protocol_version: number; policy: SidecarPolicy }
  | { command: "catalog"; request_id: string }
  | { command: "authenticate"; request_id: string; provider: string }
  | { command: "authentication_response"; request_id: string; prompt_id: string; value: string }
  | {
      command: "turn";
      request_id: string;
      conversation_id: string;
      provider: string;
      model: string;
      messages: VisibleMessage[];
      egress: EgressManifest;
      tools: ToolDefinition[];
      credential?: CredentialLease;
    }
  | { command: "tool_result"; request_id: string; call_id: string; result: unknown; is_error: boolean }
  | { command: "cancel"; request_id: string }
  | { command: "shutdown"; request_id: string };

export type ProtocolEvent = Record<string, unknown> & {
  event: string;
  request_id: string;
};

export function isConstrainedPolicy(policy: SidecarPolicy): boolean {
  return (
    policy.provider_network_only &&
    !policy.project_filesystem &&
    !policy.shell &&
    !policy.python &&
    !policy.direct_host_tools
  );
}

export function parseRequest(line: string): ProtocolRequest {
  const value: unknown = JSON.parse(line);
  if (typeof value !== "object" || value === null || !("command" in value) || !("request_id" in value)) {
    throw new Error("request must be a JSON object with command and request_id");
  }
  return value as ProtocolRequest;
}
