import Foundation
@testable import CasarsMacCore
import XCTest

final class AssistantDiscussionTests: XCTestCase {
    func testWebResearchRejectsLocalAndCredentialBearingURLsBeforeNetwork() {
        XCTAssertFalse(AssistantWebResearchClient.isPublicHTTPS(URL(string: "http://example.com")!))
        XCTAssertFalse(AssistantWebResearchClient.isPublicHTTPS(URL(string: "https://localhost/data")!))
        XCTAssertFalse(AssistantWebResearchClient.isPublicHTTPS(URL(string: "https://127.0.0.1/data")!))
        XCTAssertFalse(AssistantWebResearchClient.isPublicHTTPS(URL(string: "https://user:secret@example.com")!))
    }

    func testRustPersistenceAndCorpusBridgeStayProviderNeutral() throws {
        let project = try temporaryProject()
        let client = UniFFIAssistantPersistenceClient()
        var conversation = try client.createConversation(
            projectRoot: project.path,
            title: "Calibration",
            attachment: AssistantAttachmentState(
                kind: "notebook",
                identifier: "Analysis.md",
                label: "Analysis",
                primary: true
            ),
            provider: "openai-codex",
            model: "gpt-5.4"
        )
        conversation.draft = "Continue this discussion"
        try client.saveConversation(projectRoot: project.path, transcript: conversation)
        let reloaded = try client.conversations(projectRoot: project.path)
        XCTAssertEqual(reloaded.first?.draft, "Continue this discussion")
        XCTAssertEqual(reloaded.first?.provider, "openai-codex")

        let messageID = UUID().uuidString.lowercased()
        let snapshot = "### AI discussion snapshot\nA cited answer."
        let pin = try client.createPin(AssistantCreatePinEnvelope(
            conversationId: conversation.id,
            notebookId: UUID().uuidString.lowercased(),
            messageId: messageID,
            representation: "answer_with_citations",
            destination: "Analysis.md#chronological-tail",
            snapshotContent: snapshot
        ))
        XCTAssertEqual(pin.conversationId, conversation.id)
        XCTAssertEqual(pin.messageId, messageID)
        XCTAssertEqual(pin.snapshotContent, snapshot)
        XCTAssertEqual(pin.contentSha256.count, 64)

        _ = try client.indexCorpus(
            projectRoot: project.path,
            documents: [
                AssistantCorpusDocumentRequest(
                    id: "radio:gain",
                    layer: "baseline",
                    title: "Gain calibration",
                    sourceIdentity: "radio/gain.md",
                    content: "Gain calibration solves antenna amplitude and phase corrections.",
                    citation: AssistantCorpusCitationRequest(
                        label: "Radio guide",
                        locator: "section 3",
                        sourcePath: "radio/gain.md",
                        page: nil,
                        section: "Gain calibration",
                        lineStart: nil,
                        lineEnd: nil,
                        release: nil,
                        commit: nil
                    ),
                    redistributionCleared: true
                ),
            ],
            removeMissingLayers: ["baseline"]
        )
        let hits = try client.searchCorpus(
            projectRoot: project.path,
            query: "antenna phase gain",
            limit: 4
        )
        XCTAssertEqual(hits.first?.citation.section, "Gain calibration")
        XCTAssertEqual(hits.first?.untrustedEvidence, true)
    }

    func testSeatbeltSidecarUsesHostMediatedCorpusTool() throws {
        let repository = URL(fileURLWithPath: FileManager.default.currentDirectoryPath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        let entrypoint = repository.appendingPathComponent("apps/casars-assistant/dist/main.js").path
        let node = ["/opt/homebrew/bin/node", "/usr/local/bin/node"]
            .first(where: FileManager.default.isExecutableFile(atPath:))
        guard let node, FileManager.default.fileExists(atPath: entrypoint) else {
            throw XCTSkip("assistant adapter or Node is not built")
        }
        let ready = expectation(description: "sidecar ready")
        let catalog = expectation(description: "catalog")
        let toolCall = expectation(description: "corpus tool call")
        let completed = expectation(description: "turn complete")
        var startupResult: Result<Void, Error>?
        let sidecar = AssistantSidecar(configuration: AssistantSidecarConfiguration(
            nodeExecutable: node,
            entrypoint: entrypoint,
            fixtureMode: true
        ))
        sidecar.onEvent { event in
            switch event["event"] as? String {
            case "catalog":
                catalog.fulfill()
            case "tool_call":
                XCTAssertEqual(event["name"] as? String, "corpus.search")
                toolCall.fulfill()
                sidecar.send([
                    "command": "tool_result",
                    "request_id": "turn-test",
                    "call_id": event["call_id"] as? String ?? "",
                    "result": [[
                        "text": "Use a gain calibrator",
                        "citation": ["locator": "section 3"],
                    ]],
                    "is_error": false,
                ])
            case "turn_complete":
                completed.fulfill()
            case "error":
                XCTFail("sidecar error: \(event)")
            default:
                break
            }
        }
        sidecar.prepare { result in
            startupResult = result
            ready.fulfill()
        }
        wait(for: [ready], timeout: 12)
        guard case .success = startupResult else {
            XCTFail("sidecar startup failed: \(String(describing: startupResult))")
            sidecar.terminate()
            return
        }
        sidecar.send(["command": "catalog", "request_id": "catalog-test"])
        wait(for: [catalog], timeout: 5)
        sidecar.send([
            "command": "turn",
            "request_id": "turn-test",
            "conversation_id": UUID().uuidString,
            "provider": "fixture",
            "model": "fixture-v1",
            "messages": [[
                "id": UUID().uuidString,
                "role": "user",
                "content": "Please search the corpus",
                "created_at": 1,
            ]],
            "egress": [
                "provider": "fixture",
                "model": "fixture-v1",
                "destination": "fixture",
                "items": [],
                "estimated_bytes": 0,
            ],
            "tools": [[
                "name": "corpus.search",
                "description": "Search host corpus",
                "input_schema": [
                    "type": "object",
                    "properties": ["query": ["type": "string"]],
                    "required": ["query"],
                ],
                "read_only": true,
            ]],
        ])
        wait(for: [toolCall, completed], timeout: 8)
        sidecar.terminate()
    }

    func testOptInLiveProviderSmokeUsesHostKeychainLease() throws {
        let environment = ProcessInfo.processInfo.environment
        guard let provider = environment["CASA_RS_ASSISTANT_LIVE_SMOKE_PROVIDER"],
              let model = environment["CASA_RS_ASSISTANT_LIVE_SMOKE_MODEL"]
        else { throw XCTSkip("set live smoke provider and model to opt in") }
        guard let credential = try KeychainAssistantCredentialVault().load(provider: provider) else {
            XCTFail("No host Keychain credential is stored for \(provider); authenticate in casars-mac first")
            return
        }
        let complete = expectation(description: "live provider response")
        let ready = expectation(description: "live sidecar ready")
        let sidecar = AssistantSidecar(configuration: try .discover())
        sidecar.onEvent { event in
            if event["event"] as? String == "turn_complete" { complete.fulfill() }
            if event["event"] as? String == "error" { XCTFail("live smoke failed: \(event)") }
        }
        sidecar.prepare { result in
            if case let .failure(error) = result { XCTFail("live sidecar startup: \(error)") }
            ready.fulfill()
        }
        wait(for: [ready], timeout: 15)
        sidecar.send([
            "command": "turn",
            "request_id": "live-smoke-\(UUID().uuidString)",
            "conversation_id": UUID().uuidString,
            "provider": provider,
            "model": model,
            "messages": [[
                "id": UUID().uuidString,
                "role": "user",
                "content": "Reply with exactly CASA-RS assistant live smoke OK.",
                "created_at": 1,
            ]],
            "egress": [
                "provider": provider,
                "model": model,
                "destination": provider,
                "items": [],
                "estimated_bytes": 0,
            ],
            "tools": [],
            "credential": try JSONSerialization.jsonObject(with: JSONEncoder().encode(credential)),
        ])
        wait(for: [complete], timeout: 90)
        sidecar.terminate()
    }

    func testCorpusIngestorLayersBaselineProjectDocumentsAndReleaseSource() throws {
        let project = try temporaryProject()
        let documents = project.appendingPathComponent("documents", isDirectory: true)
        try FileManager.default.createDirectory(at: documents, withIntermediateDirectories: true)
        try "TW Hya has a nearly face-on disk."
            .write(to: documents.appendingPathComponent("twhya.md"), atomically: true, encoding: .utf8)

        let source = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-source-tests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(
            at: source.appendingPathComponent("docs", isDirectory: true),
            withIntermediateDirectories: true
        )
        try "# Architecture\nCASA-RS tasks use typed provider contracts."
            .write(to: source.appendingPathComponent("ARCHITECTURE.md"), atomically: true, encoding: .utf8)
        try "MeasurementSet source semantics."
            .write(to: source.appendingPathComponent("docs/source.md"), atomically: true, encoding: .utf8)
        addTeardownBlock { try? FileManager.default.removeItem(at: source) }

        let result = AssistantCorpusIngestor().collect(
            projectRoot: project.path,
            environment: ["CASA_RS_SOURCE_ROOT": source.path]
        )
        XCTAssertTrue(result.documents.contains { $0.layer == "baseline" })
        XCTAssertTrue(result.documents.contains {
            $0.layer == "project_document" && $0.sourceIdentity == "documents/twhya.md"
        })
        XCTAssertTrue(result.documents.contains {
            $0.layer == "release_source" && $0.sourceIdentity == "ARCHITECTURE.md"
        })
        XCTAssertEqual(
            result.documents.filter { $0.layer == "baseline" }.allSatisfy(\.redistributionCleared),
            true
        )
    }

    private func temporaryProject() throws -> URL {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-assistant-tests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
        addTeardownBlock { try? FileManager.default.removeItem(at: url) }
        return url
    }
}
