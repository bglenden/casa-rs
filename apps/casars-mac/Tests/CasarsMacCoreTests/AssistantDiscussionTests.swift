import Foundation
@testable import CasarsMacCore
import XCTest

final class AssistantDiscussionTests: XCTestCase {
    func testHostExecutableVersionUsesBundleMetadataWithoutLaunchingTheApp() {
        let executable = "/Applications/casars-mac.app/Contents/MacOS/casars-mac"

        XCTAssertEqual(
            WorkbenchStore.assistantHostVersionOverride(
                executablePath: executable,
                hostExecutablePath: executable,
                shortVersion: "1.2.3",
                buildVersion: "45"
            ),
            "1.2.3 (45)"
        )
        XCTAssertEqual(
            WorkbenchStore.assistantHostVersionOverride(
                executablePath: executable,
                hostExecutablePath: executable,
                shortVersion: nil,
                buildVersion: nil
            ),
            "unreported"
        )
        XCTAssertNil(
            WorkbenchStore.assistantHostVersionOverride(
                executablePath: "/usr/bin/python3",
                hostExecutablePath: executable,
                shortVersion: "1.2.3",
                buildVersion: "45"
            )
        )
    }

    func testProviderEgressNeverSerializesDeselectedHostContext() throws {
        let visible = AssistantContextItemState(
            id: "visible",
            kind: "notebook",
            label: "Visible",
            summary: "selected",
            excerpt: "selected excerpt",
            providerVisible: true,
            untrustedEvidence: true
        )
        let hidden = AssistantContextItemState(
            id: "hidden",
            kind: "source",
            label: "Hidden",
            summary: "local only",
            excerpt: "must not cross stdin",
            providerVisible: false,
            untrustedEvidence: true
        )

        let egress = AssistantEgressState.providerBound(
            provider: "fixture",
            model: "fixture-v1",
            destination: "fixture",
            contexts: [visible, hidden]
        )
        let encoded = String(decoding: try JSONEncoder().encode(egress), as: UTF8.self)

        XCTAssertEqual(egress.items.map(\.id), ["visible"])
        XCTAssertEqual(egress.estimatedBytes, visible.byteCount)
        XCTAssertFalse(encoded.contains("must not cross stdin"))
    }

    func testProviderEgressAppliesOneUnicodeSafeGlobalBudget() {
        let contexts = (0..<6).map { index in
            AssistantContextItemState(
                id: "context-\(index)",
                kind: "notebook",
                label: "Context \(index)",
                summary: "selected",
                excerpt: String(repeating: "🔭science", count: 4_000),
                providerVisible: true,
                untrustedEvidence: true
            )
        }

        let egress = AssistantEgressState.providerBound(
            provider: "fixture",
            model: "fixture-v1",
            destination: "fixture",
            contexts: contexts
        )

        XCTAssertLessThanOrEqual(egress.estimatedBytes, 65_536)
        XCTAssertEqual(egress.estimatedBytes, UInt64(egress.items.reduce(0) { $0 + $1.excerpt.utf8.count }))
        XCTAssertTrue(egress.items.allSatisfy { String(data: Data($0.excerpt.utf8), encoding: .utf8) != nil })
        XCTAssertTrue(egress.items.allSatisfy { $0.contentSha256.count == 64 })
    }

    func testProviderTranscriptProjectionExcludesHostOnlyReceiptsAndContext() throws {
        let message = AssistantMessageState(
            id: "message-1",
            role: "assistant",
            content: "Visible answer",
            createdAt: 1,
            provider: "fixture",
            model: "fixture-v1",
            citations: [AssistantCitationState(
                id: "citation",
                kind: "source",
                label: "Private source",
                locator: "line 1",
                excerpt: "host-only citation excerpt",
                sourcePath: "source.rs",
                page: nil,
                section: nil,
                lineStart: 1,
                lineEnd: 2,
                release: nil,
                commit: nil
            )],
            egress: AssistantEgressState.providerBound(
                provider: "fixture",
                model: "fixture-v1",
                destination: "fixture",
                contexts: []
            ),
            proposals: [],
            pins: []
        )

        let encoded = String(
            decoding: try JSONEncoder().encode(AssistantProviderMessageState(message: message)),
            as: UTF8.self
        )

        XCTAssertTrue(encoded.contains("Visible answer"))
        XCTAssertFalse(encoded.contains("host-only citation excerpt"))
        XCTAssertFalse(encoded.contains("citations"))
        XCTAssertFalse(encoded.contains("egress"))
        XCTAssertFalse(encoded.contains("proposals"))
        XCTAssertFalse(encoded.contains("pins"))
    }

    func testProviderTranscriptProjectionKeepsNewestMessagesWithinGlobalBudget() {
        let messages = (0..<140).map { index in
            AssistantMessageState(
                id: "message-\(index)",
                role: index.isMultiple(of: 2) ? "user" : "assistant",
                content: "\(index):" + String(repeating: "🔭", count: 4_000),
                createdAt: UInt64(index),
                provider: nil,
                model: nil,
                citations: [],
                egress: nil,
                proposals: [],
                pins: []
            )
        }

        let projection = AssistantProviderMessageState.providerBound(messages)
        XCTAssertLessThanOrEqual(projection.count, 128)
        XCTAssertEqual(projection.last?.id, "message-139")
        XCTAssertFalse(projection.contains { $0.id == "message-0" })
        XCTAssertLessThanOrEqual(projection.reduce(0) { $0 + $1.content.utf8.count }, 262_144)
        XCTAssertTrue(projection.allSatisfy { String(data: Data($0.content.utf8), encoding: .utf8) != nil })
    }

    func testWebResearchRejectsLocalAndCredentialBearingURLsBeforeNetwork() {
        XCTAssertFalse(AssistantWebResearchClient.isPublicHTTPS(URL(string: "http://example.com")!))
        XCTAssertFalse(AssistantWebResearchClient.isPublicHTTPS(URL(string: "https://localhost/data")!))
        XCTAssertFalse(AssistantWebResearchClient.isPublicHTTPS(URL(string: "https://127.0.0.1/data")!))
        XCTAssertFalse(AssistantWebResearchClient.isPublicHTTPS(URL(string: "https://user:secret@example.com")!))
        XCTAssertTrue(AssistantWebResearchClient.isPublicIPAddress("8.8.8.8"))
        XCTAssertTrue(AssistantWebResearchClient.isPublicIPAddress("2001:4860:4860::8888"))
        XCTAssertFalse(AssistantWebResearchClient.isPublicIPAddress("127.0.0.1"))
        XCTAssertFalse(AssistantWebResearchClient.isPublicIPAddress("192.168.1.4"))
        XCTAssertFalse(AssistantWebResearchClient.isPublicIPAddress("100.64.0.1"))
        XCTAssertFalse(AssistantWebResearchClient.isPublicIPAddress("fc00::1"))
        XCTAssertFalse(AssistantWebResearchClient.isPublicIPAddress("::ffff:127.0.0.1"))
        XCTAssertFalse(AssistantWebResearchClient.isPublicIPAddress("2001:db8::1"))
        XCTAssertThrowsError(try AssistantApprovedDownloadClient().download(
            URL(string: "https://127.0.0.1/private")!,
            isCancelled: { false }
        ))
    }

    func testAssistantProjectWritesRejectTraversalAndSymlinkAncestors() throws {
        let project = try temporaryProject()
        let outside = try temporaryProject()
        let safe = try WorkbenchStore.assistantProjectDestination(
            projectRoot: project.path,
            relativePath: "documents/paper.pdf"
        )
        XCTAssertEqual(safe.path, project.appendingPathComponent("documents/paper.pdf").path)
        XCTAssertThrowsError(try WorkbenchStore.assistantProjectDestination(
            projectRoot: project.path,
            relativePath: "../outside.txt"
        ))

        let link = project.appendingPathComponent("linked", isDirectory: true)
        try FileManager.default.createSymbolicLink(at: link, withDestinationURL: outside)
        XCTAssertThrowsError(try WorkbenchStore.assistantProjectDestination(
            projectRoot: project.path,
            relativePath: "linked/escaped.txt"
        ))
    }

    func testDiscoveredFixtureModeIsOwnedByTheHostConfiguration() throws {
        let root = try temporaryProject()
        let node = root.appendingPathComponent("node")
        let entrypoint = root.appendingPathComponent("main.js")
        try "#!/bin/sh\necho v22.19.0\n".write(to: node, atomically: true, encoding: .utf8)
        try FileManager.default.setAttributes([.posixPermissions: 0o755], ofItemAtPath: node.path)
        try "// fixture\n".write(to: entrypoint, atomically: true, encoding: .utf8)

        let configuration = try AssistantSidecarConfiguration.discover(environment: [
            "CASA_RS_ASSISTANT_NODE": node.path,
            "CASA_RS_ASSISTANT_ENTRYPOINT": entrypoint.path,
            "CASA_RS_ASSISTANT_FIXTURE": "1",
            "PATH": "",
        ])

        XCTAssertEqual(configuration.nodeExecutable, node.path)
        XCTAssertEqual(configuration.entrypoint, entrypoint.path)
        XCTAssertTrue(configuration.fixtureMode)
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
        let toolResult = AssistantContextItemState(
            id: "tool:turn-1:0",
            kind: "tool_result",
            label: "proposal.note",
            summary: #"{"title":"Add a note"}"#,
            excerpt: #"{"proposal_id":"proposal-1","status":"pending_user_review"}"#,
            providerVisible: true,
            untrustedEvidence: true
        )
        conversation.messages.append(AssistantMessageState(
            id: UUID().uuidString.lowercased(),
            role: "assistant",
            content: "I prepared a notebook proposal.",
            createdAt: 1,
            provider: "openai-codex",
            model: "gpt-5.4",
            citations: [],
            egress: AssistantEgressState(
                provider: "openai-codex",
                model: "gpt-5.4",
                destination: "provider",
                items: [toolResult],
                estimatedBytes: toolResult.byteCount
            ),
            proposals: [],
            pins: []
        ))
        try client.saveConversation(projectRoot: project.path, transcript: conversation)
        let reloaded = try client.conversations(projectRoot: project.path)
        XCTAssertEqual(reloaded.first?.draft, "Continue this discussion")
        XCTAssertEqual(reloaded.first?.provider, "openai-codex")
        XCTAssertEqual(reloaded.first?.messages.first?.egress?.items.first?.kind, "tool_result")

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
        var response = ""
        let sidecar = AssistantSidecar(configuration: try .discover())
        sidecar.onEvent { event in
            if event["event"] as? String == "turn_complete" {
                response = (event["message"] as? [String: Any])?["content"] as? String ?? ""
                complete.fulfill()
            }
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
        XCTAssertEqual(
            response.trimmingCharacters(in: .whitespacesAndNewlines),
            "CASA-RS assistant live smoke OK"
        )
        sidecar.terminate()
    }

    func testCorpusIngestorLayersBaselineProjectDocumentsAndReleaseSource() throws {
        let project = try temporaryProject()
        let documents = project.appendingPathComponent("documents", isDirectory: true)
        try FileManager.default.createDirectory(at: documents, withIntermediateDirectories: true)
        try "TW Hya has a nearly face-on disk."
            .write(to: documents.appendingPathComponent("twhya.md"), atomically: true, encoding: .utf8)
        let outside = project.appendingPathComponent("outside-secret.md")
        try "must not enter the corpus".write(to: outside, atomically: true, encoding: .utf8)
        try FileManager.default.createSymbolicLink(
            at: documents.appendingPathComponent("linked-secret.md"),
            withDestinationURL: outside
        )

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
        try #"{"schema_version":1,"release":"0.24.1","commit":"abc123"}"#
            .write(to: source.appendingPathComponent("casars-source.json"), atomically: true, encoding: .utf8)
        addTeardownBlock { try? FileManager.default.removeItem(at: source) }

        let result = AssistantCorpusIngestor().collect(
            projectRoot: project.path,
            environment: ["CASA_RS_SOURCE_ROOT": source.path]
        )
        XCTAssertTrue(result.documents.contains { $0.layer == "baseline" })
        XCTAssertTrue(result.documents.contains {
            $0.layer == "project_document" && $0.sourceIdentity == "documents/twhya.md"
        })
        XCTAssertFalse(result.documents.contains { $0.content.contains("must not enter the corpus") })
        XCTAssertTrue(result.documents.contains {
            $0.layer == "release_source"
                && $0.sourceIdentity == "ARCHITECTURE.md"
                && $0.citation.release == "0.24.1"
                && $0.citation.commit == "abc123"
        })
        XCTAssertEqual(
            result.documents.filter { $0.layer == "baseline" }.allSatisfy(\.redistributionCleared),
            true
        )
    }

    func testCorpusIngestorRejectsSymlinkedProjectDocumentRoot() throws {
        let project = try temporaryProject()
        let outside = try temporaryProject()
        try "must remain outside".write(
            to: outside.appendingPathComponent("paper.md"),
            atomically: true,
            encoding: .utf8
        )
        try FileManager.default.createSymbolicLink(
            at: project.appendingPathComponent("documents"),
            withDestinationURL: outside
        )

        let result = AssistantCorpusIngestor().collect(projectRoot: project.path, environment: [:])
        XCTAssertFalse(result.documents.contains { $0.content.contains("must remain outside") })
        XCTAssertTrue(result.diagnostics.contains { $0.contains("symbolic-link or invalid corpus root") })
    }

    private func temporaryProject() throws -> URL {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-assistant-tests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
        addTeardownBlock { try? FileManager.default.removeItem(at: url) }
        return url
    }
}
