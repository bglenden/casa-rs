import Foundation
import XCTest
@testable import CasarsMacCore

final class StandardAssistantCorpusTests: XCTestCase {
    func testBundledCorpusLoadsWithoutOracleAndRetainsExactSlideCitations() throws {
        let project = try temporaryDirectory("project")
        defer { try? FileManager.default.removeItem(at: project) }

        let started = Date()
        let result = AssistantCorpusIngestor().collect(
            projectRoot: project.path,
            environment: [:]
        )
        let baseline = result.documents.filter { $0.layer == "baseline" }
        XCTAssertEqual(baseline.count, 2_315)
        XCTAssertEqual(Set(baseline.compactMap(\.citation.sourcePath)).count, 29)
        XCTAssertTrue(result.diagnostics.contains {
            $0 == "Installed baseline casa-rs-standard-radio-astronomy@2026.07.1: 29/29 sources available, 2315 cited documents."
        })
        let geometry = try XCTUnwrap(baseline.first {
            $0.citation.label == "Perley-Geometry-SIW2026.pdf" && $0.citation.page == 15
        })
        XCTAssertTrue(geometry.content.contains("fringe frequency"))
        XCTAssertEqual(geometry.citation.locator, "Perley-Geometry-SIW2026.pdf, slide 15")

        let client = UniFFIAssistantPersistenceClient()
        _ = try client.indexCorpus(
            projectRoot: project.path,
            documents: baseline,
            removeMissingLayers: ["baseline"]
        )
        let hits = try client.searchCorpus(
            projectRoot: project.path,
            query: "fringe frequency geometric time delay",
            limit: 16
        )
        XCTAssertTrue(hits.contains {
            $0.citation.label == "Perley-Geometry-SIW2026.pdf" && $0.citation.page == 15
        })
        let theoryHits = try client.searchCorpus(
            projectRoot: project.path,
            query: "van Cittert Zernike theorem",
            limit: 32
        )
        XCTAssertTrue(theoryHits.contains {
            $0.citation.label == "Interferometry and Synthesis in Radio Astronomy.pdf"
        })

        let index = project.appendingPathComponent(".casa-rs/corpus/index.sqlite3")
        let indexBytes = try XCTUnwrap(
            try index.resourceValues(forKeys: [.fileSizeKey]).fileSize
        )
        print(
            "standard corpus measurement: documents=\(baseline.count) "
                + "seconds=\(Date().timeIntervalSince(started)) index_bytes=\(indexBytes)"
        )
    }

    func testVersionedManifestRejectsContentWhoseDigestDoesNotMatch() throws {
        let project = try temporaryDirectory("project")
        let baseline = try temporaryDirectory("baseline")
        defer {
            try? FileManager.default.removeItem(at: project)
            try? FileManager.default.removeItem(at: baseline)
        }
        try "tampered content".write(
            to: baseline.appendingPathComponent("document.md"),
            atomically: true,
            encoding: .utf8
        )
        let manifest = #"""
        {
          "schema_version": 2,
          "id": "test-pack",
          "version": "1.0.0",
          "documents": [{
            "path": "document.md",
            "format": "utf8_text",
            "title": "Test",
            "citation_label": "Test",
            "citation_kind": "document",
            "source_path": "document.md",
            "content_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "source_sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "origin_url": "https://example.invalid/document",
            "license": {"id": "CC0-1.0", "url": "https://creativecommons.org/publicdomain/zero/1.0/"},
            "redistribution_basis": "test fixture"
          }]
        }
        """#
        try manifest.write(
            to: baseline.appendingPathComponent("corpus-pack.json"),
            atomically: true,
            encoding: .utf8
        )

        let result = AssistantCorpusIngestor().collect(
            projectRoot: project.path,
            environment: ["CASA_RS_ASSISTANT_BASELINE_ROOT": baseline.path]
        )
        XCTAssertFalse(result.documents.contains { $0.layer == "baseline" })
        XCTAssertTrue(result.diagnostics.contains {
            $0.contains("mismatched digest document.md")
        })
    }

    func testBaselineUpgradeRemovesStaleBaselineButPreservesProjectAndConversation() throws {
        let project = try temporaryDirectory("project")
        defer { try? FileManager.default.removeItem(at: project) }
        let client = UniFFIAssistantPersistenceClient()
        let projectDocument = document(
            id: "project-paper",
            layer: "project_document",
            content: "persistent project evidence cobalt observatory"
        )
        let oldBaseline = document(
            id: "baseline-old",
            layer: "baseline",
            content: "obsolete baseline evidence heliotrope correlator"
        )
        _ = try client.indexCorpus(
            projectRoot: project.path,
            documents: [projectDocument, oldBaseline],
            removeMissingLayers: []
        )
        let conversation = try client.createConversation(
            projectRoot: project.path,
            title: "Persistent discussion",
            attachment: AssistantAttachmentState(
                kind: "notebook", identifier: "analysis", label: "Analysis", primary: true
            ),
            profile: AssistantSessionProfileState()
        )

        let newBaseline = document(
            id: "baseline-new",
            layer: "baseline",
            content: "replacement baseline evidence vermilion synthesis"
        )
        _ = try client.indexCorpus(
            projectRoot: project.path,
            documents: [newBaseline],
            removeMissingLayers: ["baseline"]
        )

        XCTAssertTrue(try client.searchCorpus(
            projectRoot: project.path, query: "heliotrope correlator", limit: 8
        ).allSatisfy { $0.documentId != "baseline-old" })
        XCTAssertTrue(try client.searchCorpus(
            projectRoot: project.path, query: "cobalt observatory", limit: 8
        ).contains { $0.documentId == "project-paper" })
        XCTAssertTrue(try client.conversations(projectRoot: project.path).contains {
            $0.id == conversation.id
        })
    }

    func testOptInSubscriptionAgentRetrievesBookAndCurrentWorkshopWithCitations() throws {
        guard ProcessInfo.processInfo.environment["CASA_RS_CODEX_LIVE_STANDARD_CORPUS"] == "1"
        else {
            throw XCTSkip("Set CASA_RS_CODEX_LIVE_STANDARD_CORPUS=1 for the subscription retrieval check.")
        }
        let project = try temporaryDirectory("live-agent")
        defer { try? FileManager.default.removeItem(at: project) }
        let ingestion = AssistantCorpusIngestor().collect(
            projectRoot: project.path,
            environment: ["CASA_RS_SOURCE_ROOT": FileManager.default.currentDirectoryPath]
        )
        let persistence = UniFFIAssistantPersistenceClient()
        _ = try persistence.indexCorpus(
            projectRoot: project.path,
            documents: ingestion.documents,
            removeMissingLayers: ingestion.refreshedLayers
        )
        let expectedBook = try XCTUnwrap(persistence.searchCorpus(
            projectRoot: project.path, query: "van Cittert Zernike theorem", limit: 32
        ).first { $0.citation.label == "Interferometry and Synthesis in Radio Astronomy.pdf" })
        let expectedWorkshop = try XCTUnwrap(persistence.searchCorpus(
            projectRoot: project.path,
            query: "fringe frequency geometric time delay",
            limit: 32
        ).first {
            $0.citation.label == "Perley-Geometry-SIW2026.pdf" && $0.citation.page == 15
        })

        let session = CodexAppServerSession(configuration: try .discover())
        defer { session.terminate() }
        let ready = expectation(description: "Codex App Server ready")
        let account = expectation(description: "ChatGPT subscription account")
        let threadStarted = expectation(description: "standard-corpus thread started")
        let turnFinished = expectation(description: "standard-corpus turn finished")
        let profile = CasaAgentRuntimeProfile(
            authority: .explore,
            sessionNonce: String(repeating: "s", count: 32),
            pythonCommand: "python3"
        )
        var sent = false
        var observedAccount = false
        var corpusSearchCalls = 0
        var answer = ""
        var agentError: String?
        var eventTrace: [String] = []
        session.onEvent { event in
            if let result = event["result"] as? [String: Any] {
                if !observedAccount,
                   result.keys.contains("requiresOpenaiAuth"), result["account"] != nil
                {
                    observedAccount = true
                    account.fulfill()
                }
                if !sent,
                   let thread = result["thread"] as? [String: Any],
                   let id = thread["id"] as? String
                {
                    sent = true
                    threadStarted.fulfill()
                    session.sendTurn(AgentTurnRequest(
                        threadID: id,
                        text: """
                        Use only the \(profile.mcpServerName) project tools with the exact current nonce; do not use shell or web. Make two separate corpus.search calls restricted to the baseline layer. First search for `van Cittert Zernike theorem`. Then search for `fringe frequency geometric time delay`. Report one result from the synthesis-interferometry book and one from the 2026 Perley geometry deck, repeating each returned locator verbatim.
                        """,
                        model: "",
                        effort: "low"
                    ))
                }
            }
            guard let method = event["method"] as? String,
                  let params = event["params"] as? [String: Any]
            else { return }
            if method == "casa/error" || method == "error" {
                let message = String(describing: params["message"] ?? params)
                eventTrace.append("\(method): \(message)")
                if agentError == nil {
                    agentError = message
                    if sent { turnFinished.fulfill() }
                }
            } else if method == "item/completed",
                      let item = params["item"] as? [String: Any],
                      item["type"] as? String == "mcpToolCall",
                      item["server"] as? String == profile.mcpServerName,
                      item["tool"] as? String == "corpus.search"
            {
                corpusSearchCalls += 1
            } else if method == "item/agentMessage/delta",
                      let delta = params["delta"] as? String
            {
                answer += delta
            } else if method == "turn/completed" {
                eventTrace.append(method)
                if agentError == nil { turnFinished.fulfill() }
            }
        }
        session.prepare { result in
            if case let .failure(error) = result { XCTFail("Codex failed: \(error)") }
            ready.fulfill()
        }
        wait(for: [ready, account], timeout: 25)
        session.startConversation(AgentConversationRequest(
            projectRoot: project.path,
            model: "",
            effort: "low",
            resumeThreadID: nil,
            runtimeProfile: profile
        ))
        guard XCTWaiter.wait(for: [threadStarted], timeout: 25) == .completed else {
            XCTFail("Project-aware Codex thread did not start. Events: \(eventTrace)")
            return
        }
        guard XCTWaiter.wait(for: [turnFinished], timeout: 120) == .completed else {
            XCTFail("Agent corpus turn did not complete. Events: \(eventTrace)")
            return
        }
        XCTAssertNil(agentError, "Agent failed: \(agentError ?? "unknown"). Events: \(eventTrace)")
        XCTAssertGreaterThanOrEqual(corpusSearchCalls, 2, "Expected separate book and workshop searches")
        XCTAssertTrue(answer.contains(expectedBook.citation.locator), answer)
        XCTAssertTrue(answer.contains(expectedWorkshop.citation.locator), answer)
        print("CASA_RS_STANDARD_CORPUS_LIVE_AGENT \(answer)")
    }

    private func document(
        id: String,
        layer: String,
        content: String
    ) -> AssistantCorpusDocumentRequest {
        AssistantCorpusDocumentRequest(
            id: id,
            layer: layer,
            title: id,
            sourceIdentity: "fixture:\(id)",
            content: content,
            citation: AssistantCorpusCitationRequest(
                label: id,
                locator: id,
                sourcePath: "documents/\(id).md",
                page: nil,
                section: nil,
                lineStart: nil,
                lineEnd: nil,
                release: nil,
                commit: nil
            ),
            redistributionCleared: true
        )
    }

    private func temporaryDirectory(_ label: String) throws -> URL {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(
            "casars-standard-corpus-\(label)-\(UUID().uuidString)",
            isDirectory: true
        )
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        return directory
    }
}
