import AppKit
import Foundation
import XCTest
@testable import CasarsMacCore

final class ProjectCorpusRefreshTests: XCTestCase {
    func testOpeningProjectIndexesDocumentsAndWatcherRefreshesAnEditAutomatically() throws {
        let project = try temporaryProject()
        defer { try? FileManager.default.removeItem(at: project) }
        let documents = project.appendingPathComponent("documents", isDirectory: true)
        try FileManager.default.createDirectory(at: documents, withIntermediateDirectories: true)
        let note = documents.appendingPathComponent("observation.md")
        try "initial cerulean baseline".write(to: note, atomically: true, encoding: .utf8)
        let fixture = ProjectFixture(
            name: "Corpus test",
            rootPath: project.path,
            datasets: [],
            source: .probed
        )
        let store = WorkbenchStore(
            probeClient: ProjectCorpusProbeStub(
                result: ProjectFixtureProbe(project: fixture, diagnostics: [])
            )
        )
        store.openProject(path: project.path)
        waitUntil("startup corpus reconcile") {
            store.state.assistantDiscussion?.corpusStatus.contains("Local corpus ready") == true
        }
        let client = UniFFIAssistantPersistenceClient()
        XCTAssertEqual(
            try client.searchCorpus(projectRoot: project.path, query: "cerulean", limit: 4)
                .first?.citation.sourcePath,
            "documents/observation.md"
        )

        try "automatic magenta replacement".write(to: note, atomically: true, encoding: .utf8)
        waitUntil("automatic watcher refresh", timeout: 5) {
            (try? client.searchCorpus(projectRoot: project.path, query: "magenta", limit: 4)
                .first?.citation.sourcePath) == "documents/observation.md"
        }
        XCTAssertTrue(
            store.state.assistantDiscussion?.corpusDiagnostics.contains(where: {
                $0.contains("content reads")
            }) == true
        )

        let added = documents.appendingPathComponent("added.md")
        try "automatic vermilion addition".write(to: added, atomically: true, encoding: .utf8)
        waitUntil("automatic add refresh", timeout: 5) {
            (try? client.searchCorpus(projectRoot: project.path, query: "vermilion", limit: 4)
                .first?.citation.sourcePath) == "documents/added.md"
        }

        let renamed = documents.appendingPathComponent("renamed.md")
        try FileManager.default.moveItem(at: added, to: renamed)
        waitUntil("automatic rename refresh", timeout: 5) {
            (try? client.searchCorpus(projectRoot: project.path, query: "vermilion", limit: 4)
                .first?.citation.sourcePath) == "documents/renamed.md"
        }
        XCTAssertEqual(
            try client.searchCorpus(projectRoot: project.path, query: "magenta", limit: 4)
                .first?.citation.sourcePath,
            "documents/observation.md"
        )

        try FileManager.default.removeItem(at: renamed)
        waitUntil("automatic delete refresh", timeout: 5) {
            (try? client.searchCorpus(
                projectRoot: project.path, query: "vermilion", limit: 4
            ).isEmpty) == true
        }
        XCTAssertEqual(
            try client.searchCorpus(projectRoot: project.path, query: "magenta", limit: 4)
                .first?.citation.sourcePath,
            "documents/observation.md"
        )
    }

    func testNoChangeRefreshUsesMetadataOnlyAndAtomicReplacementIsReprocessed() throws {
        let project = try temporaryProject()
        defer { try? FileManager.default.removeItem(at: project) }
        let documents = project.appendingPathComponent("documents", isDirectory: true)
        try FileManager.default.createDirectory(at: documents, withIntermediateDirectories: true)
        let note = documents.appendingPathComponent("paper.md")
        try "original violet evidence".write(to: note, atomically: true, encoding: .utf8)
        try writeTextPDF(
            "unchanged PDF control phrase",
            to: documents.appendingPathComponent("control.pdf")
        )

        let ingestor = AssistantCorpusIngestor()
        let client = UniFFIAssistantPersistenceClient()
        let firstInventory = ingestor.projectDocumentInventory(projectRoot: project.path)
        let firstPlan = try client.projectCorpusPlan(
            projectRoot: project.path, sources: firstInventory.sources
        )
        XCTAssertEqual(firstPlan.extractPaths, ["documents/control.pdf", "documents/paper.md"])
        let first = ingestor.collect(
            projectRoot: project.path,
            projectInventory: firstInventory,
            extractProjectPaths: Set(firstPlan.extractPaths),
            scope: .projectDocuments
        )
        XCTAssertEqual(first.metrics.projectContentReads, 2)
        XCTAssertEqual(first.metrics.projectPDFExtractions, 1)
        _ = try client.indexCorpus(
            projectRoot: project.path,
            documents: first.documents,
            removeMissingLayers: first.refreshedLayers,
            projectSources: first.projectSources,
            failedProjectSources: first.failedProjectSources
        )

        let unchangedInventory = ingestor.projectDocumentInventory(projectRoot: project.path)
        let unchangedPlan = try client.projectCorpusPlan(
            projectRoot: project.path, sources: unchangedInventory.sources
        )
        XCTAssertTrue(unchangedPlan.extractPaths.isEmpty)
        let unchanged = ingestor.collect(
            projectRoot: project.path,
            projectInventory: unchangedInventory,
            extractProjectPaths: Set(unchangedPlan.extractPaths),
            scope: .projectDocuments
        )
        XCTAssertEqual(unchanged.metrics.projectContentReads, 0)
        XCTAssertEqual(unchanged.metrics.projectPDFExtractions, 0)
        XCTAssertEqual(unchanged.metrics.projectOCRCalls, 0)

        let originalDate = try XCTUnwrap(
            FileManager.default.attributesOfItem(atPath: note.path)[.modificationDate] as? Date
        )
        try "replacement ultraviolet evidence".write(to: note, atomically: true, encoding: .utf8)
        try FileManager.default.setAttributes([.modificationDate: originalDate], ofItemAtPath: note.path)
        let replacementInventory = ingestor.projectDocumentInventory(projectRoot: project.path)
        let replacementPlan = try client.projectCorpusPlan(
            projectRoot: project.path, sources: replacementInventory.sources
        )
        XCTAssertEqual(replacementPlan.extractPaths, ["documents/paper.md"])
        let replacement = ingestor.collect(
            projectRoot: project.path,
            projectInventory: replacementInventory,
            extractProjectPaths: Set(replacementPlan.extractPaths),
            scope: .projectDocuments
        )
        XCTAssertEqual(replacement.metrics.projectContentReads, 1)
        XCTAssertEqual(replacement.metrics.projectPDFExtractions, 0)
        _ = try client.indexCorpus(
            projectRoot: project.path,
            documents: replacement.documents,
            removeMissingLayers: replacement.refreshedLayers,
            projectSources: replacement.projectSources,
            failedProjectSources: replacement.failedProjectSources
        )
        XCTAssertTrue(try client.searchCorpus(
            projectRoot: project.path, query: "violet", limit: 4
        ).isEmpty)
        XCTAssertEqual(
            try client.searchCorpus(
                projectRoot: project.path, query: "replacement ultraviolet evidence", limit: 4
            ).first?.citation.sourcePath,
            "documents/paper.md"
        )
    }

    func testWatcherCoalescesBurstyDocumentEvents() throws {
        let project = try temporaryProject()
        defer { try? FileManager.default.removeItem(at: project) }
        let documents = project.appendingPathComponent("documents", isDirectory: true)
        try FileManager.default.createDirectory(at: documents, withIntermediateDirectories: true)
        let ready = expectation(description: "watcher ready")
        let changed = expectation(description: "one coalesced change")
        changed.assertForOverFulfill = true
        let lock = NSLock()
        var changes = 0
        let watcher = ProjectCorpusWatcher(
            projectRoot: project.path,
            debounceInterval: .milliseconds(100)
        ) {
            lock.lock()
            changes += 1
            lock.unlock()
            changed.fulfill()
        }
        watcher.start { ready.fulfill() }
        wait(for: [ready], timeout: 2)

        for index in 0..<8 {
            try "burst \(index)".write(
                to: documents.appendingPathComponent("note-\(index).md"),
                atomically: true,
                encoding: .utf8
            )
        }
        wait(for: [changed], timeout: 3)
        Thread.sleep(forTimeInterval: 0.3)
        watcher.stop()
        lock.lock()
        let observed = changes
        lock.unlock()
        XCTAssertEqual(observed, 1)
    }

    func testWatcherRecoversWhenDocumentsDirectoryIsCreated() throws {
        let project = try temporaryProject()
        defer { try? FileManager.default.removeItem(at: project) }
        let documents = project.appendingPathComponent("documents", isDirectory: true)
        let ready = expectation(description: "parent watcher ready")
        let changed = expectation(description: "directory and nested file observed")
        changed.expectedFulfillmentCount = 2
        let lock = NSLock()
        var changes = 0
        let watcher = ProjectCorpusWatcher(
            projectRoot: project.path,
            debounceInterval: .milliseconds(100)
        ) {
            lock.lock()
            changes += 1
            let current = changes
            lock.unlock()
            changed.fulfill()
            if current == 1 {
                try? "new evidence".write(
                    to: documents.appendingPathComponent("new.md"),
                    atomically: true,
                    encoding: .utf8
                )
            }
        }
        watcher.start { ready.fulfill() }
        wait(for: [ready], timeout: 2)
        try FileManager.default.createDirectory(at: documents, withIntermediateDirectories: true)
        wait(for: [changed], timeout: 4)
        watcher.stop()
    }

    private func temporaryProject() throws -> URL {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-project-corpus-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        return root
    }

    private func writeTextPDF(_ text: String, to url: URL) throws {
        guard let consumer = CGDataConsumer(url: url as CFURL) else {
            throw CocoaError(.fileWriteUnknown)
        }
        var mediaBox = CGRect(x: 0, y: 0, width: 612, height: 792)
        guard let context = CGContext(consumer: consumer, mediaBox: &mediaBox, nil) else {
            throw CocoaError(.fileWriteUnknown)
        }
        context.beginPDFPage(nil)
        let graphics = NSGraphicsContext(cgContext: context, flipped: false)
        NSGraphicsContext.saveGraphicsState()
        NSGraphicsContext.current = graphics
        (text as NSString).draw(
            in: NSRect(x: 72, y: 650, width: 468, height: 72),
            withAttributes: [.font: NSFont.systemFont(ofSize: 18)]
        )
        NSGraphicsContext.restoreGraphicsState()
        context.endPDFPage()
        context.closePDF()
    }

    private func waitUntil(
        _ description: String,
        timeout: TimeInterval = 3,
        condition: () -> Bool
    ) {
        let deadline = Date().addingTimeInterval(timeout)
        while Date() < deadline {
            if condition() { return }
            RunLoop.current.run(until: Date().addingTimeInterval(0.02))
        }
        XCTFail("Timed out waiting for \(description)")
    }
}

private struct ProjectCorpusProbeStub: ProjectProbeClient {
    var result: ProjectFixtureProbe

    func probeProject(path _: String) throws -> ProjectFixtureProbe { result }

    func probePath(path _: String) throws -> DatasetSummary? { nil }
}
