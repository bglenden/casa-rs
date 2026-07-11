import XCTest
@testable import CasarsMacCore

final class NotebookPersistenceTests: XCTestCase {
    func testRustBackedNotebookCreateSaveConflictAndReceiptRoundTrip() throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-mac-notebook-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }

        let client = UniFFINotebookPersistenceClient()
        var notebook = try client.create(
            projectRoot: root.path,
            filename: nil,
            title: "GUI notebook"
        )
        XCTAssertEqual(notebook.filename, "default.md")
        XCTAssertTrue(notebook.source.contains("# CASA-RS notebook"))

        notebook.draftSource += "\nA note written in the GUI.\n"
        guard case let .saved(saved) = try client.save(
            projectRoot: root.path,
            document: notebook,
            resolution: .reject
        ) else {
            return XCTFail("expected a saved notebook")
        }
        XCTAssertTrue(saved.source.contains("A note written in the GUI."))

        let path = root.appendingPathComponent("notebooks/default.md")
        try (saved.source + "\nExternal note.\n").write(to: path, atomically: false, encoding: .utf8)
        var local = saved
        local.draftSource += "\nLocal note.\n"
        guard case let .conflict(conflict) = try client.save(
            projectRoot: root.path,
            document: local,
            resolution: .reject
        ) else {
            return XCTFail("expected external conflict")
        }
        XCTAssertTrue(conflict.external.source.contains("External note."))
        XCTAssertTrue(conflict.proposedSource.contains("Local note."))

        let start = try client.beginRecording(request: NotebookBeginRecordingRequest(
            projectRoot: root.path,
            policy: "record",
            request: NotebookRecordingRequest(
                initiatingSurface: "gui",
                operationId: "imager",
                notebookId: saved.id,
                cellId: nil,
                taskIntent: NotebookTaskIntent(
                    format: 1,
                    surface: "imager",
                    kind: "task",
                    contract: 1,
                    parameters: ["niter": .number(5)]
                ),
                providerContractVersion: 1,
                resolvedParameters: ["niter": .number(5)],
                runSafety: NotebookRunSafetyRecord(
                    classification: "product_write",
                    affectedPaths: ["products/test.image"]
                ),
                approvals: []
            )
        ))
        let handle = try XCTUnwrap(start.handle)
        try client.finalizeRecording(request: NotebookFinalizeRecordingRequest(
            projectRoot: root.path,
            handle: handle,
            finalization: NotebookReceiptFinalization(
                status: "succeeded",
                finishedAt: UInt64(Date().timeIntervalSince1970 * 1_000),
                affectedPaths: ["products/test.image"],
                products: [NotebookReceiptArtifact(
                    role: "image",
                    path: "products/test.image",
                    mediaType: nil
                )],
                artifacts: [],
                diagnostics: [],
                stdout: Array("ok".utf8),
                stderr: [],
                casaLog: nil
            )
        ))

        let reloaded = try client.loadProject(projectRoot: root.path)
        let receipt = try XCTUnwrap(
            reloaded.notebooks.first(where: { $0.id == saved.id })?.receipts.first
        )
        XCTAssertEqual(receipt.status, "succeeded")
        XCTAssertEqual(receipt.sparseIntent?.parameters["niter"], .number(5))
        XCTAssertEqual(receipt.products.first?.path, "products/test.image")
    }

    func testOneRunBypassDoesNotCreateAReceipt() throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-mac-notebook-bypass-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }

        let client = UniFFINotebookPersistenceClient()
        let result = try client.beginRecording(request: NotebookBeginRecordingRequest(
            projectRoot: root.path,
            policy: "bypass_once",
            request: NotebookRecordingRequest(
                initiatingSurface: "gui",
                operationId: "imstat",
                notebookId: nil,
                cellId: nil,
                taskIntent: nil,
                providerContractVersion: 1,
                resolvedParameters: [:],
                runSafety: NotebookRunSafetyRecord(classification: "read_only", affectedPaths: []),
                approvals: []
            )
        ))
        XCTAssertNil(result.handle)
        XCTAssertTrue(try client.loadProject(projectRoot: root.path).notebooks.isEmpty)
    }
}
