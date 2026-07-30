import CryptoKit
import Foundation
import XCTest
@testable import CasarsMacCore

final class TutorialPersistenceTests: XCTestCase {
    func testBundledTWHyaTutorialLoadsEveryGuideTaskAgainstCanonicalSurfaces() throws {
        let packageRoot = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        let template = packageRoot
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .appendingPathComponent("resources/tutorials/tw-hya-first-look", isDirectory: true)
        let project = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-tutorial-template-\(UUID().uuidString)", isDirectory: true)
        defer { try? FileManager.default.removeItem(at: project) }
        try FileManager.default.createDirectory(at: project, withIntermediateDirectories: true)

        let forked = try UniFFITutorialPersistenceClient().fork(
            projectRoot: project.path,
            templatePath: template.path,
            filename: "tw-hya-first-look.md"
        )
        let cells = forked.notebook.cells
        XCTAssertEqual(cells.map(\.taskIntent?.surface), [
            "imager",
            "imager",
            "split",
            "imager",
            "impbcor",
        ])
        XCTAssertEqual(forked.tutorial.sections.count, 9)

        let parameterClient = UniFFISurfaceParameterClient()
        for cell in cells {
            let intent = try XCTUnwrap(cell.taskIntent)
            _ = try parameterClient.load(
                surfaceID: intent.surface,
                profileTOML: intent.profileTOML,
                sourcePath: "\(project.path)/notebooks/tw-hya-first-look.md#\(cell.id)"
            )
        }

        let targetImage = try XCTUnwrap(
            cells.first { $0.id == "019f6666-6666-7666-8666-666666666666" }?.taskIntent
        )
        let targetSnapshot = try parameterClient.load(
            surfaceID: targetImage.surface,
            profileTOML: targetImage.profileTOML,
            sourcePath: "tw-hya-first-look.md#\(cells[3].id)"
        )
        XCTAssertEqual(targetSnapshot.states["vis"]?.value, .array([.string("data/twhya_smoothed.ms")]))
        XCTAssertEqual(targetSnapshot.states["field"]?.value, .string("0"))
        XCTAssertEqual(targetSnapshot.states["niter"]?.value, .integer(10_000))
        XCTAssertEqual(targetSnapshot.states["threshold"]?.value, .string("0.015Jy"))
        XCTAssertEqual(targetSnapshot.states["mask_box"]?.value, .string("100,100,150,150"))
    }

    func testRustBackedTemplateForkAcquisitionAndOfflineReopenRoundTrip() throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-tutorial-swift-\(UUID().uuidString)", isDirectory: true)
        let project = root.appendingPathComponent("project", isDirectory: true)
        let template = root.appendingPathComponent("template", isDirectory: true)
        defer { try? FileManager.default.removeItem(at: root) }
        try FileManager.default.createDirectory(at: project, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: template, withIntermediateDirectories: true)
        let bytes = Data("swift tutorial source".utf8)
        let source = template.appendingPathComponent("source.bin")
        try bytes.write(to: source)
        let digest = SHA256.hash(data: bytes).map { String(format: "%02x", $0) }.joined()
        try """
        # Swift tutorial

        Learner notes stay editable.

        <!-- casa-rs-cell:v1 id=019f5555-5555-7555-8555-555555555555 kind=task -->
        ```toml
        [casars]
        format = 1
        surface = "imager"
        kind = "task"
        contract = 1

        [parameters]
        vis = "data/science.bin"
        imagename = "products/science"
        weighting = "briggs"
        robust = -0.5
        ```
        <!-- /casa-rs-cell -->
        """.write(to: template.appendingPathComponent("tutorial.md"), atomically: true, encoding: .utf8)
        try """
        schema_version = 1
        tutorial_id = "swift-roundtrip"
        title = "Swift roundtrip"

        [[datasets]]
        id = "science"
        display_name = "Science input"
        uri = "file://\(source.path)"
        destination = "data/science.bin"
        expected_size_bytes = \(bytes.count)
        sha256 = "\(digest)"

        [[datasets.checks]]
        id = "regular-file"
        label = "Regular file"
        kind = "regular_file"
        path = ""

        [[sections]]
        id = "run"
        title = "Run"
        dataset_ids = ["science"]
        cell_ids = ["019f5555-5555-7555-8555-555555555555"]
        """.write(to: template.appendingPathComponent("tutorial.toml"), atomically: true, encoding: .utf8)

        let client = UniFFITutorialPersistenceClient()
        let forked = try client.fork(
            projectRoot: project.path,
            templatePath: template.path,
            filename: "Learner.md"
        )
        XCTAssertTrue(forked.notebook.source.contains("Learner notes stay editable."))
        XCTAssertEqual(forked.notebook.cells.first?.taskIntent?.surface, "imager")
        XCTAssertEqual(forked.tutorial.datasets.first?.phase, .missing)

        let plan = try client.plan(
            projectRoot: project.path,
            notebookID: forked.tutorial.notebookId,
            datasetID: "science",
            sourceOverride: nil
        )
        XCTAssertEqual(plan.expectedSha256, digest)
        XCTAssertEqual(plan.destination, "data/science.bin")
        var state = try client.begin(
            projectRoot: project.path,
            plan: plan,
            approval: TutorialAcquisitionApprovalState(
                approvalSha256: plan.approvalSha256,
                allowMissingDigest: false,
                skippedCheckIds: []
            )
        )
        XCTAssertFalse(FileManager.default.fileExists(atPath: project.appendingPathComponent("data/science.bin").path))
        for _ in 0..<12 where state.phase != .ready {
            state = try client.action(
                .advance,
                projectRoot: project.path,
                notebookID: forked.tutorial.notebookId,
                datasetID: "science",
                generation: state.currentGeneration
            )
        }
        XCTAssertEqual(state.phase, .ready)
        XCTAssertTrue(state.staged)
        XCTAssertEqual(try Data(contentsOf: project.appendingPathComponent("data/science.bin")), bytes)
        XCTAssertEqual(state.currentAttempt?.checks.first?.status, "passed")

        let taskIntent = try XCTUnwrap(forked.notebook.cells.first?.taskIntent)
        let taskSnapshot = try UniFFISurfaceParameterClient().load(
            surfaceID: taskIntent.surface,
            profileTOML: taskIntent.profileTOML,
            sourcePath: "\(project.path)/notebooks/Learner.md#\(forked.notebook.cells[0].id)"
        )
        XCTAssertEqual(
            taskSnapshot.states["vis"]?.value,
            .array([.string("data/science.bin")])
        )
        XCTAssertEqual(taskSnapshot.states["robust"]?.value, .float(-0.5))

        try FileManager.default.removeItem(at: source)
        let reopened = try client.list(projectRoot: project.path)
        XCTAssertEqual(reopened.first?.tutorial.datasets.first?.phase, .ready)
        XCTAssertEqual(reopened.first?.notebook.receipts.first?.operationId, "tutorial.acquire.science")
    }
}
