import Foundation
import XCTest
@testable import CasarsMacCore

final class ProjectItemRemovalTests: XCTestCase {
    func testImmediateDeletionRemovesOnlyAnItemInsideTheProject() throws {
        let project = try makeProject()
        defer { try? FileManager.default.removeItem(at: project) }
        let item = project.appendingPathComponent("large.ms", isDirectory: true)
        try FileManager.default.createDirectory(at: item, withIntermediateDirectories: true)

        try FileManagerProjectItemRemovalClient().remove(
            ProjectItemRemovalTarget(
                id: item.path,
                name: item.lastPathComponent,
                path: item.path,
                kind: .dataset
            ),
            fromProjectRoot: project.path,
            mode: .deleteImmediately
        )

        XCTAssertFalse(FileManager.default.fileExists(atPath: item.path))
        XCTAssertTrue(FileManager.default.fileExists(atPath: project.path))
    }

    func testRemovalRejectsProjectRootManagedStateAndOutsidePaths() throws {
        let project = try makeProject()
        let outside = try makeProject()
        defer {
            try? FileManager.default.removeItem(at: project)
            try? FileManager.default.removeItem(at: outside)
        }
        let managed = project.appendingPathComponent(".casa-rs/notebook.lock")
        try FileManager.default.createDirectory(
            at: managed.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        try Data().write(to: managed)
        let client = FileManagerProjectItemRemovalClient()

        XCTAssertThrowsError(try client.remove(
            target(project, kind: .folder),
            fromProjectRoot: project.path,
            mode: .deleteImmediately
        )) { error in
            XCTAssertEqual(error as? ProjectItemRemovalError, .unsafeTarget(project.path))
        }
        XCTAssertThrowsError(try client.remove(
            target(managed, kind: .file),
            fromProjectRoot: project.path,
            mode: .deleteImmediately
        )) { error in
            XCTAssertEqual(
                error as? ProjectItemRemovalError,
                .managedState(".casa-rs/notebook.lock")
            )
        }
        XCTAssertThrowsError(try client.remove(
            target(outside, kind: .folder),
            fromProjectRoot: project.path,
            mode: .deleteImmediately
        )) { error in
            XCTAssertEqual(error as? ProjectItemRemovalError, .outsideProject(outside.path))
        }

        XCTAssertTrue(FileManager.default.fileExists(atPath: project.path))
        XCTAssertTrue(FileManager.default.fileExists(atPath: managed.path))
        XCTAssertTrue(FileManager.default.fileExists(atPath: outside.path))
    }

    private func makeProject() throws -> URL {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-removal-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
        return url
    }

    private func target(_ url: URL, kind: ProjectItemKind) -> ProjectItemRemovalTarget {
        ProjectItemRemovalTarget(
            id: url.path,
            name: url.lastPathComponent,
            path: url.path,
            kind: kind
        )
    }
}
