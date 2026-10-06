import XCTest
@testable import CasarsMacCore

final class ProjectDirectoryInventoryTests: XCTestCase {
    func testTreeUsesOnlyProvidedSnapshotWithoutReadingPaths() {
        let entries = [
            ProjectDirectoryEntry(path: "/nonexistent/notes.txt", relativePath: "notes.txt", isDirectory: false, sizeBytes: 12),
            ProjectDirectoryEntry(path: "/nonexistent/results", relativePath: "results", isDirectory: true, sizeBytes: 0),
            ProjectDirectoryEntry(path: "/nonexistent/results/map.fits", relativePath: "results/map.fits", isDirectory: false, sizeBytes: 42),
            ProjectDirectoryEntry(path: "/nonexistent/.hidden", relativePath: ".hidden", isDirectory: false, sizeBytes: 1, looseFileCandidate: false),
            ProjectDirectoryEntry(path: "/nonexistent/beyond-limit", relativePath: "beyond-limit", isDirectory: false, sizeBytes: 1, showInTree: false),
        ]
        let nodes = ProjectFileNode.build(entries: entries)
        XCTAssertEqual(nodes.first?.name, "results")
        XCTAssertEqual(nodes.first?.children?.first?.name, "map.fits")
        XCTAssertEqual(nodes.first?.children?.first?.sizeBytes, 42)
        XCTAssertTrue(nodes.contains { $0.name == ".hidden" })
        XCTAssertFalse(nodes.contains { $0.name == "beyond-limit" })
        XCTAssertEqual(ProjectFileNode.build(entries: entries), nodes)
        XCTAssertTrue(ProjectFileNode.build(entries: []).isEmpty)
    }
}
