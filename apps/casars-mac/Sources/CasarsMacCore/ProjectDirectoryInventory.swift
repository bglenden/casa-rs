import Foundation
import CasarsFrontendServices

/// Transient entries from one bounded frontend directory walk.
public struct ProjectDirectoryEntry: Equatable {
    public var path: String
    public var relativePath: String
    public var isDirectory: Bool
    public var sizeBytes: UInt64
    public var dataset: DatasetSummary?
    public var showInTree: Bool
    public var looseFileCandidate: Bool

    public init(path: String, relativePath: String, isDirectory: Bool, sizeBytes: UInt64, dataset: DatasetSummary? = nil, showInTree: Bool = true, looseFileCandidate: Bool = true) {
        self.path = path
        self.relativePath = relativePath
        self.isDirectory = isDirectory
        self.sizeBytes = sizeBytes
        self.dataset = dataset
        self.showInTree = showInTree
        self.looseFileCandidate = looseFileCandidate
    }

    init(_ entry: CasarsFrontendServices.ProjectFileEntry) {
        self.init(path: entry.path, relativePath: entry.relativePath, isDirectory: entry.isDirectory,
                  sizeBytes: entry.sizeBytes, dataset: entry.dataset.map(DatasetSummary.init(probe:)),
                  showInTree: entry.showInTree, looseFileCandidate: entry.looseFileCandidate)
    }
}

/// A presentation tree built solely from the cached inventory, never from disk.
public struct ProjectFileNode: Identifiable, Hashable {
    public let id: String
    public let name: String
    public let path: String
    public let relativePath: String
    public let isDirectory: Bool
    public let sizeBytes: Int?
    public let children: [ProjectFileNode]?

    public static func build(entries: [ProjectDirectoryEntry]) -> [ProjectFileNode] {
        let byParent = Dictionary(grouping: entries.filter(\.showInTree)) { entry in
            (entry.relativePath as NSString).deletingLastPathComponent
        }
        func children(_ parent: String) -> [ProjectFileNode] {
            (byParent[parent] ?? []).sorted { left, right in
                if left.isDirectory != right.isDirectory { return left.isDirectory }
                return (left.path as NSString).lastPathComponent.localizedStandardCompare(
                    (right.path as NSString).lastPathComponent
                ) == .orderedAscending
            }.map { entry in
                ProjectFileNode(
                    id: entry.path, name: (entry.path as NSString).lastPathComponent,
                    path: entry.path, relativePath: entry.relativePath, isDirectory: entry.isDirectory,
                    sizeBytes: Int(exactly: entry.sizeBytes),
                    children: entry.isDirectory && entry.dataset == nil ? children(entry.relativePath) : nil
                )
            }
        }
        return children("")
    }
}
