import Foundation

package enum ProjectItemKind: String, Equatable {
    case notebook
    case dataset
    case file
    case folder
}

package struct ProjectItemRemovalTarget: Identifiable, Equatable {
    package let id: String
    package let name: String
    package let path: String
    package let kind: ProjectItemKind
    package let sizeBytes: UInt64?

    package init(
        id: String,
        name: String,
        path: String,
        kind: ProjectItemKind,
        sizeBytes: UInt64? = nil
    ) {
        self.id = id
        self.name = name
        self.path = path
        self.kind = kind
        self.sizeBytes = sizeBytes
    }
}

package enum ProjectItemRemovalMode: Equatable {
    case trash
    case deleteImmediately
}

package protocol ProjectItemRemovalClient {
    func remove(
        _ target: ProjectItemRemovalTarget,
        fromProjectRoot projectRoot: String,
        mode: ProjectItemRemovalMode
    ) throws
}

package struct FileManagerProjectItemRemovalClient: ProjectItemRemovalClient {
    private let fileManager: FileManager

    package init(fileManager: FileManager = .default) {
        self.fileManager = fileManager
    }

    package func remove(
        _ target: ProjectItemRemovalTarget,
        fromProjectRoot projectRoot: String,
        mode: ProjectItemRemovalMode
    ) throws {
        let targetURL = try validatedTargetURL(target.path, projectRoot: projectRoot)
        guard fileManager.fileExists(atPath: targetURL.path) else {
            throw ProjectItemRemovalError.itemNotFound(targetURL.path)
        }

        switch mode {
        case .trash:
            try fileManager.trashItem(at: targetURL, resultingItemURL: nil)
        case .deleteImmediately:
            try fileManager.removeItem(at: targetURL)
        }
    }

    private func validatedTargetURL(_ path: String, projectRoot: String) throws -> URL {
        let rootURL = URL(fileURLWithPath: projectRoot, isDirectory: true).standardizedFileURL
        let targetURL = URL(fileURLWithPath: path).standardizedFileURL
        let rootPath = rootURL.path
        let targetPath = targetURL.path
        guard rootURL.isFileURL, targetURL.isFileURL,
              !rootPath.isEmpty, targetPath != rootPath
        else {
            throw ProjectItemRemovalError.unsafeTarget(path)
        }
        let prefix = rootPath.hasSuffix("/") ? rootPath : rootPath + "/"
        guard targetPath.hasPrefix(prefix) else {
            throw ProjectItemRemovalError.outsideProject(targetPath)
        }
        let relativePath = String(targetPath.dropFirst(prefix.count))
        guard relativePath.split(separator: "/").first != ".casa-rs" else {
            throw ProjectItemRemovalError.managedState(relativePath)
        }
        return targetURL
    }
}

package enum ProjectItemRemovalError: LocalizedError, Equatable {
    case unsafeTarget(String)
    case outsideProject(String)
    case managedState(String)
    case itemNotFound(String)

    package var errorDescription: String? {
        switch self {
        case let .unsafeTarget(path):
            "The project root cannot be removed: \(path)"
        case let .outsideProject(path):
            "Only items inside the open project can be removed: \(path)"
        case let .managedState(path):
            "Workbench-managed project state cannot be removed directly: \(path)"
        case let .itemNotFound(path):
            "The selected item no longer exists: \(path)"
        }
    }
}
