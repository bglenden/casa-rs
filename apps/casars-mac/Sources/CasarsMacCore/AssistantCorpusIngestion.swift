import AppKit
import CryptoKit
import Darwin
import Foundation
import PDFKit
import Vision

package struct AssistantCorpusIngestionResult {
    package var documents: [AssistantCorpusDocumentRequest]
    package var diagnostics: [String]
    package var refreshedLayers: Set<String>
    package var projectSources: [AssistantProjectCorpusSourceRequest]
    package var failedProjectSources: Set<String>
    package var metrics: AssistantCorpusRefreshMetricsState
}

package struct AssistantProjectCorpusInventory {
    package var sources: [AssistantProjectCorpusSourceRequest]
    package var diagnostics: [String]
    package var metrics: AssistantCorpusRefreshMetricsState
}

package enum AssistantCorpusRefreshScope: Equatable {
    case allLayers
    case projectDocuments
}

package struct AssistantCorpusIngestor {
    /// Local Vision bitmap envelope; extracted text is not truncated to this size.
    private static let ocrMaximumScale: CGFloat = 2
    private static let ocrMaximumDimension: CGFloat = 4_000
    private let fileManager = FileManager.default

    package init() {}

    package func collect(
        projectRoot: String,
        environment: [String: String] = ProcessInfo.processInfo.environment,
        projectInventory: AssistantProjectCorpusInventory,
        extractProjectPaths: Set<String>,
        scope: AssistantCorpusRefreshScope
    ) -> AssistantCorpusIngestionResult {
        let project = URL(fileURLWithPath: projectRoot).standardizedFileURL
        var documents: [AssistantCorpusDocumentRequest] = []
        var diagnostics = projectInventory.diagnostics
        var metrics = projectInventory.metrics
        var failedProjectSources: Set<String> = []
        var refreshedLayers: Set<String> = []

        collectProjectDocuments(
            project: project,
            inventory: projectInventory.sources,
            extractPaths: extractProjectPaths,
            documents: &documents,
            diagnostics: &diagnostics,
            failedSources: &failedProjectSources,
            metrics: &metrics
        )

        if scope == .allLayers {
            refreshedLayers.formUnion(["release_source", "live_source"])
            if let baseline = baselineRoot(environment: environment) {
                collectBaseline(
                    root: baseline,
                    documents: &documents,
                    diagnostics: &diagnostics
                )
                refreshedLayers.insert("baseline")
            } else {
                diagnostics.append("No redistribution-cleared CASA-RS baseline corpus pack is installed.")
            }
            if let source = sourceRoot(environment: environment) {
                let gitCommit = gitValue(source, arguments: ["rev-parse", "HEAD"])
                let manifest = sourceManifest(source)
                let commit = gitCommit.map {
                    gitWorkingTreeIsDirty(source) ? "\($0)+dirty" : $0
                } ?? manifest?.commit
                let release = gitValue(source, arguments: ["describe", "--tags", "--always"])
                    ?? manifest?.release
                let layer = gitCommit == nil ? "release_source" : "live_source"
                collectSourceTree(
                    root: source,
                    layer: layer,
                    release: release,
                    commit: commit,
                    documents: &documents,
                    diagnostics: &diagnostics
                )
            } else {
                diagnostics.append(
                    "CASA-RS source corpus unavailable; set CASA_RS_SOURCE_ROOT or install bundled source metadata."
                )
            }
        }

        return AssistantCorpusIngestionResult(
            documents: documents,
            diagnostics: diagnostics,
            refreshedLayers: refreshedLayers,
            projectSources: projectInventory.sources,
            failedProjectSources: failedProjectSources,
            metrics: metrics
        )
    }

    package func projectDocumentInventory(projectRoot: String) -> AssistantProjectCorpusInventory {
        let project = URL(fileURLWithPath: projectRoot).standardizedFileURL
        let root = project.appendingPathComponent("documents", isDirectory: true)
        var diagnostics: [String] = []
        var metrics = AssistantCorpusRefreshMetricsState()
        guard let rootValues = try? root.resourceValues(forKeys: [.isDirectoryKey, .isSymbolicLinkKey]),
              rootValues.isDirectory == true,
              rootValues.isSymbolicLink != true
        else {
            return AssistantProjectCorpusInventory(
                sources: [], diagnostics: diagnostics, metrics: metrics
            )
        }
        let keys: [URLResourceKey] = [.isRegularFileKey, .isDirectoryKey, .isSymbolicLinkKey]
        let options: FileManager.DirectoryEnumerationOptions = [.skipsHiddenFiles, .skipsPackageDescendants]
        guard let enumerator = fileManager.enumerator(
            at: root,
            includingPropertiesForKeys: keys,
            options: options,
            errorHandler: { url, error in
                diagnostics.append("Could not inspect project corpus path \(url.path): \(error.localizedDescription)")
                return true
            }
        ) else {
            return AssistantProjectCorpusInventory(
                sources: [], diagnostics: diagnostics, metrics: metrics
            )
        }
        var sources: [AssistantProjectCorpusSourceRequest] = []
        for case let url as URL in enumerator {
            guard let values = try? url.resourceValues(forKeys: Set(keys)) else {
                diagnostics.append("Could not inspect project corpus path \(relativePath(url, root: root))")
                continue
            }
            if values.isSymbolicLink == true {
                enumerator.skipDescendants()
                diagnostics.append("Skipped symbolic-link corpus entry documents/\(relativePath(url, root: root))")
                continue
            }
            guard values.isRegularFile == true else { continue }
            let relative = "documents/\(relativePath(url, root: root))"
            guard supportedExtension(url.pathExtension) else {
                diagnostics.append("Unsupported corpus file type \(relative)")
                continue
            }
            metrics.projectMetadataReads += 1
            guard let source = projectSourceMetadata(url: url, relativePath: relative) else {
                diagnostics.append("Could not inspect project corpus metadata \(relative)")
                continue
            }
            sources.append(source)
        }
        sources.sort { $0.relativePath < $1.relativePath }
        return AssistantProjectCorpusInventory(
            sources: sources,
            diagnostics: diagnostics,
            metrics: metrics
        )
    }

    private func collectProjectDocuments(
        project: URL,
        inventory: [AssistantProjectCorpusSourceRequest],
        extractPaths: Set<String>,
        documents: inout [AssistantCorpusDocumentRequest],
        diagnostics: inout [String],
        failedSources: inout Set<String>,
        metrics: inout AssistantCorpusRefreshMetricsState
    ) {
        let sources = Dictionary(uniqueKeysWithValues: inventory.map { ($0.relativePath, $0) })
        for path in extractPaths.sorted() {
            guard let source = sources[path] else {
                diagnostics.append("Project corpus plan referenced missing source \(path)")
                continue
            }
            let unresolvedURL = project.appendingPathComponent(path).standardizedFileURL
            let resolvedURL = unresolvedURL.resolvingSymlinksInPath()
            guard resolvedURL.path.hasPrefix(project.path + "/"),
                  resolvedURL.path == unresolvedURL.path,
                  fileManager.isReadableFile(atPath: unresolvedURL.path)
            else {
                diagnostics.append("Project corpus source became unreadable or symbolic-linked \(path)")
                failedSources.insert(path)
                continue
            }
            let firstDocument = documents.count
            metrics.projectContentReads += 1
            if source.fileType.lowercased() == "pdf" {
                metrics.projectPDFExtractions += 1
                let extraction = extractPDF(unresolvedURL, relative: path, diagnostics: &diagnostics)
                metrics.projectOCRCalls += extraction.ocrCalls
                for (page, content) in extraction.pages {
                    documents.append(document(
                        id: "project_document:\(path)#page=\(page)",
                        layer: "project_document",
                        title: unresolvedURL.deletingPathExtension().lastPathComponent,
                        relative: path,
                        content: content,
                        page: UInt32(page),
                        release: nil,
                        commit: nil,
                        redistributionCleared: false
                    ))
                }
            } else if let data = try? Data(contentsOf: unresolvedURL),
                      let content = String(data: data, encoding: .utf8),
                      !content.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            {
                documents.append(document(
                    id: "project_document:\(path)",
                    layer: "project_document",
                    title: unresolvedURL.lastPathComponent,
                    relative: path,
                    content: content,
                    page: nil,
                    release: nil,
                    commit: nil,
                    redistributionCleared: false
                ))
            } else {
                diagnostics.append("No UTF-8 text extracted from \(path)")
            }
            metrics.projectMetadataReads += 1
            let current = projectSourceMetadata(url: unresolvedURL, relativePath: path)
            if current != source {
                documents.removeSubrange(firstDocument..<documents.count)
                diagnostics.append("Project corpus source changed while being read; retrying \(path)")
                failedSources.insert(path)
            } else if documents.count == firstDocument {
                failedSources.insert(path)
            }
        }
    }

    private func projectSourceMetadata(
        url: URL,
        relativePath: String
    ) -> AssistantProjectCorpusSourceRequest? {
        var metadata = stat()
        guard lstat(url.path, &metadata) == 0,
              metadata.st_mode & S_IFMT == S_IFREG
        else { return nil }
        let modified = Int64(metadata.st_mtimespec.tv_sec) * 1_000_000_000
            + Int64(metadata.st_mtimespec.tv_nsec)
        let changed = Int64(metadata.st_ctimespec.tv_sec) * 1_000_000_000
            + Int64(metadata.st_ctimespec.tv_nsec)
        return AssistantProjectCorpusSourceRequest(
            relativePath: relativePath,
            fileType: url.pathExtension.lowercased(),
            sizeBytes: UInt64(max(metadata.st_size, 0)),
            modifiedUnixNs: modified,
            statusChangedUnixNs: changed,
            fileIdentity: "\(metadata.st_dev):\(metadata.st_ino)"
        )
    }

    private func collectBaseline(
        root: URL,
        documents: inout [AssistantCorpusDocumentRequest],
        diagnostics: inout [String]
    ) {
        guard let rootValues = try? root.resourceValues(forKeys: [.isDirectoryKey, .isSymbolicLinkKey]),
              rootValues.isDirectory == true,
              rootValues.isSymbolicLink != true
        else {
            diagnostics.append("Skipped symbolic-link or invalid baseline root \(root.path)")
            return
        }
        let manifestURL = root.appendingPathComponent("corpus-pack.json")
        guard let manifestValues = try? manifestURL.resourceValues(forKeys: [.isSymbolicLinkKey]),
              manifestValues.isSymbolicLink != true,
              let data = try? Data(contentsOf: manifestURL),
              let manifest = try? JSONDecoder().decode(AssistantBaselineManifest.self, from: data),
              manifest.schemaVersion == AssistantBaselineManifest.currentSchemaVersion
        else {
            diagnostics.append("Baseline corpus manifest is missing or unsupported at \(manifestURL.path)")
            return
        }
        let firstBaselineDocument = documents.count
        var availableSources = 0
        for entry in manifest.documents {
            let canonicalRoot = root.standardizedFileURL.resolvingSymlinksInPath()
            let unresolvedURL = root.appendingPathComponent(entry.path).standardizedFileURL
            let url = unresolvedURL.resolvingSymlinksInPath()
            let values = try? unresolvedURL.resourceValues(forKeys: [.isSymbolicLinkKey])
            guard url.path.hasPrefix(canonicalRoot.path + "/"),
                  values?.isSymbolicLink != true,
                  fileManager.isReadableFile(atPath: url.path),
                  entry.path.hasPrefix("standard-v1/"),
                  !entry.path.split(separator: "/").contains(".."),
                  entry.format == "normalized_pages_json",
                  entry.hasRequiredProvenance
            else {
                diagnostics.append("Skipped invalid baseline path \(entry.path)")
                continue
            }
            guard let contentData = try? Data(contentsOf: url) else {
                diagnostics.append("Could not read baseline document \(entry.path)")
                continue
            }
            if Self.sha256(contentData) != entry.contentSHA256 {
                diagnostics.append("Skipped baseline document with mismatched digest \(entry.path)")
                continue
            }
            let sourcePath = entry.sourcePath
            let citationPath = "baseline/\(manifest.id)/\(sourcePath)"
            let firstEntryDocument = documents.count
            guard let pages = try? JSONDecoder().decode([AssistantBaselinePage].self, from: contentData),
                  !pages.isEmpty,
                  pages.enumerated().allSatisfy({ offset, page in
                      page.page == offset + 1
                          && !page.content.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
                  })
            else {
                diagnostics.append("Could not decode contiguous baseline pages \(entry.path)")
                continue
            }
            for page in pages {
                let locatorKind = entry.citationKind == "slide" ? "slide" : "page"
                documents.append(AssistantCorpusDocumentRequest(
                    id: "baseline:\(manifest.id):\(manifest.version):\(entry.path)#page=\(page.page)",
                    layer: "baseline",
                    title: entry.title,
                    sourceIdentity: "\(manifest.id)@\(manifest.version):\(entry.sourceSHA256)",
                    content: page.content,
                    citation: AssistantCorpusCitationRequest(
                        label: entry.citationLabel,
                        locator: "\(entry.citationLabel), \(locatorKind) \(page.page)",
                        sourcePath: citationPath,
                        page: UInt32(page.page),
                        section: nil,
                        lineStart: nil,
                        lineEnd: nil,
                        release: manifest.version,
                        commit: nil
                    ),
                    redistributionCleared: true
                ))
            }
            if documents.count > firstEntryDocument { availableSources += 1 }
        }
        diagnostics.append(
            "Installed baseline \(manifest.id)@\(manifest.version): "
                + "\(availableSources)/\(manifest.documents.count) sources available, "
                + "\(documents.count - firstBaselineDocument) cited documents."
        )
    }

    private static func sha256(_ data: Data) -> String {
        SHA256.hash(data: data).map { String(format: "%02x", $0) }.joined()
    }

    private func collectSourceTree(
        root: URL,
        layer: String,
        release: String?,
        commit: String?,
        documents: inout [AssistantCorpusDocumentRequest],
        diagnostics: inout [String]
    ) {
        collectTree(
            root: root,
            identityRoot: root,
            identityPrefix: nil,
            layer: layer,
            release: release,
            commit: commit,
            redistributionCleared: true,
            documents: &documents,
            diagnostics: &diagnostics
        )
    }

    private func collectTree(
        root: URL,
        identityRoot: URL,
        identityPrefix: String?,
        layer: String,
        release: String?,
        commit: String?,
        redistributionCleared: Bool,
        documents: inout [AssistantCorpusDocumentRequest],
        diagnostics: inout [String]
    ) {
        guard let rootValues = try? root.resourceValues(forKeys: [.isDirectoryKey, .isSymbolicLinkKey]),
              rootValues.isDirectory == true,
              rootValues.isSymbolicLink != true
        else {
            diagnostics.append("Skipped symbolic-link or invalid corpus root \(root.path)")
            return
        }
        let keys: [URLResourceKey] = [
            .isRegularFileKey, .isDirectoryKey, .isSymbolicLinkKey, .fileSizeKey,
        ]
        let options: FileManager.DirectoryEnumerationOptions = [.skipsHiddenFiles, .skipsPackageDescendants]
        guard let enumerator = fileManager.enumerator(
            at: root,
            includingPropertiesForKeys: keys,
            options: options,
            errorHandler: nil
        ) else { return }
        for case let url as URL in enumerator {
            if shouldSkipDirectory(url) {
                enumerator.skipDescendants()
                continue
            }
            guard let values = try? url.resourceValues(forKeys: Set(keys)) else { continue }
            if values.isSymbolicLink == true {
                enumerator.skipDescendants()
                diagnostics.append("Skipped symbolic-link corpus entry \(relativePath(url, root: identityRoot))")
                continue
            }
            guard values.isRegularFile == true else { continue }
            let treeRelative = relativePath(url, root: identityRoot)
            let relative = identityPrefix.map { "\($0)/\(treeRelative)" } ?? treeRelative
            guard supportedExtension(url.pathExtension) else {
                if layer == "project_document" {
                    diagnostics.append("Unsupported corpus file type \(relative)")
                }
                continue
            }
            if url.pathExtension.lowercased() == "pdf" {
                let extraction = extractPDF(url, relative: relative, diagnostics: &diagnostics)
                for (page, content) in extraction.pages {
                    documents.append(document(
                        id: "\(layer):\(relative)#page=\(page)",
                        layer: layer,
                        title: url.deletingPathExtension().lastPathComponent,
                        relative: relative,
                        content: content,
                        page: UInt32(page),
                        release: release,
                        commit: commit,
                        redistributionCleared: redistributionCleared
                    ))
                }
                continue
            }
            guard let data = try? Data(contentsOf: url),
                  let content = String(data: data, encoding: .utf8),
                  !content.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            else {
                diagnostics.append("No UTF-8 text extracted from \(relative)")
                continue
            }
            documents.append(document(
                id: "\(layer):\(relative)",
                layer: layer,
                title: url.lastPathComponent,
                relative: relative,
                content: content,
                page: nil,
                release: release,
                commit: commit,
                redistributionCleared: redistributionCleared
            ))
        }
    }

    private func extractPDF(
        _ url: URL,
        relative: String,
        diagnostics: inout [String]
    ) -> (pages: [(Int, String)], ocrCalls: Int) {
        guard let pdf = PDFDocument(url: url) else {
            diagnostics.append("Could not open PDF \(relative)")
            return ([], 0)
        }
        var pages: [(Int, String)] = []
        var ocrCalls = 0
        for index in 0..<pdf.pageCount {
            guard let page = pdf.page(at: index) else { continue }
            var text = page.string?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
            if text.isEmpty {
                ocrCalls += 1
                text = recognizeText(page: page)
                if !text.isEmpty {
                    diagnostics.append("Used local Vision OCR for \(relative), page \(index + 1)")
                }
            }
            if !text.isEmpty { pages.append((index + 1, text)) }
        }
        if pages.isEmpty { diagnostics.append("No text extracted from PDF \(relative)") }
        return (pages, ocrCalls)
    }

    private func recognizeText(page: PDFPage) -> String {
        let bounds = page.bounds(for: .mediaBox)
        let scale = min(
            Self.ocrMaximumScale,
            Self.ocrMaximumDimension / max(max(bounds.width, bounds.height), 1)
        )
        let thumbnail = page.thumbnail(
            of: NSSize(width: max(1, bounds.width * scale), height: max(1, bounds.height * scale)),
            for: .mediaBox
        )
        guard let data = thumbnail.tiffRepresentation,
              let bitmap = NSBitmapImageRep(data: data),
              let image = bitmap.cgImage
        else { return "" }
        let request = VNRecognizeTextRequest()
        request.recognitionLevel = .accurate
        request.usesLanguageCorrection = true
        let handler = VNImageRequestHandler(cgImage: image)
        guard (try? handler.perform([request])) != nil else { return "" }
        return (request.results ?? [])
            .compactMap { $0.topCandidates(1).first?.string }
            .joined(separator: "\n")
    }

    private func document(
        id: String,
        layer: String,
        title: String,
        relative: String,
        content: String,
        page: UInt32?,
        release: String?,
        commit: String?,
        redistributionCleared: Bool
    ) -> AssistantCorpusDocumentRequest {
        AssistantCorpusDocumentRequest(
            id: id,
            layer: layer,
            title: title,
            sourceIdentity: relative,
            content: content,
            citation: AssistantCorpusCitationRequest(
                label: title,
                locator: page.map { "\(relative), page \($0)" } ?? relative,
                sourcePath: relative,
                page: page,
                section: nil,
                lineStart: nil,
                lineEnd: nil,
                release: release,
                commit: commit
            ),
            redistributionCleared: redistributionCleared
        )
    }

    private func sourceRoot(environment: [String: String]) -> URL? {
        let current = URL(fileURLWithPath: fileManager.currentDirectoryPath)
        let checkoutRoots = sequence(first: current) { url in
            let parent = url.deletingLastPathComponent()
            return parent.path == url.path ? nil : parent
        }
        .prefix(6)
        .filter { fileManager.fileExists(atPath: $0.appendingPathComponent("Cargo.toml").path) }
        let candidates = [
            environment["CASA_RS_SOURCE_ROOT"].map(URL.init(fileURLWithPath:)),
            Bundle.main.resourceURL?.appendingPathComponent("casars-source", isDirectory: true),
        ].compactMap { $0?.standardizedFileURL } + checkoutRoots
        return candidates.first(where: {
            fileManager.fileExists(atPath: $0.appendingPathComponent("ARCHITECTURE.md").path)
        })
    }

    private func baselineRoot(environment: [String: String]) -> URL? {
        let candidates = [
            environment["CASA_RS_ASSISTANT_BASELINE_ROOT"].map(URL.init(fileURLWithPath:)),
            Bundle.module.resourceURL?.appendingPathComponent("assistant-corpus", isDirectory: true),
            Bundle.main.resourceURL?.appendingPathComponent("assistant-corpus", isDirectory: true),
        ].compactMap { $0?.standardizedFileURL }
        return candidates.first(where: {
            fileManager.fileExists(atPath: $0.appendingPathComponent("corpus-pack.json").path)
        })
    }

    private func gitValue(_ root: URL, arguments: [String]) -> String? {
        guard fileManager.fileExists(atPath: root.appendingPathComponent(".git").path) else { return nil }
        let process = Process()
        let output = Pipe()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/git")
        process.arguments = ["-C", root.path] + arguments
        process.standardOutput = output
        process.standardError = Pipe()
        guard (try? process.run()) != nil else { return nil }
        process.waitUntilExit()
        guard process.terminationStatus == 0 else { return nil }
        return String(decoding: output.fileHandleForReading.readDataToEndOfFile(), as: UTF8.self)
            .trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private func gitWorkingTreeIsDirty(_ root: URL) -> Bool {
        guard let status = gitValue(
            root,
            arguments: ["status", "--porcelain", "--untracked-files=normal"]
        ) else { return false }
        return !status.isEmpty
    }

    private func supportedExtension(_ value: String) -> Bool {
        ["md", "txt", "rst", "toml", "rs", "swift", "ts", "py", "pdf"]
            .contains(value.lowercased())
    }

    private func shouldSkipDirectory(_ url: URL) -> Bool {
        [".git", ".casa-rs", "target", "node_modules", ".build"].contains(url.lastPathComponent)
    }

    private func relativePath(_ value: URL, root: URL) -> String {
        let prefix = root.path.hasSuffix("/") ? root.path : root.path + "/"
        return value.path.hasPrefix(prefix) ? String(value.path.dropFirst(prefix.count)) : value.lastPathComponent
    }

    private func sourceManifest(_ root: URL) -> AssistantSourceManifest? {
        let url = root.appendingPathComponent("casars-source.json")
        guard let values = try? url.resourceValues(forKeys: [.fileSizeKey, .isSymbolicLinkKey]),
              values.isSymbolicLink != true,
              (values.fileSize ?? Int.max) <= 1_048_576,
              let data = try? Data(contentsOf: url),
              let manifest = try? JSONDecoder().decode(AssistantSourceManifest.self, from: data),
              manifest.schemaVersion == 1
        else { return nil }
        return manifest
    }
}

private struct AssistantSourceManifest: Decodable {
    var schemaVersion: Int
    var release: String
    var commit: String

    private enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
        case release, commit
    }
}

private struct AssistantBaselineManifest: Decodable {
    static let currentSchemaVersion = 3

    var schemaVersion: Int
    var id: String
    var version: String
    var documents: [AssistantBaselineDocument]

    private enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
        case id, version, documents
    }
}

private struct AssistantBaselineDocument: Decodable {
    var path: String
    var format: String
    var title: String
    var citationLabel: String
    var citationKind: String
    var sourcePath: String
    var contentSHA256: String
    var sourceSHA256: String
    var originURL: String
    var license: AssistantBaselineLicense
    var redistributionBasis: String
    var contributors: [String]
    var modifications: String

    var hasRequiredProvenance: Bool {
        contentSHA256.count == 64
            && sourceSHA256.count == 64
            && !originURL.isEmpty
            && !license.id.isEmpty
            && !license.name.isEmpty
            && !license.url.isEmpty
            && !redistributionBasis.isEmpty
            && !contributors.isEmpty
            && !modifications.isEmpty
    }

    private enum CodingKeys: String, CodingKey {
        case path, format, title, license, contributors, modifications
        case citationLabel = "citation_label"
        case citationKind = "citation_kind"
        case sourcePath = "source_path"
        case contentSHA256 = "content_sha256"
        case sourceSHA256 = "source_sha256"
        case originURL = "origin_url"
        case redistributionBasis = "redistribution_basis"
    }
}

private struct AssistantBaselineLicense: Decodable {
    var id: String
    var name: String
    var url: String
}

private struct AssistantBaselinePage: Decodable {
    var page: Int
    var content: String
}
