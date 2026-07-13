import AppKit
import Foundation
import PDFKit
import Vision

package struct AssistantCorpusIngestionResult {
    package var documents: [AssistantCorpusDocumentRequest]
    package var diagnostics: [String]
    package var refreshedLayers: Set<String>
}

package struct AssistantCorpusIngestor {
    private let fileManager = FileManager.default
    private let maximumFileBytes = 64 * 1024 * 1024
    private let maximumTotalBytes = 64 * 1024 * 1024
    private let maximumDocuments = 5_000
    private let maximumPDFPages = 2_000

    package init() {}

    package func collect(
        projectRoot: String,
        environment: [String: String] = ProcessInfo.processInfo.environment
    ) -> AssistantCorpusIngestionResult {
        let project = URL(fileURLWithPath: projectRoot).standardizedFileURL
        var documents: [AssistantCorpusDocumentRequest] = []
        var diagnostics: [String] = []
        var totalBytes = 0
        var refreshedLayers: Set<String> = ["project_document", "release_source", "live_source"]
        if let baseline = baselineRoot(environment: environment) {
            collectBaseline(
                root: baseline,
                documents: &documents,
                diagnostics: &diagnostics,
                totalBytes: &totalBytes
            )
            refreshedLayers.insert("baseline")
        } else {
            diagnostics.append("No redistribution-cleared CASA-RS baseline corpus pack is installed.")
        }
        let projectDocuments = project.appendingPathComponent("documents", isDirectory: true)
        if fileManager.fileExists(atPath: projectDocuments.path) {
            collectTree(
                root: projectDocuments,
                identityRoot: projectDocuments,
                identityPrefix: "documents",
                layer: "project_document",
                release: nil,
                commit: nil,
                redistributionCleared: false,
                documents: &documents,
                diagnostics: &diagnostics,
                totalBytes: &totalBytes
            )
        }

        if let source = sourceRoot(environment: environment) {
            let gitCommit = gitValue(source, arguments: ["rev-parse", "HEAD"])
            let manifest = sourceManifest(source)
            let commit = gitCommit ?? manifest?.commit
            let release = gitValue(source, arguments: ["describe", "--tags", "--always"])
                ?? manifest?.release
            let layer = gitCommit == nil ? "release_source" : "live_source"
            collectSourceTree(
                root: source,
                layer: layer,
                release: release,
                commit: commit,
                documents: &documents,
                diagnostics: &diagnostics,
                totalBytes: &totalBytes
            )
        } else {
            diagnostics.append(
                "CASA-RS source corpus unavailable; set CASA_RS_SOURCE_ROOT or install bundled source metadata."
            )
        }

        return AssistantCorpusIngestionResult(
            documents: Array(documents.prefix(maximumDocuments)),
            diagnostics: diagnostics,
            refreshedLayers: refreshedLayers
        )
    }

    private func collectBaseline(
        root: URL,
        documents: inout [AssistantCorpusDocumentRequest],
        diagnostics: inout [String],
        totalBytes: inout Int
    ) {
        guard let rootValues = try? root.resourceValues(forKeys: [.isDirectoryKey, .isSymbolicLinkKey]),
              rootValues.isDirectory == true,
              rootValues.isSymbolicLink != true
        else {
            diagnostics.append("Skipped symbolic-link or invalid baseline root \(root.path)")
            return
        }
        let manifestURL = root.appendingPathComponent("corpus-pack.json")
        guard let manifestValues = try? manifestURL.resourceValues(forKeys: [.fileSizeKey, .isSymbolicLinkKey]),
              manifestValues.isSymbolicLink != true,
              (manifestValues.fileSize ?? Int.max) <= 1_048_576,
              let data = try? Data(contentsOf: manifestURL),
              let manifest = try? JSONDecoder().decode(AssistantBaselineManifest.self, from: data),
              manifest.schemaVersion == 1
        else {
            diagnostics.append("Baseline corpus manifest is missing or unsupported at \(manifestURL.path)")
            return
        }
        for entry in manifest.documents {
            guard entry.redistributionCleared else {
                diagnostics.append("Skipped uncleared baseline document \(entry.path)")
                continue
            }
            let canonicalRoot = root.standardizedFileURL.resolvingSymlinksInPath()
            let unresolvedURL = root.appendingPathComponent(entry.path).standardizedFileURL
            let url = unresolvedURL.resolvingSymlinksInPath()
            let values = try? unresolvedURL.resourceValues(forKeys: [.fileSizeKey, .isSymbolicLinkKey])
            guard url.path.hasPrefix(canonicalRoot.path + "/"),
                  values?.isSymbolicLink != true,
                  (values?.fileSize ?? Int.max) <= maximumFileBytes,
                  fileManager.isReadableFile(atPath: url.path),
                  supportedExtension(url.pathExtension)
            else {
                diagnostics.append("Skipped invalid baseline path \(entry.path)")
                continue
            }
            let citationPath = "baseline/\(manifest.id)/\(entry.path)"
            if url.pathExtension.lowercased() == "pdf" {
                for (page, content) in extractPDF(url, relative: citationPath, diagnostics: &diagnostics) {
                    let bytes = content.utf8.count
                    guard totalBytes + bytes <= maximumTotalBytes else { return }
                    totalBytes += bytes
                    documents.append(AssistantCorpusDocumentRequest(
                        id: "baseline:\(manifest.id):\(manifest.version):\(entry.path)#page=\(page)",
                        layer: "baseline",
                        title: entry.title,
                        sourceIdentity: "\(manifest.id)@\(manifest.version):\(entry.path)",
                        content: content,
                        citation: AssistantCorpusCitationRequest(
                            label: entry.citationLabel,
                            locator: "\(citationPath), page \(page)",
                            sourcePath: citationPath,
                            page: UInt32(page),
                            section: nil,
                            lineStart: nil,
                            lineEnd: nil,
                            release: manifest.version,
                            commit: nil
                        ),
                        redistributionCleared: true
                    ))
                }
            } else if let data = try? Data(contentsOf: url),
                      let content = String(data: data, encoding: .utf8),
                      !content.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            {
                let bytes = content.utf8.count
                guard totalBytes + bytes <= maximumTotalBytes else { return }
                totalBytes += bytes
                documents.append(AssistantCorpusDocumentRequest(
                    id: "baseline:\(manifest.id):\(manifest.version):\(entry.path)",
                    layer: "baseline",
                    title: entry.title,
                    sourceIdentity: "\(manifest.id)@\(manifest.version):\(entry.path)",
                    content: content,
                    citation: AssistantCorpusCitationRequest(
                        label: entry.citationLabel,
                        locator: citationPath,
                        sourcePath: citationPath,
                        page: nil,
                        section: nil,
                        lineStart: nil,
                        lineEnd: nil,
                        release: manifest.version,
                        commit: nil
                    ),
                    redistributionCleared: true
                ))
            }
        }
    }

    private func collectSourceTree(
        root: URL,
        layer: String,
        release: String?,
        commit: String?,
        documents: inout [AssistantCorpusDocumentRequest],
        diagnostics: inout [String],
        totalBytes: inout Int
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
            diagnostics: &diagnostics,
            totalBytes: &totalBytes
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
        diagnostics: inout [String],
        totalBytes: inout Int
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
            guard documents.count < maximumDocuments, totalBytes < maximumTotalBytes else { return }
            guard let values = try? url.resourceValues(forKeys: Set(keys)) else { continue }
            if values.isSymbolicLink == true {
                enumerator.skipDescendants()
                diagnostics.append("Skipped symbolic-link corpus entry \(relativePath(url, root: identityRoot))")
                continue
            }
            guard
                  values.isRegularFile == true,
                  let fileSize = values.fileSize,
                  fileSize <= maximumFileBytes,
                  supportedExtension(url.pathExtension)
            else { continue }
            let treeRelative = relativePath(url, root: identityRoot)
            let relative = identityPrefix.map { "\($0)/\(treeRelative)" } ?? treeRelative
            if url.pathExtension.lowercased() == "pdf" {
                let pages = extractPDF(url, relative: relative, diagnostics: &diagnostics)
                for (page, content) in pages {
                    let bytes = content.utf8.count
                    guard totalBytes + bytes <= maximumTotalBytes else { return }
                    totalBytes += bytes
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
            let bytes = content.utf8.count
            guard totalBytes + bytes <= maximumTotalBytes else { return }
            totalBytes += bytes
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
    ) -> [(Int, String)] {
        guard let pdf = PDFDocument(url: url) else {
            diagnostics.append("Could not open PDF \(relative)")
            return []
        }
        var pages: [(Int, String)] = []
        if pdf.pageCount > maximumPDFPages {
            diagnostics.append("Limited PDF \(relative) to the first \(maximumPDFPages) pages")
        }
        for index in 0..<min(pdf.pageCount, maximumPDFPages) {
            guard let page = pdf.page(at: index) else { continue }
            var text = page.string?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
            if text.isEmpty {
                text = recognizeText(page: page)
                if !text.isEmpty {
                    diagnostics.append("Used local Vision OCR for \(relative), page \(index + 1)")
                }
            }
            if !text.isEmpty { pages.append((index + 1, text)) }
        }
        if pages.isEmpty { diagnostics.append("No text extracted from PDF \(relative)") }
        return pages
    }

    private func recognizeText(page: PDFPage) -> String {
        let bounds = page.bounds(for: .mediaBox)
        let scale = min(2, 4_000 / max(max(bounds.width, bounds.height), 1))
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
        let candidates = [
            environment["CASA_RS_SOURCE_ROOT"].map(URL.init(fileURLWithPath:)),
            Bundle.main.resourceURL?.appendingPathComponent("casars-source", isDirectory: true),
            fileManager.fileExists(atPath: current.appendingPathComponent("Cargo.toml").path)
                ? current : nil,
        ].compactMap { $0?.standardizedFileURL }
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
    var title: String
    var citationLabel: String
    var redistributionCleared: Bool

    private enum CodingKeys: String, CodingKey {
        case path, title
        case citationLabel = "citation_label"
        case redistributionCleared = "redistribution_cleared"
    }
}
