@_exported import CasarsFrontendServices
import Foundation

package enum NotebookDocumentViewMode: String, CaseIterable, Codable, Identifiable {
    case rich
    case raw

    package var id: String { rawValue }
}

package typealias JSONValue = NotebookValue

extension NotebookTaskIntent {
    package var profileTOML: String {
        var lines = [
            "[casars]",
            "format = \(format)",
            "surface = \(JSONValue.string(surface).tomlLiteral)",
            "kind = \(JSONValue.string(kind).tomlLiteral)",
            "contract = \(contract)",
            "",
            "[parameters]",
        ]
        lines.append(contentsOf: parameters.sorted { $0.key < $1.key }.map {
            "\($0.key) = \($0.value.tomlLiteral)"
        })
        lines.append("")
        return lines.joined(separator: "\n")
    }
}

extension NotebookValue {
    package static func string(_ value: String) -> Self { .string(value: value) }
    package static func number(_ value: Double) -> Self { .number(value: value) }
    package static func bool(_ value: Bool) -> Self { .bool(value: value) }
    package static func array(_ values: [Self]) -> Self { .array(values: values) }
    package static func object(_ values: [String: Self]) -> Self {
        .object(entries: values.sorted { $0.key < $1.key }.map {
            NotebookValueEntry(name: $0.key, value: $0.value)
        })
    }

    package init(parameterValue: SurfaceParameterValue) {
        switch parameterValue {
        case let .bool(value): self = .bool(value: value)
        case let .integer(value): self = .number(value: Double(value))
        case let .float(value): self = .number(value: value)
        case let .string(value): self = .string(value: value)
        case let .array(values): self = .array(values: values.map(Self.init(parameterValue:)))
        case let .table(entries):
            self = .object(entries: entries.map {
                NotebookValueEntry(name: $0.name, value: Self(parameterValue: $0.value))
            })
        }
    }

    package var displayText: String {
        switch self {
        case let .string(value): value
        case let .number(value): value.rounded() == value ? String(Int(value)) : String(value)
        case let .bool(value): value ? "true" : "false"
        case let .array(values): "[" + values.map(\.displayText).joined(separator: ", ") + "]"
        case let .object(entries): "{" + entries.sorted { $0.name < $1.name }.map { "\($0.name): \($0.value.displayText)" }.joined(separator: ", ") + "}"
        case .null: "null"
        }
    }

    package var objectValue: [String: JSONValue]? {
        guard case let .object(entries) = self else { return nil }
        return Dictionary(uniqueKeysWithValues: entries.map { ($0.name, $0.value) })
    }

    package var tomlLiteral: String {
        switch self {
        case let .string(value):
            return Self.tomlBasicString(value)
        case let .number(value): return value.rounded() == value ? String(Int(value)) : String(value)
        case let .bool(value): return value ? "true" : "false"
        case let .array(values): return "[" + values.map(\.tomlLiteral).joined(separator: ", ") + "]"
        case let .object(entries):
            return "{ " + entries.sorted { $0.name < $1.name }.map {
                "\(JSONValue.string($0.name).tomlLiteral) = \($0.value.tomlLiteral)"
            }.joined(separator: ", ") + " }"
        case .null: return "\"\""
        }
    }

    private static func tomlBasicString(_ value: String) -> String {
        var encoded = "\""
        for scalar in value.unicodeScalars {
            switch scalar.value {
            case 0x08: encoded += "\\b"
            case 0x09: encoded += "\\t"
            case 0x0A: encoded += "\\n"
            case 0x0C: encoded += "\\f"
            case 0x0D: encoded += "\\r"
            case 0x22: encoded += "\\\""
            case 0x5C: encoded += "\\\\"
            case 0x00...0x1F, 0x7F:
                encoded += String(format: "\\u%04X", scalar.value)
            default:
                encoded.unicodeScalars.append(scalar)
            }
        }
        encoded += "\""
        return encoded
    }
}

extension NotebookExecutionReceipt: Identifiable {
    public var id: String { "\(runId)-\(revision)" }
}

extension NotebookCellState: Identifiable {}
extension NotebookPythonOutputEvent: Identifiable {
    public var id: String { "\(order)-\(channel)" }
}
extension NotebookVisualizationSnapshot: Identifiable {}
extension NotebookVisualizationRevision: Identifiable {
    public var id: UInt64 { revision }
}

package struct NotebookTaskReplacementDiff: Codable, Equatable, Identifiable {
    package var parameter: String
    package var currentValue: JSONValue?
    package var notebookValue: JSONValue?

    package var id: String { parameter }
}

package struct NotebookTaskReplacementPreview: Codable, Equatable, Identifiable {
    package var targetTabID: String
    package var cellID: String
    package var sourcePath: String
    package var intent: NotebookTaskIntent
    package var receipt: NotebookExecutionReceipt?
    package var differences: [NotebookTaskReplacementDiff]

    package var id: String { "\(targetTabID)::\(cellID)" }
}

package struct NotebookDocumentState: Codable, Equatable, Identifiable {
    package var id: String
    package var filename: String
    package var source: String
    package var contentHash: String
    package var cells: [NotebookCellState]
    package var receipts: [NotebookExecutionReceipt]
    package var visualizations: [NotebookVisualizationSnapshot]
    package var draftSource: String
    package var viewMode: NotebookDocumentViewMode
    package var conflict: NotebookConflictState?

    private enum CodingKeys: String, CodingKey {
        case id, filename, source, contentHash, cells, receipts, visualizations
    }

    package init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        id = try values.decode(String.self, forKey: .id)
        filename = try values.decode(String.self, forKey: .filename)
        source = try values.decode(String.self, forKey: .source)
        contentHash = try values.decode(String.self, forKey: .contentHash)
        cells = try values.decodeIfPresent([NotebookCellState].self, forKey: .cells) ?? []
        receipts = try values.decode([NotebookExecutionReceipt].self, forKey: .receipts)
        visualizations = try values.decodeIfPresent(
            [NotebookVisualizationSnapshot].self,
            forKey: .visualizations
        ) ?? []
        draftSource = source
        viewMode = .rich
        conflict = nil
    }

    package init(projection: NotebookDocumentProjection) {
        id = projection.id
        filename = projection.filename
        source = projection.source
        contentHash = projection.contentHash
        cells = projection.cells
        receipts = projection.receipts
        visualizations = projection.visualizations
        draftSource = projection.source
        viewMode = .rich
        conflict = nil
    }

    package var isDirty: Bool { draftSource != source }
    package var title: String {
        source.split(separator: "\n").lazy
            .map(String.init)
            .first { $0.hasPrefix("# ") }?
            .dropFirst(2)
            .description ?? filename
    }
}

package struct NotebookConflictState: Codable, Equatable {
    package var baseHash: String
    package var external: NotebookExternalDocument
    package var proposedSource: String
}

package typealias NotebookExternalDocument = NotebookDocumentProjection

package struct ScientificNotebookProjectState: Codable, Equatable {
    package var schemaVersion: UInt32
    package var projectRoot: String
    package var notebooks: [NotebookDocumentState]
    package var activeNotebookID: String?

    private enum CodingKeys: String, CodingKey {
        case schemaVersion, projectRoot, notebooks
    }

    package init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        schemaVersion = try values.decode(UInt32.self, forKey: .schemaVersion)
        projectRoot = try values.decode(String.self, forKey: .projectRoot)
        notebooks = try values.decode([NotebookDocumentState].self, forKey: .notebooks)
        activeNotebookID = notebooks.first?.id
    }

    package init(projection: ScientificNotebookProjectProjection) {
        schemaVersion = projection.schemaVersion
        projectRoot = projection.projectRoot
        notebooks = projection.notebooks.map(NotebookDocumentState.init(projection:))
        activeNotebookID = notebooks.first?.id
    }

    package var activeNotebook: NotebookDocumentState? {
        notebooks.first { $0.id == activeNotebookID }
    }
}

package enum NotebookSaveResult: Equatable {
    case saved(NotebookDocumentState)
    case reloaded(NotebookDocumentState)
    case conflict(NotebookConflictState)
}

package protocol NotebookPersistenceClient {
    func projectCells(source: String) throws -> [NotebookCellState]
    func loadProject(projectRoot: String) throws -> ScientificNotebookProjectState
    func create(projectRoot: String, filename: String?, title: String) throws -> NotebookDocumentState
    func save(
        projectRoot: String,
        document: NotebookDocumentState,
        resolution: NotebookConflictResolution
    ) throws -> NotebookSaveResult
    func beginRecording(request: NotebookBeginRecordingRequest) throws -> NotebookBeginRecordingResult
    func finalizeRecording(request: NotebookFinalizeRecordingRequest) throws
    func saveVisualization(request: NotebookSaveVisualizationEnvelope) throws -> NotebookVisualizationSnapshot
}

package struct UniFFINotebookPersistenceClient: NotebookPersistenceClient {
    package init() {}

    package func projectCells(source: String) throws -> [NotebookCellState] {
        try CasarsFrontendServices.notebookCells(source: source)
    }

    package func loadProject(projectRoot: String) throws -> ScientificNotebookProjectState {
        ScientificNotebookProjectState(
            projection: try CasarsFrontendServices.notebookProject(projectRoot: projectRoot)
        )
    }

    package func create(
        projectRoot: String,
        filename: String?,
        title: String
    ) throws -> NotebookDocumentState {
        NotebookDocumentState(
            projection: try CasarsFrontendServices.notebookCreate(
                request: NotebookCreateRequest(
                    projectRoot: projectRoot,
                    filename: filename,
                    title: title
                )
            )
        )
    }

    package func save(
        projectRoot: String,
        document: NotebookDocumentState,
        resolution: NotebookConflictResolution
    ) throws -> NotebookSaveResult {
        let result = try CasarsFrontendServices.notebookSave(
            request: NotebookSaveRequest(
                projectRoot: projectRoot,
                filename: document.filename,
                baseHash: document.contentHash,
                source: document.draftSource,
                resolution: resolution
            )
        )
        switch result {
        case let .saved(notebook):
            return .saved(NotebookDocumentState(projection: notebook))
        case let .reloaded(notebook):
            return .reloaded(NotebookDocumentState(projection: notebook))
        case let .conflict(baseHash, external, proposedSource):
            return .conflict(NotebookConflictState(
                baseHash: baseHash,
                external: external,
                proposedSource: proposedSource
            ))
        }
    }

    package func beginRecording(request: NotebookBeginRecordingRequest) throws -> NotebookBeginRecordingResult {
        try CasarsFrontendServices.notebookBeginRecording(request: request)
    }

    package func finalizeRecording(request: NotebookFinalizeRecordingRequest) throws {
        try CasarsFrontendServices.notebookFinalizeRecording(request: request)
    }

    package func saveVisualization(
        request: NotebookSaveVisualizationEnvelope
    ) throws -> NotebookVisualizationSnapshot {
        try CasarsFrontendServices.notebookSaveVisualization(request: request)
    }
}
