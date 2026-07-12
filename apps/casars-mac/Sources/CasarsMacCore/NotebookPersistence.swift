import CasarsFrontendServices
import Foundation

package enum NotebookDocumentViewMode: String, CaseIterable, Codable, Identifiable {
    case rich
    case raw

    package var id: String { rawValue }
}

package struct NotebookReceiptArtifact: Codable, Equatable {
    package var role: String
    package var path: String
    package var mediaType: String?
}

package struct NotebookTaskIntent: Codable, Equatable {
    package var format: UInt32
    package var surface: String
    package var kind: String
    package var contract: UInt32
    package var parameters: [String: JSONValue]

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

package enum JSONValue: Codable, Equatable {
    case string(String)
    case number(Double)
    case bool(Bool)
    case array([JSONValue])
    case object([String: JSONValue])
    case null

    package init(parameterValue: SurfaceParameterValue) {
        switch parameterValue {
        case let .bool(value): self = .bool(value)
        case let .integer(value): self = .number(Double(value))
        case let .float(value): self = .number(value)
        case let .string(value): self = .string(value)
        case let .array(values): self = .array(values.map(JSONValue.init(parameterValue:)))
        case let .table(values): self = .object(values.mapValues(JSONValue.init(parameterValue:)))
        }
    }

    package init(from decoder: Decoder) throws {
        let value = try decoder.singleValueContainer()
        if value.decodeNil() { self = .null }
        else if let decoded = try? value.decode(Bool.self) { self = .bool(decoded) }
        else if let decoded = try? value.decode(Double.self) { self = .number(decoded) }
        else if let decoded = try? value.decode(String.self) { self = .string(decoded) }
        else if let decoded = try? value.decode([JSONValue].self) { self = .array(decoded) }
        else { self = .object(try value.decode([String: JSONValue].self)) }
    }

    package func encode(to encoder: Encoder) throws {
        var value = encoder.singleValueContainer()
        switch self {
        case let .string(decoded): try value.encode(decoded)
        case let .number(decoded): try value.encode(decoded)
        case let .bool(decoded): try value.encode(decoded)
        case let .array(decoded): try value.encode(decoded)
        case let .object(decoded): try value.encode(decoded)
        case .null: try value.encodeNil()
        }
    }

    package var displayText: String {
        switch self {
        case let .string(value): value
        case let .number(value): value.rounded() == value ? String(Int(value)) : String(value)
        case let .bool(value): value ? "true" : "false"
        case let .array(values): "[" + values.map(\.displayText).joined(separator: ", ") + "]"
        case let .object(values): "{" + values.sorted { $0.key < $1.key }.map { "\($0.key): \($0.value.displayText)" }.joined(separator: ", ") + "}"
        case .null: "null"
        }
    }

    package var objectValue: [String: JSONValue]? {
        guard case let .object(value) = self else { return nil }
        return value
    }

    package var tomlLiteral: String {
        switch self {
        case let .string(value):
            let data = try? JSONEncoder().encode(value)
            return data.map { String(decoding: $0, as: UTF8.self) } ?? "\"\""
        case let .number(value): return value.rounded() == value ? String(Int(value)) : String(value)
        case let .bool(value): return value ? "true" : "false"
        case let .array(values): return "[" + values.map(\.tomlLiteral).joined(separator: ", ") + "]"
        case let .object(values):
            return "{ " + values.sorted { $0.key < $1.key }.map {
                "\(JSONValue.string($0.key).tomlLiteral) = \($0.value.tomlLiteral)"
            }.joined(separator: ", ") + " }"
        case .null: return "\"\""
        }
    }
}

package struct NotebookExecutionReceipt: Codable, Equatable, Identifiable {
    package var schemaVersion: UInt32
    package var runId: String
    package var revision: UInt64
    package var notebookId: String
    package var cellId: String
    package var initiatingSurface: String
    package var operationId: String
    package var startedAt: UInt64
    package var finishedAt: UInt64
    package var status: String
    package var sparseIntent: NotebookTaskIntent?
    package var executionInput: NotebookExecutionInput?
    package var orderedOutputs: [NotebookPythonOutputEvent]?
    package var resolvedParameters: [String: JSONValue]
    package var providerContractVersion: UInt32
    package var affectedPaths: [String]
    package var products: [NotebookReceiptArtifact]
    package var artifacts: [NotebookReceiptArtifact]
    package var diagnostics: [String]
    package var replayClaim: String

    package var id: String { "\(runId)-\(revision)" }
}

package struct NotebookCellState: Codable, Equatable, Identifiable {
    package var id: String
    package var kind: String
    package var body: String
    package var taskIntent: NotebookTaskIntent?

    private enum CodingKeys: String, CodingKey {
        case id, kind, body, taskIntent
    }

    package init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        id = try values.decode(String.self, forKey: .id)
        kind = try values.decode(String.self, forKey: .kind)
        body = try values.decodeIfPresent(String.self, forKey: .body) ?? ""
        taskIntent = try values.decodeIfPresent(NotebookTaskIntent.self, forKey: .taskIntent)
    }
}

package struct NotebookExecutionInput: Codable, Equatable {
    package var kind: String
    package var details: NotebookPythonExecutionInput
}

package struct NotebookPythonExecutionInput: Codable, Equatable {
    package var source: String
    package var sourceSHA256: String
    package var authority: String
    package var inputReferences: [String]
    package var environment: NotebookPythonEnvironmentIdentity

    private enum CodingKeys: String, CodingKey {
        case source
        case sourceSHA256 = "sourceSha256"
        case authority, inputReferences, environment
    }
}

package struct NotebookPythonEnvironmentIdentity: Codable, Equatable {
    package var environmentId: String
    package var interpreter: String
    package var implementation: String
    package var version: String
    package var casaRsVersion: String?
    package var packages: [String: String]
    package var fingerprintSHA256: String

    private enum CodingKeys: String, CodingKey {
        case environmentId, interpreter, implementation, version, casaRsVersion, packages
        case fingerprintSHA256 = "fingerprintSha256"
    }
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

package struct NotebookExternalDocument: Codable, Equatable {
    package var id: String
    package var filename: String
    package var source: String
    package var contentHash: String
    package var cells: [NotebookCellState]
    package var receipts: [NotebookExecutionReceipt]
    package var visualizations: [NotebookVisualizationSnapshot]

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
    }
}

package struct NotebookVisualizationSnapshot: Codable, Equatable, Identifiable {
    package var schemaVersion: UInt32
    package var id: String
    package var notebookId: String
    package var cellId: String
    package var title: String
    package var revisions: [NotebookVisualizationRevision]
}

package struct NotebookVisualizationRevision: Codable, Equatable, Identifiable {
    package var revision: UInt64
    package var createdAt: UInt64
    package var assetPath: String
    package var sourceReferences: [String]
    package var reopen: NotebookVisualizationReopenIntent
    package var render: NotebookVisualizationRenderMetadata
    package var id: UInt64 { revision }
}

package struct NotebookVisualizationReopenIntent: Codable, Equatable {
    package var surface: String
    package var contractVersion: UInt32
    package var parameters: [String: JSONValue]
    package var profileTOML: String?
}

package struct NotebookVisualizationRenderMetadata: Codable, Equatable {
    package var renderer: String
    package var mediaType: String
    package var width: UInt32
    package var height: UInt32
    package var settings: [String: JSONValue]
}

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

    package var activeNotebook: NotebookDocumentState? {
        notebooks.first { $0.id == activeNotebookID }
    }
}

package enum NotebookConflictResolution: String, Encodable {
    case reject
    case keepLocal = "keep_local"
    case reloadExternal = "reload_external"
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

package struct NotebookBeginRecordingRequest: Encodable {
    package var projectRoot: String
    package var policy: String
    package var request: NotebookRecordingRequest
}

package struct NotebookRecordingRequest: Encodable {
    package var initiatingSurface: String
    package var operationId: String
    package var notebookId: String?
    package var cellId: String?
    package var taskIntent: NotebookTaskIntent?
    package var executionInput: NotebookExecutionInput?
    package var providerContractVersion: UInt32
    package var resolvedParameters: [String: JSONValue]
    package var runSafety: NotebookRunSafetyRecord
    package var approvals: [NotebookApprovalRecord]
}

package struct NotebookRunSafetyRecord: Encodable {
    package var classification: String
    package var affectedPaths: [String]
}

package struct NotebookApprovalRecord: Encodable {
    package var kind: String
    package var actor: String
    package var timestamp: UInt64
    package var contentHash: String?
}

package struct NotebookBeginRecordingResult: Decodable, Equatable {
    package var handle: NotebookAttemptHandle?
    package var warning: String?
}

package struct NotebookAttemptHandle: Codable, Equatable {
    package var runId: String
    package var revision: UInt64
    package var notebookId: String
    package var cellId: String
    package var startedAt: UInt64
}

package struct NotebookFinalizeRecordingRequest: Encodable {
    package var projectRoot: String
    package var handle: NotebookAttemptHandle
    package var finalization: NotebookReceiptFinalization
}

package struct NotebookReceiptFinalization: Encodable {
    package var status: String
    package var finishedAt: UInt64
    package var affectedPaths: [String]
    package var products: [NotebookReceiptArtifact]
    package var artifacts: [NotebookReceiptArtifact]
    package var diagnostics: [String]
    package var stdout: [UInt8]
    package var stderr: [UInt8]
    package var casaLog: String?
}

package struct NotebookSaveVisualizationEnvelope: Encodable {
    package var projectRoot: String
    package var request: NotebookSaveVisualizationRequest
}

package struct NotebookSaveVisualizationRequest: Encodable {
    package var notebookId: String?
    package var visualizationId: String?
    package var title: String
    package var sourceAsset: String
    package var sourceReferences: [String]
    package var reopen: NotebookVisualizationReopenIntent
    package var render: NotebookVisualizationRenderMetadata
}

package struct UniFFINotebookPersistenceClient: NotebookPersistenceClient {
    private let encoder: JSONEncoder
    private let decoder: JSONDecoder

    package init() {
        encoder = JSONEncoder()
        encoder.keyEncodingStrategy = .convertToSnakeCase
        decoder = JSONDecoder()
        decoder.keyDecodingStrategy = .convertFromSnakeCase
    }

    package func projectCells(source: String) throws -> [NotebookCellState] {
        let json = try CasarsFrontendServices.notebookCellsJson(source: source)
        return try decoder.decode([NotebookCellState].self, from: Data(json.utf8))
    }

    package func loadProject(projectRoot: String) throws -> ScientificNotebookProjectState {
        let json = try CasarsFrontendServices.notebookProjectJson(projectRoot: projectRoot)
        return try decoder.decode(ScientificNotebookProjectState.self, from: Data(json.utf8))
    }

    package func create(
        projectRoot: String,
        filename: String?,
        title: String
    ) throws -> NotebookDocumentState {
        let request = NotebookCreateRequest(projectRoot: projectRoot, filename: filename, title: title)
        let json = try CasarsFrontendServices.notebookCreateJson(
            requestJson: String(decoding: try encoder.encode(request), as: UTF8.self)
        )
        return try decoder.decode(NotebookDocumentState.self, from: Data(json.utf8))
    }

    package func save(
        projectRoot: String,
        document: NotebookDocumentState,
        resolution: NotebookConflictResolution
    ) throws -> NotebookSaveResult {
        let request = NotebookSaveRequest(
            projectRoot: projectRoot,
            filename: document.filename,
            baseHash: document.contentHash,
            source: document.draftSource,
            resolution: resolution
        )
        let json = try CasarsFrontendServices.notebookSaveJson(
            requestJson: String(decoding: try encoder.encode(request), as: UTF8.self)
        )
        let envelope = try decoder.decode(NotebookSaveEnvelope.self, from: Data(json.utf8))
        switch envelope.outcome {
        case "saved": return .saved(try envelope.requiredNotebook())
        case "reloaded": return .reloaded(try envelope.requiredNotebook())
        case "conflict":
            guard let baseHash = envelope.baseHash,
                  let external = envelope.external,
                  let proposedSource = envelope.proposedSource
            else { throw NotebookPersistenceError.invalidSaveResponse }
            return .conflict(NotebookConflictState(
                baseHash: baseHash,
                external: external,
                proposedSource: proposedSource
            ))
        default: throw NotebookPersistenceError.invalidSaveResponse
        }
    }

    package func beginRecording(request: NotebookBeginRecordingRequest) throws -> NotebookBeginRecordingResult {
        let json = try CasarsFrontendServices.notebookBeginRecordingJson(
            requestJson: String(decoding: try encoder.encode(request), as: UTF8.self)
        )
        return try decoder.decode(NotebookBeginRecordingResult.self, from: Data(json.utf8))
    }

    package func finalizeRecording(request: NotebookFinalizeRecordingRequest) throws {
        _ = try CasarsFrontendServices.notebookFinalizeRecordingJson(
            requestJson: String(decoding: try encoder.encode(request), as: UTF8.self)
        )
    }

    package func saveVisualization(
        request: NotebookSaveVisualizationEnvelope
    ) throws -> NotebookVisualizationSnapshot {
        let json = try CasarsFrontendServices.notebookSaveVisualizationJson(
            requestJson: String(decoding: try encoder.encode(request), as: UTF8.self)
        )
        return try decoder.decode(NotebookVisualizationSnapshot.self, from: Data(json.utf8))
    }
}

private struct NotebookSaveRequest: Encodable {
    var projectRoot: String
    var filename: String
    var baseHash: String
    var source: String
    var resolution: NotebookConflictResolution
}

private struct NotebookCreateRequest: Encodable {
    var projectRoot: String
    var filename: String?
    var title: String
}

private struct NotebookSaveEnvelope: Decodable {
    var outcome: String
    var notebook: NotebookDocumentState?
    var baseHash: String?
    var external: NotebookExternalDocument?
    var proposedSource: String?

    func requiredNotebook() throws -> NotebookDocumentState {
        guard let notebook else { throw NotebookPersistenceError.invalidSaveResponse }
        return notebook
    }
}

package enum NotebookPersistenceError: Error {
    case invalidSaveResponse
}
