import Foundation

public enum TutorialPackLoadError: Error, Equatable, CustomStringConvertible {
    case missingManifest(String)
    case invalidManifest(String)

    public var description: String {
        switch self {
        case .missingManifest(let path):
            "No tutorial pack manifest found at \(path)"
        case .invalidManifest(let detail):
            "Invalid tutorial pack manifest: \(detail)"
        }
    }
}

public struct TutorialPackManifest: Codable, Equatable {
    public var schemaVersion: String
    public var packID: String
    public var tutorialID: String
    public var title: String
    public var declaredCasaVersion: String
    public var inputs: [TutorialPackInput]
    public var workspace: TutorialPackWorkspace
    public var learner: TutorialPackLearnerView
    public var regression: TutorialPackRegressionView
    public var sections: [TutorialPackSection]

    enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
        case packID = "pack_id"
        case tutorialID = "tutorial_id"
        case title
        case declaredCasaVersion = "declared_casa_version"
        case inputs
        case workspace
        case learner
        case regression
        case sections
    }
}

public struct TutorialPackInput: Codable, Equatable, Identifiable {
    public var id: String
    public var displayName: String
    public var kind: String
    public var registryKey: String
    public var sourceArtifactURL: String
    public var filename: String
    public var sizeBytes: UInt64
    public var checksumPolicy: String
    public var packPath: String
    public var materialization: String

    enum CodingKeys: String, CodingKey {
        case id
        case displayName = "display_name"
        case kind
        case registryKey = "registry_key"
        case sourceArtifactURL = "source_artifact_url"
        case filename
        case sizeBytes = "size_bytes"
        case checksumPolicy = "checksum_policy"
        case packPath = "pack_path"
        case materialization
    }
}

public struct TutorialPackWorkspace: Codable, Equatable {
    public var root: String
    public var nativePath: String
    public var oraclePath: String
    public var scratchPath: String

    enum CodingKeys: String, CodingKey {
        case root
        case nativePath = "native_path"
        case oraclePath = "oracle_path"
        case scratchPath = "scratch_path"
    }
}

public struct TutorialPackLearnerView: Codable, Equatable {
    public var docsIndex: String
    public var sectionDocsPath: String
    public var screenshotPath: String
    public var includeInternalEvidence: Bool

    enum CodingKeys: String, CodingKey {
        case docsIndex = "docs_index"
        case sectionDocsPath = "section_docs_path"
        case screenshotPath = "screenshot_path"
        case includeInternalEvidence = "include_internal_evidence"
    }
}

public struct TutorialPackRegressionView: Codable, Equatable {
    public var evidencePath: String
    public var dataManifest: String
    public var nativeRuns: String
    public var oracleRuns: String
    public var comparisons: String
    public var timings: String
    public var providerProvenance: String
    public var reviewPath: String
    public var reviewRecordSchema: String
    public var screenshotSpecsPath: String

    enum CodingKeys: String, CodingKey {
        case evidencePath = "evidence_path"
        case dataManifest = "data_manifest"
        case nativeRuns = "native_runs"
        case oracleRuns = "oracle_runs"
        case comparisons
        case timings
        case providerProvenance = "provider_provenance"
        case reviewPath = "review_path"
        case reviewRecordSchema = "review_record_schema"
        case screenshotSpecsPath = "screenshot_specs_path"
    }
}

public struct TutorialPackSection: Codable, Equatable, Identifiable {
    public var id: String
    public var sequence: UInt64
    public var title: String
    public var observableResult: String
    public var inputRefs: [String]
    public var tasks: [String]
    public var steps: [TutorialPackStep]
    public var reviewCheckpoint: TutorialPackReviewCheckpoint

    enum CodingKeys: String, CodingKey {
        case id
        case sequence
        case title
        case observableResult = "observable_result"
        case inputRefs = "input_refs"
        case tasks
        case steps
        case reviewCheckpoint = "review_checkpoint"
    }
}

public struct TutorialPackStep: Codable, Equatable, Identifiable {
    public var id: String
    public var surface: String
    public var providerKind: String
    public var taskID: String
    public var commandTemplate: String?
    public var uiPath: String?
    public var parameters: [String: TutorialPackValue]

    enum CodingKeys: String, CodingKey {
        case id
        case surface
        case providerKind = "provider_kind"
        case taskID = "task_id"
        case commandTemplate = "command_template"
        case uiPath = "ui_path"
        case parameters
    }
}

public enum TutorialPackValue: Codable, Equatable {
    case string(String)
    case bool(Bool)
    case number(Double)
    case array([TutorialPackValue])
    case object([String: TutorialPackValue])
    case null

    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        if container.decodeNil() {
            self = .null
        } else if let value = try? container.decode(Bool.self) {
            self = .bool(value)
        } else if let value = try? container.decode(Double.self) {
            self = .number(value)
        } else if let value = try? container.decode([TutorialPackValue].self) {
            self = .array(value)
        } else if let value = try? container.decode([String: TutorialPackValue].self) {
            self = .object(value)
        } else {
            self = .string(try container.decode(String.self))
        }
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        switch self {
        case .string(let value):
            try container.encode(value)
        case .bool(let value):
            try container.encode(value)
        case .number(let value):
            try container.encode(value)
        case .array(let value):
            try container.encode(value)
        case .object(let value):
            try container.encode(value)
        case .null:
            try container.encodeNil()
        }
    }

    public var stringValue: String? {
        switch self {
        case .string(let value):
            value
        case .number(let value):
            value.rounded() == value ? String(Int(value)) : String(value)
        case .bool, .array, .object, .null:
            nil
        }
    }

    public var boolValue: Bool? {
        switch self {
        case .bool(let value):
            value
        case .string(let value):
            value == "true" ? true : value == "false" ? false : nil
        case .number, .array, .object, .null:
            nil
        }
    }
}

public struct TutorialPackReviewCheckpoint: Codable, Equatable {
    public var required: Bool
    public var status: String
    public var recordPath: String

    enum CodingKeys: String, CodingKey {
        case required
        case status
        case recordPath = "record_path"
    }
}

public enum TutorialPackInputStatus: String, Codable, Equatable {
    case missing
    case staged
}

public struct TutorialPackInputState: Codable, Equatable, Identifiable {
    public var id: String
    public var displayName: String
    public var kind: String
    public var filename: String
    public var registryKey: String
    public var sourceArtifactURL: String
    public var packPath: String
    public var resolvedPath: String
    public var status: TutorialPackInputStatus
}

public struct TutorialPackContext: Codable, Equatable {
    public var packID: String
    public var tutorialID: String
    public var title: String
    public var declaredCasaVersion: String
    public var rootPath: String
    public var manifestPath: String
    public var inputs: [TutorialPackInputState]
    public var sections: [TutorialPackSection]
    public var selectedSectionID: String?
    public var workspaceRoot: String
    public var nativeWorkspacePath: String
    public var oracleWorkspacePath: String
    public var reviewPath: String
    public var learnerDocsIndex: String

    public var selectedSection: TutorialPackSection? {
        guard let selectedSectionID else { return sections.first }
        return sections.first { $0.id == selectedSectionID } ?? sections.first
    }

    public static func load(path: String, fileManager: FileManager = .default) throws -> TutorialPackContext {
        let suppliedURL = URL(fileURLWithPath: (path as NSString).expandingTildeInPath)
            .standardizedFileURL
        let manifestURL: URL
        var isDirectory: ObjCBool = false
        if fileManager.fileExists(atPath: suppliedURL.path, isDirectory: &isDirectory), isDirectory.boolValue {
            manifestURL = suppliedURL.appendingPathComponent("pack.json")
        } else {
            manifestURL = suppliedURL
        }
        guard fileManager.fileExists(atPath: manifestURL.path) else {
            throw TutorialPackLoadError.missingManifest(manifestURL.path)
        }

        let data = try Data(contentsOf: manifestURL)
        let manifest: TutorialPackManifest
        do {
            manifest = try JSONDecoder().decode(TutorialPackManifest.self, from: data)
        } catch {
            throw TutorialPackLoadError.invalidManifest("\(error)")
        }
        guard manifest.schemaVersion == "tutorial-pack.v0" else {
            throw TutorialPackLoadError.invalidManifest("unsupported schema_version \(manifest.schemaVersion)")
        }

        let rootURL = manifestURL.deletingLastPathComponent().standardizedFileURL
        let inputs = manifest.inputs.map { input in
            let resolved = rootURL.appendingPathComponent(input.packPath).standardizedFileURL
            let exists = fileManager.fileExists(atPath: resolved.path)
            return TutorialPackInputState(
                id: input.id,
                displayName: input.displayName,
                kind: input.kind,
                filename: input.filename,
                registryKey: input.registryKey,
                sourceArtifactURL: input.sourceArtifactURL,
                packPath: input.packPath,
                resolvedPath: resolved.path,
                status: exists ? .staged : .missing
            )
        }

        return TutorialPackContext(
            packID: manifest.packID,
            tutorialID: manifest.tutorialID,
            title: manifest.title,
            declaredCasaVersion: manifest.declaredCasaVersion,
            rootPath: rootURL.path,
            manifestPath: manifestURL.path,
            inputs: inputs,
            sections: manifest.sections,
            selectedSectionID: manifest.sections.first?.id,
            workspaceRoot: rootURL.appendingPathComponent(manifest.workspace.root).standardizedFileURL.path,
            nativeWorkspacePath: rootURL.appendingPathComponent(manifest.workspace.nativePath).standardizedFileURL.path,
            oracleWorkspacePath: rootURL.appendingPathComponent(manifest.workspace.oraclePath).standardizedFileURL.path,
            reviewPath: rootURL.appendingPathComponent(manifest.regression.reviewPath).standardizedFileURL.path,
            learnerDocsIndex: rootURL.appendingPathComponent(manifest.learner.docsIndex).standardizedFileURL.path
        )
    }

    public func datasetSummaries() -> [DatasetSummary] {
        inputs.compactMap { input in
            guard input.status == .staged else { return nil }
            let pathURL = URL(fileURLWithPath: input.resolvedPath)
            return DatasetSummary(
                id: input.resolvedPath,
                name: pathURL.lastPathComponent,
                path: input.resolvedPath,
                kind: input.datasetKind,
                size: "tutorial input",
                units: "",
                notes: "Tutorial pack input: \(input.displayName)",
                diagnostics: [
                    "registry_key=\(input.registryKey)",
                    "pack_path=\(input.packPath)"
                ]
            )
        }
    }
}

private extension TutorialPackInputState {
    var datasetKind: DatasetKind {
        switch kind.lowercased() {
        case "measurement-set":
            return .measurementSet
        case "calibration-table":
            return .calibrationTable
        case "region":
            return .region
        case "table":
            return .table
        default:
            let lowerPath = packPath.lowercased()
            if lowerPath.hasSuffix(".ms") {
                return .measurementSet
            }
            if lowerPath.hasSuffix(".crtf") {
                return .region
            }
            return .imageCube
        }
    }
}
