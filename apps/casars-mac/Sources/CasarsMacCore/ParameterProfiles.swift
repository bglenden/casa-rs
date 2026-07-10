import Foundation
import CasarsFrontendServices

public enum SurfaceParameterValue: Codable, Equatable, Sendable {
    case bool(Bool)
    case integer(Int64)
    case float(Double)
    case string(String)
    case array([SurfaceParameterValue])
    case table([String: SurfaceParameterValue])

    private enum CodingKeys: String, CodingKey {
        case kind
        case value
    }

    private enum Kind: String, Codable {
        case bool
        case integer
        case float
        case string
        case array
        case table
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        switch try container.decode(Kind.self, forKey: .kind) {
        case .bool:
            self = .bool(try container.decode(Bool.self, forKey: .value))
        case .integer:
            self = .integer(try container.decode(Int64.self, forKey: .value))
        case .float:
            self = .float(try container.decode(Double.self, forKey: .value))
        case .string:
            self = .string(try container.decode(String.self, forKey: .value))
        case .array:
            self = .array(try container.decode([SurfaceParameterValue].self, forKey: .value))
        case .table:
            self = .table(try container.decode([String: SurfaceParameterValue].self, forKey: .value))
        }
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        switch self {
        case .bool(let value):
            try container.encode(Kind.bool, forKey: .kind)
            try container.encode(value, forKey: .value)
        case .integer(let value):
            try container.encode(Kind.integer, forKey: .kind)
            try container.encode(value, forKey: .value)
        case .float(let value):
            try container.encode(Kind.float, forKey: .kind)
            try container.encode(value, forKey: .value)
        case .string(let value):
            try container.encode(Kind.string, forKey: .kind)
            try container.encode(value, forKey: .value)
        case .array(let value):
            try container.encode(Kind.array, forKey: .kind)
            try container.encode(value, forKey: .value)
        case .table(let value):
            try container.encode(Kind.table, forKey: .kind)
            try container.encode(value, forKey: .value)
        }
    }

    public var displayText: String {
        switch self {
        case .bool(let value):
            return value ? "true" : "false"
        case .integer(let value):
            return String(value)
        case .float(let value):
            return String(format: "%.15g", value)
        case .string(let value):
            return value
        case .array(let values):
            return values.map(\.displayText).joined(separator: ",")
        case .table(let values):
            guard let data = try? JSONEncoder.sorted.encode(SurfaceParameterValue.table(values)) else { return "{}" }
            return String(decoding: data, as: UTF8.self)
        }
    }

    public var boolValue: Bool? {
        guard case .bool(let value) = self else { return nil }
        return value
    }
}

private extension JSONEncoder {
    static var sorted: JSONEncoder {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        return encoder
    }
}

public indirect enum SurfaceParameterType: Codable, Equatable, Sendable {
    case bool
    case integer
    case float
    case string
    case path(resourceKind: String?)
    case choice(values: [String])
    case quantity(dimension: String, canonicalUnit: String, specialValues: [String])
    case array(element: SurfaceParameterType, minItems: Int, maxItems: Int?, allowScalar: Bool)
    case table(fields: [String: SurfaceParameterType])
    case optional(value: SurfaceParameterType, states: [String])

    private enum CodingKeys: String, CodingKey {
        case kind
        case resourceKind = "resource_kind"
        case values
        case dimension
        case canonicalUnit = "canonical_unit"
        case specialValues = "special_values"
        case element
        case minItems = "min_items"
        case maxItems = "max_items"
        case allowScalar = "allow_scalar"
        case fields
        case value
        case states
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let kind = try container.decode(String.self, forKey: .kind)
        switch kind {
        case "bool": self = .bool
        case "integer": self = .integer
        case "float": self = .float
        case "string": self = .string
        case "path": self = .path(resourceKind: try container.decodeIfPresent(String.self, forKey: .resourceKind))
        case "choice": self = .choice(values: try container.decode([String].self, forKey: .values))
        case "quantity":
            self = .quantity(
                dimension: try container.decode(String.self, forKey: .dimension),
                canonicalUnit: try container.decode(String.self, forKey: .canonicalUnit),
                specialValues: try container.decodeIfPresent([String].self, forKey: .specialValues) ?? []
            )
        case "array":
            self = .array(
                element: try container.decode(SurfaceParameterType.self, forKey: .element),
                minItems: try container.decode(Int.self, forKey: .minItems),
                maxItems: try container.decodeIfPresent(Int.self, forKey: .maxItems),
                allowScalar: try container.decodeIfPresent(Bool.self, forKey: .allowScalar) ?? false
            )
        case "table": self = .table(fields: try container.decode([String: SurfaceParameterType].self, forKey: .fields))
        case "optional":
            self = .optional(
                value: try container.decode(SurfaceParameterType.self, forKey: .value),
                states: try container.decode([String].self, forKey: .states)
            )
        default:
            throw DecodingError.dataCorruptedError(
                forKey: .kind,
                in: container,
                debugDescription: "unknown parameter type \(kind)"
            )
        }
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        switch self {
        case .bool:
            try container.encode("bool", forKey: .kind)
        case .integer:
            try container.encode("integer", forKey: .kind)
        case .float:
            try container.encode("float", forKey: .kind)
        case .string:
            try container.encode("string", forKey: .kind)
        case .path(let resourceKind):
            try container.encode("path", forKey: .kind)
            try container.encodeIfPresent(resourceKind, forKey: .resourceKind)
        case .choice(let values):
            try container.encode("choice", forKey: .kind)
            try container.encode(values, forKey: .values)
        case .quantity(let dimension, let canonicalUnit, let specialValues):
            try container.encode("quantity", forKey: .kind)
            try container.encode(dimension, forKey: .dimension)
            try container.encode(canonicalUnit, forKey: .canonicalUnit)
            try container.encode(specialValues, forKey: .specialValues)
        case .array(let element, let minItems, let maxItems, let allowScalar):
            try container.encode("array", forKey: .kind)
            try container.encode(element, forKey: .element)
            try container.encode(minItems, forKey: .minItems)
            try container.encodeIfPresent(maxItems, forKey: .maxItems)
            try container.encode(allowScalar, forKey: .allowScalar)
        case .table(let fields):
            try container.encode("table", forKey: .kind)
            try container.encode(fields, forKey: .fields)
        case .optional(let value, let states):
            try container.encode("optional", forKey: .kind)
            try container.encode(value, forKey: .value)
            try container.encode(states, forKey: .states)
        }
    }

    public func value(from text: String) -> SurfaceParameterValue {
        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
        switch self {
        case .bool:
            return .bool(["1", "true", "yes", "on"].contains(trimmed.lowercased()))
        case .integer:
            return Int64(trimmed).map(SurfaceParameterValue.integer) ?? .string(text)
        case .float:
            return Double(trimmed).map(SurfaceParameterValue.float) ?? .string(text)
        case .string, .path, .choice, .quantity:
            return .string(text)
        case .array(let element, _, _, let allowScalar):
            let body = trimmed.hasPrefix("[") && trimmed.hasSuffix("]")
                ? String(trimmed.dropFirst().dropLast())
                : trimmed
            let parts = body.split(separator: ",", omittingEmptySubsequences: false).map {
                String($0).trimmingCharacters(in: .whitespacesAndNewlines)
            }
            if allowScalar, parts.count == 1 {
                return element.value(from: parts[0])
            }
            return .array(parts.map(element.value(from:)))
        case .table:
            guard let data = text.data(using: .utf8),
                  let value = try? JSONDecoder().decode([String: SurfaceParameterValue].self, from: data)
            else { return .string(text) }
            return .table(value)
        case .optional(let value, let states):
            if states.contains(trimmed) { return .string(trimmed) }
            return value.value(from: text)
        }
    }

    public var canonicalUnit: String? {
        switch self {
        case .quantity(_, let canonicalUnit, _): return canonicalUnit
        case .array(let element, _, _, _), .optional(let element, _): return element.canonicalUnit
        default: return nil
        }
    }

    public var isPathLike: Bool {
        switch self {
        case .path: return true
        case .array(let element, _, _, _), .optional(let element, _): return element.isPathLike
        default: return false
        }
    }

    public var resourceKind: String? {
        switch self {
        case .path(let resourceKind): return resourceKind
        case .array(let element, _, _, _), .optional(let element, _): return element.resourceKind
        default: return nil
        }
    }
}

public struct SurfaceParameterConcept: Codable, Equatable, Sendable {
    public var id: String
    public var semanticRevision: UInt64
    public var casaName: String
    public var valueDomain: SurfaceParameterType
    public var unitDimension: String?
    public var semanticRole: String
    public var documentation: SurfaceParameterDocumentation
    public var persistenceClass: String

    enum CodingKeys: String, CodingKey {
        case id
        case semanticRevision = "semantic_revision"
        case casaName = "casa_name"
        case valueDomain = "value_domain"
        case unitDimension = "unit_dimension"
        case semanticRole = "semantic_role"
        case documentation
        case persistenceClass = "persistence_class"
    }
}

public struct SurfaceParameterDocumentation: Codable, Equatable, Sendable {
    public var summary: String
    public var details: String?
    public var examples: [String]

    enum CodingKeys: String, CodingKey {
        case summary
        case details
        case examples
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        summary = try container.decode(String.self, forKey: .summary)
        details = try container.decodeIfPresent(String.self, forKey: .details)
        examples = try container.decodeIfPresent([String].self, forKey: .examples) ?? []
    }
}

public struct SurfaceParameterCatalog: Codable, Equatable, Sendable {
    public var schemaVersion: UInt64
    public var concepts: [SurfaceParameterConcept]

    enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
        case concepts
    }
}

public struct SurfaceParameterConceptReference: Codable, Equatable, Sendable {
    public var id: String
    public var semanticRevision: UInt64

    enum CodingKeys: String, CodingKey {
        case id
        case semanticRevision = "semantic_revision"
    }
}

public struct SurfaceParameterPresentation: Codable, Equatable, Sendable {
    public var label: String
    public var group: String
    public var advanced: Bool
    public var hidden: Bool
}

public struct SurfaceParameterProjections: Codable, Equatable, Sendable {
    public var cli: SurfaceParameterCLIProjection?
    public var provider: SurfaceParameterProviderProjection?
    public var presentation: SurfaceParameterPresentation
}

public struct SurfaceParameterCLIProjection: Codable, Equatable, Sendable {
    public var positional: Int?
    public var flags: [String]
    public var falseFlags: [String]
    public var metavar: String?

    enum CodingKeys: String, CodingKey {
        case positional
        case flags
        case falseFlags = "false_flags"
        case metavar
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        positional = try container.decodeIfPresent(Int.self, forKey: .positional)
        flags = try container.decodeIfPresent([String].self, forKey: .flags) ?? []
        falseFlags = try container.decodeIfPresent([String].self, forKey: .falseFlags) ?? []
        metavar = try container.decodeIfPresent(String.self, forKey: .metavar)
    }
}

public struct SurfaceParameterProviderProjection: Codable, Equatable, Sendable {
    public var field: String
    public var adapter: String
    public var emitWhen: SurfaceParameterPredicate?

    enum CodingKeys: String, CodingKey {
        case field
        case adapter
        case emitWhen = "emit_when"
    }
}

public indirect enum SurfaceParameterPredicate: Codable, Equatable, Sendable {
    case always
    case never
    case isSet(parameter: String)
    case equals(parameter: String, value: SurfaceParameterValue)
    case not(SurfaceParameterPredicate)
    case all([SurfaceParameterPredicate])
    case any([SurfaceParameterPredicate])

    private enum CodingKeys: String, CodingKey {
        case kind
        case parameter
        case value
        case predicate
        case predicates
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let kind = try container.decode(String.self, forKey: .kind)
        switch kind {
        case "always": self = .always
        case "never": self = .never
        case "is_set": self = .isSet(parameter: try container.decode(String.self, forKey: .parameter))
        case "equals":
            self = .equals(
                parameter: try container.decode(String.self, forKey: .parameter),
                value: try container.decode(SurfaceParameterValue.self, forKey: .value)
            )
        case "not": self = .not(try container.decode(SurfaceParameterPredicate.self, forKey: .predicate))
        case "all": self = .all(try container.decode([SurfaceParameterPredicate].self, forKey: .predicates))
        case "any": self = .any(try container.decode([SurfaceParameterPredicate].self, forKey: .predicates))
        default:
            throw DecodingError.dataCorruptedError(forKey: .kind, in: container, debugDescription: "unknown predicate \(kind)")
        }
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        switch self {
        case .always:
            try container.encode("always", forKey: .kind)
        case .never:
            try container.encode("never", forKey: .kind)
        case .isSet(let parameter):
            try container.encode("is_set", forKey: .kind)
            try container.encode(parameter, forKey: .parameter)
        case .equals(let parameter, let value):
            try container.encode("equals", forKey: .kind)
            try container.encode(parameter, forKey: .parameter)
            try container.encode(value, forKey: .value)
        case .not(let predicate):
            try container.encode("not", forKey: .kind)
            try container.encode(predicate, forKey: .predicate)
        case .all(let predicates):
            try container.encode("all", forKey: .kind)
            try container.encode(predicates, forKey: .predicates)
        case .any(let predicates):
            try container.encode("any", forKey: .kind)
            try container.encode(predicates, forKey: .predicates)
        }
    }

    public func evaluate(values: [String: SurfaceParameterValue]) -> Bool {
        switch self {
        case .always: return true
        case .never: return false
        case .isSet(let parameter): return values[parameter] != nil
        case .equals(let parameter, let value): return values[parameter] == value
        case .not(let predicate): return !predicate.evaluate(values: values)
        case .all(let predicates): return predicates.allSatisfy { $0.evaluate(values: values) }
        case .any(let predicates): return predicates.contains { $0.evaluate(values: values) }
        }
    }
}

public struct SurfaceParameterBinding: Codable, Equatable, Sendable, Identifiable {
    public var name: String
    public var concept: SurfaceParameterConceptReference
    public var order: Int
    public var refinements: [SurfaceNarrowingConstraint]
    public var contextRole: String?
    public var surfaceNote: String?
    public var projections: SurfaceParameterProjections

    public var id: String { name }

    enum CodingKeys: String, CodingKey {
        case name
        case concept
        case order
        case refinements
        case contextRole = "context_role"
        case surfaceNote = "surface_note"
        case projections
    }
}

public struct SurfaceNarrowingConstraint: Codable, Equatable, Sendable {
    public var kind: String
}

public struct SurfaceParameterDefinition: Codable, Equatable, Sendable {
    public var kind: String
    public var id: String
    public var contractVersion: UInt64
    public var displayName: String
    public var category: String
    public var summary: String
    public var execution: SurfaceExecutionProjection
    public var bindings: [SurfaceParameterBinding]

    enum CodingKeys: String, CodingKey {
        case kind
        case id
        case contractVersion = "contract_version"
        case displayName = "display_name"
        case category
        case summary
        case execution
        case bindings
    }
}

public struct SurfaceExecutionProjection: Codable, Equatable, Sendable {
    public var invocationName: String
    public var fixedArgs: [String]

    enum CodingKeys: String, CodingKey {
        case invocationName = "invocation_name"
        case fixedArgs = "fixed_args"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        invocationName = try container.decodeIfPresent(String.self, forKey: .invocationName) ?? ""
        fixedArgs = try container.decodeIfPresent([String].self, forKey: .fixedArgs) ?? []
    }
}

public struct SurfaceParameterBundle: Codable, Equatable, Sendable {
    public var schemaVersion: UInt64
    public var surface: SurfaceParameterDefinition
    public var catalog: SurfaceParameterCatalog

    enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
        case surface
        case catalog
    }

    public func concept(for parameter: String) -> SurfaceParameterConcept? {
        guard let reference = surface.bindings.first(where: { $0.name == parameter })?.concept else { return nil }
        return catalog.concepts.first {
            $0.id == reference.id && $0.semanticRevision == reference.semanticRevision
        }
    }
}

public struct SurfaceParameterState: Codable, Equatable, Sendable {
    public var value: SurfaceParameterValue?
    public var origin: String
    public var active: Bool
    public var required: Bool
    public var explicit: Bool
}

public struct SurfaceParameterLocation: Codable, Equatable, Sendable {
    public var line: Int
    public var column: Int
}

public struct SurfaceParameterDiagnostic: Codable, Equatable, Identifiable, Sendable {
    public var level: String
    public var code: String
    public var message: String
    public var parameter: String?
    public var location: SurfaceParameterLocation?
    public var suggestions: [String]

    enum CodingKeys: String, CodingKey {
        case level
        case code
        case message
        case parameter
        case location
        case suggestions
    }

    public init(
        level: String,
        code: String,
        message: String,
        parameter: String? = nil,
        location: SurfaceParameterLocation? = nil,
        suggestions: [String] = []
    ) {
        self.level = level
        self.code = code
        self.message = message
        self.parameter = parameter
        self.location = location
        self.suggestions = suggestions
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        level = try container.decode(String.self, forKey: .level)
        code = try container.decode(String.self, forKey: .code)
        message = try container.decode(String.self, forKey: .message)
        parameter = try container.decodeIfPresent(String.self, forKey: .parameter)
        location = try container.decodeIfPresent(SurfaceParameterLocation.self, forKey: .location)
        suggestions = try container.decodeIfPresent([String].self, forKey: .suggestions) ?? []
    }

    public var id: String {
        [level, code, parameter ?? "", message, location.map { "\($0.line):\($0.column)" } ?? ""].joined(separator: "|")
    }
}

public enum SurfaceParameterBaseSource: String, Codable, CaseIterable, Sendable {
    case defaults
    case last
    case lastSuccessful = "last_successful"
    case file

    public var title: String {
        switch self {
        case .defaults: return "Defaults"
        case .last: return "Last"
        case .lastSuccessful: return "Last Successful"
        case .file: return "Named File"
        }
    }
}

public struct SurfaceParameterSnapshot: Codable, Equatable, Sendable {
    public var schemaVersion: UInt64
    public var surfaceID: String
    public var surfaceKind: String
    public var contractVersion: UInt64
    public var baseSource: SurfaceParameterSourceRecord
    public var dirty: Bool
    public var states: [String: SurfaceParameterState]
    public var diagnostics: [SurfaceParameterDiagnostic]
    public var profileTOML: String?

    enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
        case surfaceID = "surface_id"
        case surfaceKind = "surface_kind"
        case contractVersion = "contract_version"
        case baseSource = "base_source"
        case dirty
        case states
        case diagnostics
        case profileTOML = "profile_toml"
    }
}

public enum SurfaceParameterSourceRecord: Codable, Equatable, Sendable {
    case defaults
    case last
    case lastSuccessful
    case file(String)

    private enum CodingKeys: String, CodingKey {
        case kind
        case path
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        switch try container.decode(String.self, forKey: .kind) {
        case "defaults": self = .defaults
        case "last": self = .last
        case "last_successful": self = .lastSuccessful
        case "file": self = .file(try container.decode(String.self, forKey: .path))
        case let kind:
            throw DecodingError.dataCorruptedError(forKey: .kind, in: container, debugDescription: "unknown source \(kind)")
        }
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        switch self {
        case .defaults: try container.encode("defaults", forKey: .kind)
        case .last: try container.encode("last", forKey: .kind)
        case .lastSuccessful: try container.encode("last_successful", forKey: .kind)
        case .file(let path):
            try container.encode("file", forKey: .kind)
            try container.encode(path, forKey: .path)
        }
    }
}

public struct SurfaceParameterPatch: Codable, Equatable, Sendable {
    public var values: [String: SurfaceParameterValue]
    public var unset: Set<String>

    public init(values: [String: SurfaceParameterValue] = [:], unset: Set<String> = []) {
        self.values = values
        self.unset = unset
    }
}

public struct SurfaceParameterSession: Codable, Equatable, Sendable {
    public var bundle: SurfaceParameterBundle
    public var snapshot: SurfaceParameterSnapshot
    public var selectedSource: SurfaceParameterBaseSource
    public var baseProfileTOML: String?
    public var baseProfilePath: String?
    public var contextPatch: SurfaceParameterPatch
    public var overridePatch: SurfaceParameterPatch
    public var draftText: [String: String]
    public var workspace: String
    public var saveLast: Bool

    public init(
        bundle: SurfaceParameterBundle,
        snapshot: SurfaceParameterSnapshot,
        selectedSource: SurfaceParameterBaseSource,
        baseProfileTOML: String?,
        baseProfilePath: String?,
        workspace: String,
        saveLast: Bool = true
    ) {
        self.bundle = bundle
        self.snapshot = snapshot
        self.selectedSource = selectedSource
        self.baseProfileTOML = baseProfileTOML
        self.baseProfilePath = baseProfilePath
        contextPatch = SurfaceParameterPatch()
        overridePatch = SurfaceParameterPatch()
        draftText = [:]
        self.workspace = workspace
        self.saveLast = saveLast
    }

    public var values: [String: SurfaceParameterValue] {
        snapshot.states.compactMapValues(\.value)
    }

    public var hasErrors: Bool {
        snapshot.diagnostics.contains { $0.level == "error" }
    }

    public func text(for parameter: String) -> String {
        draftText[parameter] ?? snapshot.states[parameter]?.value?.displayText ?? ""
    }

    public func toggle(for parameter: String) -> Bool {
        snapshot.states[parameter]?.value?.boolValue ?? false
    }

    public func origin(for parameter: String) -> String {
        snapshot.states[parameter]?.origin ?? "default"
    }
}

public struct SurfaceParameterWriteResult: Codable, Equatable, Sendable {
    public var path: String
    public var bytesWritten: UInt64
    public var managedKind: String?

    enum CodingKeys: String, CodingKey {
        case path
        case bytesWritten = "bytes_written"
        case managedKind = "managed_kind"
    }
}

public struct SurfaceRunSafety: Codable, Equatable, Sendable {
    public var classes: [String]
    public var requiresInteractiveConfirmation: Bool
    public var requiresOverwriteConfirmation: Bool
    public var requiresInputMutationConfirmation: Bool

    enum CodingKeys: String, CodingKey {
        case classes
        case requiresInteractiveConfirmation = "requires_interactive_confirmation"
        case requiresOverwriteConfirmation = "requires_overwrite_confirmation"
        case requiresInputMutationConfirmation = "requires_input_mutation_confirmation"
    }
}

public protocol SurfaceParameterClient {
    func loadBundle(surfaceID: String) throws -> SurfaceParameterBundle
    func defaults(surfaceID: String) throws -> SurfaceParameterSnapshot
    func last(surfaceID: String, workspace: String, successful: Bool) throws -> SurfaceParameterSnapshot?
    func load(surfaceID: String, profileTOML: String, sourcePath: String) throws -> SurfaceParameterSnapshot
    func resolve(
        surfaceID: String,
        baseSource: SurfaceParameterBaseSource,
        profileTOML: String?,
        profilePath: String?,
        context: SurfaceParameterPatch,
        override: SurfaceParameterPatch
    ) throws -> SurfaceParameterSnapshot
    func save(surfaceID: String, values: [String: SurfaceParameterValue], destinationPath: String) throws -> SurfaceParameterWriteResult
    func writeLast(
        surfaceID: String,
        workspace: String,
        values: [String: SurfaceParameterValue],
        successful: Bool
    ) throws -> SurfaceParameterWriteResult
    func runSafety(
        surfaceID: String,
        values: [String: SurfaceParameterValue]
    ) throws -> SurfaceRunSafety
    func providerInvocation(
        surfaceID: String,
        values: [String: SurfaceParameterValue]
    ) throws -> SurfaceProviderInvocation
}

public extension SurfaceParameterClient {
    func runSafety(
        surfaceID: String,
        values: [String: SurfaceParameterValue]
    ) throws -> SurfaceRunSafety {
        let valuesJSON = String(decoding: try JSONEncoder.sorted.encode(values), as: UTF8.self)
        let json = try CasarsFrontendServices.parameterRunSafetyJson(
            surfaceId: surfaceID,
            valuesJson: valuesJSON
        )
        return try JSONDecoder().decode(SurfaceRunSafety.self, from: Data(json.utf8))
    }

    func providerInvocation(
        surfaceID: String,
        values: [String: SurfaceParameterValue]
    ) throws -> SurfaceProviderInvocation {
        let valuesJSON = String(decoding: try JSONEncoder.sorted.encode(values), as: UTF8.self)
        let json = try CasarsFrontendServices.parameterProviderInvocationJson(
            surfaceId: surfaceID,
            valuesJson: valuesJSON
        )
        return try JSONDecoder().decode(SurfaceProviderInvocation.self, from: Data(json.utf8))
    }
}

public struct UniFFISurfaceParameterClient: SurfaceParameterClient {
    public init() {}

    public func loadBundle(surfaceID: String) throws -> SurfaceParameterBundle {
        try decode(try CasarsFrontendServices.parameterSurfaceBundleJson(surfaceId: surfaceID))
    }

    public func defaults(surfaceID: String) throws -> SurfaceParameterSnapshot {
        try decode(try CasarsFrontendServices.parameterDefaultsJson(surfaceId: surfaceID))
    }

    public func last(surfaceID: String, workspace: String, successful: Bool) throws -> SurfaceParameterSnapshot? {
        guard let json = try CasarsFrontendServices.parameterLastJson(
            surfaceId: surfaceID,
            workspace: workspace,
            successful: successful
        ) else { return nil }
        return try decode(json)
    }

    public func load(surfaceID: String, profileTOML: String, sourcePath: String) throws -> SurfaceParameterSnapshot {
        try decode(try CasarsFrontendServices.parameterLoadJson(
            surfaceId: surfaceID,
            profileToml: profileTOML,
            sourcePath: sourcePath
        ))
    }

    public func resolve(
        surfaceID: String,
        baseSource: SurfaceParameterBaseSource,
        profileTOML: String?,
        profilePath: String?,
        context: SurfaceParameterPatch,
        override: SurfaceParameterPatch
    ) throws -> SurfaceParameterSnapshot {
        try decode(try CasarsFrontendServices.parameterResolveJson(
            surfaceId: surfaceID,
            baseSource: baseSource.rawValue,
            profileToml: profileTOML,
            profilePath: profilePath,
            contextPatchJson: try encode(context),
            overridePatchJson: try encode(override)
        ))
    }

    public func save(
        surfaceID: String,
        values: [String: SurfaceParameterValue],
        destinationPath: String
    ) throws -> SurfaceParameterWriteResult {
        try decode(try CasarsFrontendServices.parameterSaveJson(
            surfaceId: surfaceID,
            valuesJson: try encode(values),
            destinationPath: destinationPath
        ))
    }

    public func writeLast(
        surfaceID: String,
        workspace: String,
        values: [String: SurfaceParameterValue],
        successful: Bool
    ) throws -> SurfaceParameterWriteResult {
        try decode(try CasarsFrontendServices.parameterWriteLastJson(
            surfaceId: surfaceID,
            workspace: workspace,
            valuesJson: try encode(values),
            successful: successful
        ))
    }

    private func decode<T: Decodable>(_ json: String) throws -> T {
        try JSONDecoder().decode(T.self, from: Data(json.utf8))
    }

    private func encode<T: Encodable>(_ value: T) throws -> String {
        let data = try JSONEncoder.sorted.encode(value)
        return String(decoding: data, as: UTF8.self)
    }
}
