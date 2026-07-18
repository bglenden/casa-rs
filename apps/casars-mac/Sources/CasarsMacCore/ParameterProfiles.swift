import Foundation
import CasarsFrontendServices

public extension SurfaceParameterValue {
    static func bool(_ value: Bool) -> Self { .bool(value: value) }
    static func integer(_ value: Int64) -> Self { .integer(value: value) }
    static func float(_ value: Double) -> Self { .float(value: value) }
    static func string(_ value: String) -> Self { .string(value: value) }
    static func array(_ values: [Self]) -> Self { .array(values: values) }
    static func table(_ values: [String: Self]) -> Self {
        .table(entries: values.keys.sorted().map { SurfaceParameterEntry(name: $0, value: values[$0]!) })
    }

    var tableValues: [String: Self]? {
        guard case .table(let entries) = self else { return nil }
        return Dictionary(uniqueKeysWithValues: entries.map { ($0.name, $0.value) })
    }

    var displayText: String {
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
        case .table:
            guard let data = try? JSONSerialization.data(
                withJSONObject: jsonValue,
                options: [.sortedKeys]
            ) else { return "{}" }
            return String(decoding: data, as: UTF8.self)
        }
    }

    var boolValue: Bool? {
        guard case .bool(let value) = self else { return nil }
        return value
    }

    fileprivate var jsonValue: Any {
        switch self {
        case .bool(let value): value
        case .integer(let value): value
        case .float(let value): value
        case .string(let value): value
        case .array(let values): values.map(\.jsonValue)
        case .table(let entries):
            Dictionary(uniqueKeysWithValues: entries.map { ($0.name, $0.value.jsonValue) })
        }
    }

    fileprivate static func fromJSONValue(_ value: Any) -> Self? {
        switch value {
        case let value as Bool:
            return .bool(value)
        case let value as NSNumber:
            let double = value.doubleValue
            return double.rounded() == double ? .integer(value.int64Value) : .float(double)
        case let value as String:
            return .string(value)
        case let values as [Any]:
            let converted = values.compactMap(Self.fromJSONValue)
            return converted.count == values.count ? .array(converted) : nil
        case let values as [String: Any]:
            let converted = values.compactMapValues(Self.fromJSONValue)
            return converted.count == values.count ? .table(converted) : nil
        default:
            return nil
        }
    }
}

public extension SurfaceParameterType {
    func value(from text: String) -> SurfaceParameterValue {
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
        case .array(let elements, _, _, let allowScalar):
            let element = requiredNestedType(elements, kind: "array")
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
                  let object = try? JSONSerialization.jsonObject(with: data),
                  let dictionary = object as? [String: Any],
                  let value = SurfaceParameterValue.fromJSONValue(dictionary)
            else { return .string(text) }
            return value
        case .optional(let values, let states):
            if states.contains(trimmed) { return .string(trimmed) }
            return requiredNestedType(values, kind: "optional").value(from: text)
        }
    }

    var canonicalUnit: String? {
        switch self {
        case .quantity(_, let canonicalUnit, _): return canonicalUnit
        case .array(let elements, _, _, _): return requiredNestedType(elements, kind: "array").canonicalUnit
        case .optional(let values, _): return requiredNestedType(values, kind: "optional").canonicalUnit
        default: return nil
        }
    }

    var isPathLike: Bool {
        switch self {
        case .path: return true
        case .array(let elements, _, _, _): return requiredNestedType(elements, kind: "array").isPathLike
        case .optional(let values, _): return requiredNestedType(values, kind: "optional").isPathLike
        default: return false
        }
    }

    var resourceKind: String? {
        switch self {
        case .path(let resourceKind): return resourceKind
        case .array(let elements, _, _, _): return requiredNestedType(elements, kind: "array").resourceKind
        case .optional(let values, _): return requiredNestedType(values, kind: "optional").resourceKind
        default: return nil
        }
    }

    private func requiredNestedType(_ values: [Self], kind: String) -> Self {
        precondition(values.count == 1, "generated \(kind) parameter types require exactly one nested type")
        return values[0]
    }
}

public extension SurfaceParameterBundle {
    func concept(for parameter: String) -> SurfaceParameterConcept? {
        guard let reference = surface.bindings.first(where: { $0.name == parameter })?.concept else {
            return nil
        }
        return catalog.concepts.first {
            $0.id == reference.id && $0.semanticRevision == reference.semanticRevision
        }
    }
}

extension SurfaceParameterBinding: Identifiable {
    public var id: String { name }
}

public extension SurfaceParameterPredicate {
    func evaluate(values: [String: SurfaceParameterValue]) -> Bool {
        switch self {
        case .always: true
        case .never: false
        case .isSet(let parameter): values[parameter] != nil
        case .equals(let parameter, let value): values[parameter] == value
        case .not(let predicates):
            !requiredNestedPredicate(predicates).evaluate(values: values)
        case .all(let predicates): predicates.allSatisfy { $0.evaluate(values: values) }
        case .any(let predicates): predicates.contains { $0.evaluate(values: values) }
        }
    }

    private func requiredNestedPredicate(_ values: [Self]) -> Self {
        precondition(values.count == 1, "generated not predicates require exactly one nested predicate")
        return values[0]
    }
}

extension SurfaceParameterDiagnostic: Identifiable {
    public var id: String {
        [level, code, parameter ?? "", message, location.map { "\($0.line):\($0.column)" } ?? ""]
            .joined(separator: "|")
    }
}

public extension SurfaceRunSafetyClass {
    var protocolValue: String {
        switch self {
        case .productWrite: "product_write"
        case .overwrite: "overwrite"
        case .inputMutation: "input_mutation"
        }
    }
}

public extension SurfaceParameterBaseSource {
    var title: String {
        switch self {
        case .defaults: "Defaults"
        case .last: "Last"
        case .lastSuccessful: "Last Successful"
        case .file: "Named File"
        }
    }
}

public extension SurfaceParameterPatch {
    init(
        values: [String: SurfaceParameterValue] = [:],
        unset: Set<String> = []
    ) {
        self.init(values: values, unset: unset.sorted())
    }

    mutating func removeUnset(_ name: String) {
        unset.removeAll { $0 == name }
    }

    mutating func insertUnset(_ name: String) {
        if !unset.contains(name) {
            unset.append(name)
            unset.sort()
        }
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
    func runSafety(surfaceID: String, values: [String: SurfaceParameterValue]) throws -> SurfaceRunSafety
    func providerInvocation(surfaceID: String, values: [String: SurfaceParameterValue]) throws -> SurfaceProviderInvocation
}

public protocol SessionParameterLifecycleClient {
    func opened(surfaceID: String, workspace: String, values: [String: SurfaceParameterValue], enabled: Bool) throws -> [String]
    func acceptedDurableChange(surfaceID: String, workspace: String, values: [String: SurfaceParameterValue], enabled: Bool) throws -> [String]
    func flush(surfaceID: String, workspace: String) -> [String]
    func flushAll() -> [String]
    func takeWarnings() -> [String]
}

public protocol TaskParameterLifecycleClient {
    func beforeExecution(attemptID: String, surfaceID: String, workspace: String, values: [String: SurfaceParameterValue], enabled: Bool) throws -> [String]
    func afterCompletion(attemptID: String, successful: Bool) -> [String]
}

public extension SurfaceParameterClient {
    func runSafety(surfaceID: String, values: [String: SurfaceParameterValue]) throws -> SurfaceRunSafety {
        try CasarsFrontendServices.parameterRunSafety(surfaceId: surfaceID, values: values)
    }

    func providerInvocation(surfaceID: String, values: [String: SurfaceParameterValue]) throws -> SurfaceProviderInvocation {
        try CasarsFrontendServices.parameterProviderInvocation(surfaceId: surfaceID, values: values)
    }
}

public struct UniFFISurfaceParameterClient: SurfaceParameterClient {
    public init() {}

    public func loadBundle(surfaceID: String) throws -> SurfaceParameterBundle {
        try CasarsFrontendServices.parameterSurfaceBundle(surfaceId: surfaceID)
    }

    public func defaults(surfaceID: String) throws -> SurfaceParameterSnapshot {
        try CasarsFrontendServices.parameterDefaults(surfaceId: surfaceID)
    }

    public func last(surfaceID: String, workspace: String, successful: Bool) throws -> SurfaceParameterSnapshot? {
        try CasarsFrontendServices.parameterLast(surfaceId: surfaceID, workspace: workspace, successful: successful)
    }

    public func load(surfaceID: String, profileTOML: String, sourcePath: String) throws -> SurfaceParameterSnapshot {
        try CasarsFrontendServices.parameterLoad(surfaceId: surfaceID, profileToml: profileTOML, sourcePath: sourcePath)
    }

    public func resolve(
        surfaceID: String,
        baseSource: SurfaceParameterBaseSource,
        profileTOML: String?,
        profilePath: String?,
        context: SurfaceParameterPatch,
        override: SurfaceParameterPatch
    ) throws -> SurfaceParameterSnapshot {
        try CasarsFrontendServices.parameterResolve(
            surfaceId: surfaceID,
            baseSource: baseSource,
            profileToml: profileTOML,
            profilePath: profilePath,
            contextPatch: context,
            overridePatch: override
        )
    }

    public func save(
        surfaceID: String,
        values: [String: SurfaceParameterValue],
        destinationPath: String
    ) throws -> SurfaceParameterWriteResult {
        try CasarsFrontendServices.parameterSave(
            surfaceId: surfaceID,
            values: values,
            destinationPath: destinationPath
        )
    }
}

public final class UniFFISessionParameterLifecycleClient: SessionParameterLifecycleClient {
    private let lifecycle = CasarsFrontendServices.ParameterSessionLifecycle()

    public init() {}

    public func opened(
        surfaceID: String,
        workspace: String,
        values: [String: SurfaceParameterValue],
        enabled: Bool
    ) throws -> [String] {
        try lifecycle.opened(surfaceId: surfaceID, workspace: workspace, values: values, enabled: enabled)
    }

    public func acceptedDurableChange(
        surfaceID: String,
        workspace: String,
        values: [String: SurfaceParameterValue],
        enabled: Bool
    ) throws -> [String] {
        try lifecycle.acceptedDurableChange(
            surfaceId: surfaceID,
            workspace: workspace,
            values: values,
            enabled: enabled
        )
    }

    public func flush(surfaceID: String, workspace: String) -> [String] {
        lifecycle.flush(surfaceId: surfaceID, workspace: workspace)
    }

    public func flushAll() -> [String] { lifecycle.flushAll() }
    public func takeWarnings() -> [String] { lifecycle.takeWarnings() }
}

public final class UniFFITaskParameterLifecycleClient: TaskParameterLifecycleClient {
    private let lifecycle = CasarsFrontendServices.ParameterTaskLifecycle()

    public init() {}

    public func beforeExecution(
        attemptID: String,
        surfaceID: String,
        workspace: String,
        values: [String: SurfaceParameterValue],
        enabled: Bool
    ) throws -> [String] {
        try lifecycle.beforeExecution(
            attemptId: attemptID,
            surfaceId: surfaceID,
            workspace: workspace,
            values: values,
            enabled: enabled
        )
    }

    public func afterCompletion(attemptID: String, successful: Bool) -> [String] {
        lifecycle.afterCompletion(attemptId: attemptID, successful: successful)
    }
}
