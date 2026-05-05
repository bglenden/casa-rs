import Foundation

public enum DirtyImagingWeighting: String, CaseIterable, Codable, Equatable, Identifiable {
    case natural
    case uniform
    case briggs

    public var id: String { rawValue }

    public var title: String {
        switch self {
        case .natural:
            "Natural"
        case .uniform:
            "Uniform"
        case .briggs:
            "Briggs"
        }
    }
}

public enum DirtyImagingDimensionSeverity: String, Equatable {
    case good
    case warning
    case terrible
}

public struct DirtyImagingDimensionAssessment: Equatable {
    public var value: Int
    public var severity: DirtyImagingDimensionSeverity
    public var adjustedValue: Int
    public var message: String

    public var needsAdjustment: Bool {
        adjustedValue != value
    }
}

public struct DirtyImagingTaskParameters: Codable, Equatable {
    public var datasetID: String
    public var measurementSetPath: String
    public var outputPrefix: String
    public var selectedField: String?
    public var phaseCenterField: String?
    public var selectedSpectralWindow: String?
    public var channelStart: String
    public var channelCount: String
    public var dataColumn: String
    public var correlation: String?
    public var imageSize: Int
    public var imageHeight: Int
    public var cellArcsec: Double
    public var weighting: DirtyImagingWeighting
    public var dirtyOnly: Bool
    public var runReason: String

    public init(
        datasetID: String,
        measurementSetPath: String,
        outputPrefix: String,
        selectedField: String?,
        phaseCenterField: String?,
        selectedSpectralWindow: String?,
        channelStart: String = "",
        channelCount: String = "",
        dataColumn: String,
        correlation: String? = nil,
        imageSize: Int = 512,
        imageHeight: Int? = nil,
        cellArcsec: Double = 1.0,
        weighting: DirtyImagingWeighting = .natural,
        dirtyOnly: Bool = true,
        runReason: String = "Initial dirty image from selected MeasurementSet."
    ) {
        self.datasetID = datasetID
        self.measurementSetPath = measurementSetPath
        self.outputPrefix = outputPrefix
        self.selectedField = selectedField
        self.phaseCenterField = phaseCenterField
        self.selectedSpectralWindow = selectedSpectralWindow
        self.channelStart = channelStart
        self.channelCount = channelCount
        self.dataColumn = dataColumn
        self.correlation = correlation
        self.imageSize = imageSize
        self.imageHeight = imageHeight ?? imageSize
        self.cellArcsec = cellArcsec
        self.weighting = weighting
        self.dirtyOnly = dirtyOnly
        self.runReason = runReason
    }

    public var requestSummary: String {
        [
            "ms=\(measurementSetPath)",
            "imagename=\(outputPrefix)",
            "field=\(selectedField ?? "all")",
            "phasecenter=\(phaseCenterField ?? "auto")",
            "spw=\(selectedSpectralWindow ?? "all")",
            "data=\(dataColumn)",
            "plane=\(correlation ?? "Stokes I")",
            "image=\(imageSize)x\(imageHeight) px",
            "cell=\(cellArcsec) arcsec",
            "weighting=\(weighting.rawValue)",
            "dirty-only=\(dirtyOnly)"
        ].joined(separator: ", ")
    }

    public func validationErrors() -> [String] {
        var errors: [String] = []
        if measurementSetPath.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            errors.append("MeasurementSet path is required.")
        }
        if outputPrefix.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            errors.append("Output image prefix is required.")
        }
        if imageSize <= 0 {
            errors.append("Image width must be positive.")
        }
        if imageHeight <= 0 {
            errors.append("Image height must be positive.")
        }
        if imageSize != imageHeight {
            errors.append("Rectangular image sizes are not supported by the current casars-imager backend yet.")
        }
        if !(cellArcsec.isFinite && cellArcsec > 0) {
            errors.append("Cell size must be a positive finite arcsecond value.")
        }
        if parseOptionalNonNegativeInt(channelStart) == nil && !channelStart.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            errors.append("Channel start must be a non-negative integer.")
        }
        if parseOptionalPositiveInt(channelCount) == nil && !channelCount.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            errors.append("Channel count must be a positive integer.")
        }
        return errors
    }

    public static func imageDimensionAssessment(_ value: Int) -> DirtyImagingDimensionAssessment {
        guard value > 0 else {
            return DirtyImagingDimensionAssessment(
                value: value,
                severity: .terrible,
                adjustedValue: 512,
                message: "must be positive"
            )
        }

        let adjusted = nearestNiceImageDimension(to: value)
        if isNiceImageDimension(value) {
            return DirtyImagingDimensionAssessment(
                value: value,
                severity: .good,
                adjustedValue: value,
                message: "FFT-friendly"
            )
        }

        let largestFactor = largestPrimeFactor(value)
        let severity: DirtyImagingDimensionSeverity = largestFactor >= 127 ? .terrible : .warning
        let message = severity == .terrible ? "large prime factor" : "awkward FFT factors"
        return DirtyImagingDimensionAssessment(
            value: value,
            severity: severity,
            adjustedValue: adjusted,
            message: message
        )
    }

    public static func nearestNiceImageDimension(to value: Int) -> Int {
        guard value > 0 else { return 512 }
        let clamped = min(max(value, 32), 8192)
        if isNiceImageDimension(clamped) {
            return clamped
        }

        for candidate in clamped...8192 {
            if isNiceImageDimension(candidate) {
                return candidate
            }
        }

        for delta in 1...8192 {
            let lower = clamped - delta
            if lower >= 32, isNiceImageDimension(lower) {
                return lower
            }
        }

        return clamped
    }
}

private func isNiceImageDimension(_ value: Int) -> Bool {
    guard value > 0 else { return false }
    var remainder = value
    for factor in [2, 3, 5] {
        while remainder % factor == 0 {
            remainder /= factor
        }
    }
    return remainder == 1
}

private func largestPrimeFactor(_ value: Int) -> Int {
    var remaining = abs(value)
    var largest = 1
    var factor = 2
    while factor * factor <= remaining {
        while remaining % factor == 0 {
            largest = factor
            remaining /= factor
        }
        factor += factor == 2 ? 1 : 2
    }
    if remaining > 1 {
        largest = remaining
    }
    return largest
}

public struct DirtyImagingTaskRequest: Codable, Equatable {
    public var runID: String
    public var parameters: DirtyImagingTaskParameters

    public init(runID: String, parameters: DirtyImagingTaskParameters) {
        self.runID = runID
        self.parameters = parameters
    }

    public func encodedImagerJSON() throws -> Data {
        let request = ImagerTaskRequestEnvelope(
            request: ImagerRunRequestPayload(
                measurementSet: parameters.measurementSetPath,
                imageName: parameters.outputPrefix,
                imageSize: parameters.imageSize,
                cellArcsec: parameters.cellArcsec,
                fieldIDs: parsedFieldIDs(),
                phasecenterField: parsedPhaseCenterField(),
                spwSelector: selectorToken(parameters.selectedSpectralWindow),
                channelStart: parseOptionalNonNegativeInt(parameters.channelStart),
                channelCount: parseOptionalPositiveInt(parameters.channelCount),
                dataColumn: parameters.dataColumn.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
                    ? nil
                    : parameters.dataColumn,
                correlation: selectorToken(parameters.correlation),
                weighting: ImagerWeightingPayload(weighting: parameters.weighting),
                niter: 0,
                dirtyOnly: parameters.dirtyOnly,
                writePreviewPngs: true
            )
        )
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        return try encoder.encode(request)
    }

    private func parsedFieldIDs() -> [Int]? {
        guard let selectedField = selectorToken(parameters.selectedField),
              let fieldID = Int(selectedField)
        else {
            return nil
        }
        return [fieldID]
    }

    private func parsedPhaseCenterField() -> Int? {
        guard let phaseCenterField = selectorToken(parameters.phaseCenterField) else {
            return nil
        }
        return Int(phaseCenterField)
    }
}

public struct DirtyImagingArtifact: Codable, Equatable, Identifiable {
    public var kind: String
    public var label: String
    public var path: String
    public var exists: Bool
    public var previewPngPath: String?
    public var previewPngExists: Bool

    public var id: String { path }
}

public struct DirtyImagingRunReport: Codable, Equatable {
    public var warnings: [String]
    public var griddedSamples: UInt64
    public var majorCycles: UInt64
    public var minorIterations: UInt64
    public var channelCount: Int

    public var summary: String {
        "\(griddedSamples) gridded samples, \(majorCycles) major cycles, \(minorIterations) minor iterations"
    }
}

public struct DirtyImagingTaskResult: Codable, Equatable {
    public var request: DirtyImagingTaskRequest
    public var report: DirtyImagingRunReport
    public var artifacts: [DirtyImagingArtifact]
    public var requestJSONPath: String
    public var stdoutPath: String
    public var stderrPath: String
    public var protocolSummary: String
    public var diagnostics: [String]

    public var outputPaths: [String] {
        var paths = artifacts.flatMap { artifact -> [String] in
            var values = [artifact.path]
            if let preview = artifact.previewPngPath {
                values.append(preview)
            }
            return values
        }
        paths.append(contentsOf: [requestJSONPath, stdoutPath, stderrPath])
        return paths
    }
}

public struct DirtyImagingTaskFailure: Error, Codable, Equatable {
    public var message: String
    public var requestJSONPath: String?
    public var stdoutPath: String?
    public var stderrPath: String?
    public var diagnostics: [String]

    public init(
        message: String,
        requestJSONPath: String? = nil,
        stdoutPath: String? = nil,
        stderrPath: String? = nil,
        diagnostics: [String] = []
    ) {
        self.message = message
        self.requestJSONPath = requestJSONPath
        self.stdoutPath = stdoutPath
        self.stderrPath = stderrPath
        self.diagnostics = diagnostics
    }
}

public enum DirtyImagingTaskEvent {
    case succeeded(DirtyImagingTaskResult)
    case failed(DirtyImagingTaskFailure)
    case cancelled(DirtyImagingTaskFailure)
}

public protocol DirtyImagingTaskExecution {
    func cancel()
}

public protocol DirtyImagingTaskClient {
    func startDirtyImaging(
        request: DirtyImagingTaskRequest,
        eventHandler: @escaping (DirtyImagingTaskEvent) -> Void
    ) throws -> DirtyImagingTaskExecution
}

public final class ProcessDirtyImagingTaskClient: DirtyImagingTaskClient {
    private let executablePath: String?
    private let queue: DispatchQueue

    public init(
        executablePath: String? = nil,
        queue: DispatchQueue = DispatchQueue(label: "casars.mac.dirty-imaging-task", qos: .userInitiated)
    ) {
        self.executablePath = executablePath ?? Self.resolvedExecutablePath()
        self.queue = queue
    }

    static func resolvedExecutablePath(
        environment: [String: String] = ProcessInfo.processInfo.environment,
        bundleExecutableURL: URL? = Bundle.main.executableURL,
        isExecutable: (String) -> Bool = { FileManager.default.isExecutableFile(atPath: $0) }
    ) -> String? {
        if let path = environment["CASARS_IMAGER_BIN"], !path.isEmpty {
            return path
        }

        let bundledPath = bundleExecutableURL?
            .deletingLastPathComponent()
            .appendingPathComponent("casars-imager")
            .path
        if let bundledPath, isExecutable(bundledPath) {
            return bundledPath
        }

        return nil
    }

    public func startDirtyImaging(
        request: DirtyImagingTaskRequest,
        eventHandler: @escaping (DirtyImagingTaskEvent) -> Void
    ) throws -> DirtyImagingTaskExecution {
        let execution = ProcessDirtyImagingTaskExecution()
        queue.async {
            self.run(request: request, execution: execution, eventHandler: eventHandler)
        }
        return execution
    }

    private func run(
        request: DirtyImagingTaskRequest,
        execution: ProcessDirtyImagingTaskExecution,
        eventHandler: @escaping (DirtyImagingTaskEvent) -> Void
    ) {
        let outputPrefix = URL(fileURLWithPath: request.parameters.outputPrefix)
        let outputDirectory = outputPrefix.deletingLastPathComponent()
        let requestURL = outputPrefix.appendingPathExtension("casars-request.json")
        let stdoutURL = outputPrefix.appendingPathExtension("casars-result.json")
        let stderrURL = outputPrefix.appendingPathExtension("casars-stderr.log")

        do {
            try FileManager.default.createDirectory(at: outputDirectory, withIntermediateDirectories: true)
            try request.encodedImagerJSON().write(to: requestURL, options: .atomic)

            let protocolOutput = try runProcess(["--protocol-info"], execution: execution)
            if execution.isCancelled {
                eventHandler(.cancelled(cancelledFailure(requestURL: requestURL, stdoutURL: stdoutURL, stderrURL: stderrURL)))
                return
            }
            guard protocolOutput.exitCode == 0 else {
                eventHandler(.failed(DirtyImagingTaskFailure(
                    message: "casars-imager --protocol-info failed with exit \(protocolOutput.exitCode)",
                    requestJSONPath: requestURL.path,
                    stdoutPath: stdoutURL.path,
                    stderrPath: stderrURL.path,
                    diagnostics: [protocolOutput.stderr, protocolOutput.stdout].filter { !$0.isEmpty }
                )))
                return
            }

            let output = try runProcess(["--json-run", requestURL.path], execution: execution)
            try output.stdout.data(using: .utf8)?.write(to: stdoutURL, options: .atomic)
            try output.stderr.data(using: .utf8)?.write(to: stderrURL, options: .atomic)

            if execution.isCancelled {
                eventHandler(.cancelled(cancelledFailure(requestURL: requestURL, stdoutURL: stdoutURL, stderrURL: stderrURL)))
                return
            }
            guard output.exitCode == 0 else {
                eventHandler(.failed(DirtyImagingTaskFailure(
                    message: "casars-imager --json-run failed with exit \(output.exitCode)",
                    requestJSONPath: requestURL.path,
                    stdoutPath: stdoutURL.path,
                    stderrPath: stderrURL.path,
                    diagnostics: [output.stderr, output.stdout].filter { !$0.isEmpty }
                )))
                return
            }

            let taskResult = try parseResult(
                output.stdout,
                request: request,
                protocolSummary: protocolOutput.stdout,
                requestURL: requestURL,
                stdoutURL: stdoutURL,
                stderrURL: stderrURL
            )
            eventHandler(.succeeded(taskResult))
        } catch {
            if execution.isCancelled {
                eventHandler(.cancelled(cancelledFailure(requestURL: requestURL, stdoutURL: stdoutURL, stderrURL: stderrURL)))
            } else {
                eventHandler(.failed(DirtyImagingTaskFailure(
                    message: "\(error)",
                    requestJSONPath: requestURL.path,
                    stdoutPath: stdoutURL.path,
                    stderrPath: stderrURL.path
                )))
            }
        }
    }

    private func runProcess(_ arguments: [String], execution: ProcessDirtyImagingTaskExecution) throws -> ProcessOutput {
        if execution.isCancelled {
            return ProcessOutput(exitCode: -1, stdout: "", stderr: "cancelled before launch")
        }

        let process = Process()
        if let executablePath, !executablePath.isEmpty {
            process.executableURL = URL(fileURLWithPath: executablePath)
            process.arguments = arguments
        } else {
            process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
            process.arguments = ["casars-imager"] + arguments
        }

        let stdout = Pipe()
        let stderr = Pipe()
        process.standardOutput = stdout
        process.standardError = stderr
        guard execution.setProcess(process) else {
            return ProcessOutput(exitCode: -1, stdout: "", stderr: "cancelled before launch")
        }
        do {
            try process.run()
            process.waitUntilExit()
            execution.clearProcess(process)
        } catch {
            execution.clearProcess(process)
            throw error
        }

        return ProcessOutput(
            exitCode: process.terminationStatus,
            stdout: String(decoding: stdout.fileHandleForReading.readDataToEndOfFile(), as: UTF8.self),
            stderr: String(decoding: stderr.fileHandleForReading.readDataToEndOfFile(), as: UTF8.self)
        )
    }

    private func parseResult(
        _ text: String,
        request: DirtyImagingTaskRequest,
        protocolSummary: String,
        requestURL: URL,
        stdoutURL: URL,
        stderrURL: URL
    ) throws -> DirtyImagingTaskResult {
        let data = Data(text.utf8)
        let envelope = try JSONDecoder().decode(ImagerTaskResultEnvelope.self, from: data)
        return DirtyImagingTaskResult(
            request: request,
            report: DirtyImagingRunReport(
                warnings: envelope.result.run.warnings,
                griddedSamples: envelope.result.run.griddedSamples,
                majorCycles: envelope.result.run.majorCycles,
                minorIterations: envelope.result.run.minorIterations,
                channelCount: envelope.result.run.channels.count
            ),
            artifacts: envelope.result.artifacts.map(DirtyImagingArtifact.init(artifact:)),
            requestJSONPath: requestURL.path,
            stdoutPath: stdoutURL.path,
            stderrPath: stderrURL.path,
            protocolSummary: protocolSummary.trimmingCharacters(in: .whitespacesAndNewlines),
            diagnostics: envelope.result.run.warnings
        )
    }

    private func cancelledFailure(requestURL: URL, stdoutURL: URL, stderrURL: URL) -> DirtyImagingTaskFailure {
        DirtyImagingTaskFailure(
            message: "Dirty imaging run was cancelled.",
            requestJSONPath: requestURL.path,
            stdoutPath: stdoutURL.path,
            stderrPath: stderrURL.path
        )
    }
}

private final class ProcessDirtyImagingTaskExecution: DirtyImagingTaskExecution {
    private let lock = NSLock()
    private var process: Process?
    private var cancelled = false

    var isCancelled: Bool {
        lock.lock()
        defer { lock.unlock() }
        return cancelled
    }

    func cancel() {
        lock.lock()
        cancelled = true
        let process = process
        lock.unlock()
        process?.terminate()
    }

    func setProcess(_ process: Process) -> Bool {
        lock.lock()
        if cancelled {
            lock.unlock()
            return false
        }
        self.process = process
        lock.unlock()
        return true
    }

    func clearProcess(_ process: Process) {
        lock.lock()
        if self.process === process {
            self.process = nil
        }
        lock.unlock()
    }
}

private struct ProcessOutput {
    var exitCode: Int32
    var stdout: String
    var stderr: String
}

private struct ImagerTaskRequestEnvelope: Encodable {
    var kind = "run"
    var request: ImagerRunRequestPayload
}

private struct ImagerRunRequestPayload: Encodable {
    var measurementSet: String
    var imageName: String
    var imageSize: Int
    var cellArcsec: Double
    var fieldIDs: [Int]?
    var phasecenterField: Int?
    var spwSelector: String?
    var channelStart: Int?
    var channelCount: Int?
    var dataColumn: String?
    var correlation: String?
    var weighting: ImagerWeightingPayload
    var niter: Int
    var dirtyOnly: Bool
    var writePreviewPngs: Bool

    enum CodingKeys: String, CodingKey {
        case measurementSet = "measurement_set"
        case imageName = "image_name"
        case imageSize = "image_size"
        case cellArcsec = "cell_arcsec"
        case fieldIDs = "field_ids"
        case phasecenterField = "phasecenter_field"
        case spwSelector = "spw_selector"
        case channelStart = "channel_start"
        case channelCount = "channel_count"
        case dataColumn = "data_column"
        case correlation
        case weighting
        case niter
        case dirtyOnly = "dirty_only"
        case writePreviewPngs = "write_preview_pngs"
    }
}

private struct ImagerWeightingPayload: Encodable {
    var kind: String
    var robust: Double?

    init(weighting: DirtyImagingWeighting) {
        switch weighting {
        case .natural:
            kind = "natural"
            robust = nil
        case .uniform:
            kind = "uniform"
            robust = nil
        case .briggs:
            kind = "briggs"
            robust = 0.5
        }
    }
}

private struct ImagerTaskResultEnvelope: Decodable {
    var kind: String
    var result: ImagerRunResultPayload
}

private struct ImagerRunResultPayload: Decodable {
    var run: ImagerRunReportPayload
    var artifacts: [ImagerArtifactPayload]
}

private struct ImagerRunReportPayload: Decodable {
    var warnings: [String]
    var griddedSamples: UInt64
    var majorCycles: UInt64
    var minorIterations: UInt64
    var channels: [ImagerChannelPayload]

    enum CodingKeys: String, CodingKey {
        case warnings
        case griddedSamples = "gridded_samples"
        case majorCycles = "major_cycles"
        case minorIterations = "minor_iterations"
        case channels
    }
}

private struct ImagerChannelPayload: Decodable {}

private struct ImagerArtifactPayload: Decodable {
    var kind: String
    var label: String
    var path: String
    var exists: Bool
    var previewPngPath: String?
    var previewPngExists: Bool

    enum CodingKeys: String, CodingKey {
        case kind
        case label
        case path
        case exists
        case previewPngPath = "preview_png_path"
        case previewPngExists = "preview_png_exists"
    }
}

private extension DirtyImagingArtifact {
    init(artifact: ImagerArtifactPayload) {
        self.init(
            kind: artifact.kind,
            label: artifact.label,
            path: artifact.path,
            exists: artifact.exists,
            previewPngPath: artifact.previewPngPath,
            previewPngExists: artifact.previewPngExists
        )
    }
}

private func selectorToken(_ value: String?) -> String? {
    guard let value = value?.trimmingCharacters(in: .whitespacesAndNewlines),
          !value.isEmpty,
          value != "all"
    else {
        return nil
    }
    if value.hasPrefix("spw ") {
        let remainder = value.dropFirst(4)
        return String(remainder.prefix { $0.isNumber })
    }
    if let colon = value.firstIndex(of: ":") {
        return String(value[..<colon]).trimmingCharacters(in: .whitespacesAndNewlines)
    }
    return value
}

private func parseOptionalNonNegativeInt(_ value: String) -> Int? {
    let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !trimmed.isEmpty, let parsed = Int(trimmed), parsed >= 0 else {
        return nil
    }
    return parsed
}

private func parseOptionalPositiveInt(_ value: String) -> Int? {
    let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !trimmed.isEmpty, let parsed = Int(trimmed), parsed > 0 else {
        return nil
    }
    return parsed
}
