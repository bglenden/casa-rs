import Foundation

public protocol TaskExecution {
    func cancel()
}

public struct GenericTaskRequest: Equatable {
    public var runID: String
    public var task: TaskCatalogEntry
    public var schema: TaskUISchema
    public var values: [String: String]
    public var toggles: [String: Bool]
    public var workingDirectoryPath: String?

    public init(
        runID: String,
        task: TaskCatalogEntry,
        schema: TaskUISchema,
        values: [String: String],
        toggles: [String: Bool],
        workingDirectoryPath: String? = nil
    ) {
        self.runID = runID
        self.task = task
        self.schema = schema
        self.values = values
        self.toggles = toggles
        self.workingDirectoryPath = workingDirectoryPath
    }
}

public struct GenericTaskResult: Equatable {
    public var taskID: String
    public var arguments: [String]
    public var stdout: String
    public var stderr: String
    public var requestJSONPath: String?

    public init(
        taskID: String,
        arguments: [String],
        stdout: String,
        stderr: String,
        requestJSONPath: String? = nil
    ) {
        self.taskID = taskID
        self.arguments = arguments
        self.stdout = stdout
        self.stderr = stderr
        self.requestJSONPath = requestJSONPath
    }
}

public struct GenericTaskFailure: Error, Equatable {
    public var message: String
    public var diagnostics: [String]
}

public enum GenericTaskEvent {
    case progress(ImagerProgressSnapshot)
    case succeeded(GenericTaskResult)
    case failed(GenericTaskFailure)
    case cancelled(GenericTaskFailure)
}

public protocol GenericTaskClient {
    func startTask(
        request: GenericTaskRequest,
        eventHandler: @escaping (GenericTaskEvent) -> Void
    ) throws -> TaskExecution
}

private final class ImagerProgressJSONLTailer {
    private let url: URL
    private let runID: String
    private let queue: DispatchQueue
    private let eventHandler: (GenericTaskEvent) -> Void
    private var parser = ImagerProgressStderrParser()
    private var offset = 0
    private var timer: DispatchSourceTimer?

    init(
        path: String,
        runID: String,
        queue: DispatchQueue,
        eventHandler: @escaping (GenericTaskEvent) -> Void
    ) {
        self.url = URL(fileURLWithPath: path)
        self.runID = runID
        self.queue = queue
        self.eventHandler = eventHandler
    }

    func start() {
        let timer = DispatchSource.makeTimerSource(queue: queue)
        timer.schedule(deadline: .now() + .milliseconds(100), repeating: .milliseconds(100))
        timer.setEventHandler { [weak self] in
            self?.drainAvailable(state: .running, finish: false)
        }
        self.timer = timer
        timer.resume()
    }

    func stopAndDrain(state: TaskRunState) {
        queue.sync {
            timer?.cancel()
            timer = nil
            drainAvailable(state: state, finish: true)
        }
    }

    private func drainAvailable(state: TaskRunState, finish: Bool) {
        guard let data = try? Data(contentsOf: url) else {
            return
        }
        if offset > data.count {
            offset = 0
        }
        let chunk = data.dropFirst(offset)
        offset = data.count
        guard !chunk.isEmpty || finish else { return }
        var records: [ImagerProgressStderrRecord] = []
        if let text = String(data: chunk, encoding: .utf8), !text.isEmpty {
            records.append(contentsOf: parser.appendJSONLines(text, runID: runID, state: state))
        }
        if finish {
            records.append(contentsOf: parser.finishJSONLines(runID: runID, state: state))
        }
        for record in records {
            if case .progress(let snapshot) = record {
                eventHandler(.progress(snapshot))
            }
        }
    }
}

public struct ManagedImagingArtifact: Codable, Equatable, Identifiable {
    public var kind: String
    public var label: String
    public var path: String
    public var exists: Bool
    public var previewPngPath: String?
    public var previewPngExists: Bool

    public var id: String { path }

    enum CodingKeys: String, CodingKey {
        case kind
        case label
        case path
        case exists
        case previewPngPath = "preview_png_path"
        case previewPngExists = "preview_png_exists"
    }
}

public struct ManagedImagingRequest: Codable, Equatable {
    public var measurementSet: String
    public var imagename: String
    public var spectralMode: String
    public var weighting: String
    public var deconvolver: String
    public var imsize: Int
    public var cellArcsec: Double
    public var dirtyOnly: Bool
    public var outputChannels: Int

    enum CodingKeys: String, CodingKey {
        case measurementSet = "measurement_set"
        case imagename
        case spectralMode = "spectral_mode"
        case weighting
        case deconvolver
        case imsize
        case cellArcsec = "cell_arcsec"
        case dirtyOnly = "dirty_only"
        case outputChannels = "output_channels"
    }
}

public struct ManagedImagingRun: Codable, Equatable {
    public var warnings: [String]
    public var griddedSamples: UInt64
    public var majorCycles: UInt64
    public var minorIterations: UInt64
    public var channels: [ManagedImagingChannelRun]

    public var summary: String {
        "\(griddedSamples) gridded samples, \(majorCycles) major cycles, \(minorIterations) minor iterations"
    }

    enum CodingKeys: String, CodingKey {
        case warnings
        case griddedSamples = "gridded_samples"
        case majorCycles = "major_cycles"
        case minorIterations = "minor_iterations"
        case channels
    }
}

public struct ManagedImagingChannelRun: Codable, Equatable {
    public var channelIndex: Int?

    enum CodingKeys: String, CodingKey {
        case channelIndex = "channel_index"
    }
}

public struct ManagedImagingOutput: Codable, Equatable {
    public var request: ManagedImagingRequest
    public var run: ManagedImagingRun
    public var artifacts: [ManagedImagingArtifact]

    public var outputPaths: [String] {
        artifacts.flatMap { artifact -> [String] in
            var values = [artifact.path]
            if let preview = artifact.previewPngPath {
                values.append(preview)
            }
            return values
        }
    }
}

public final class ProcessGenericTaskClient: GenericTaskClient {
    private let queue: DispatchQueue

    public init(queue: DispatchQueue = DispatchQueue(label: "casars.mac.generic-task", qos: .userInitiated)) {
        self.queue = queue
    }

    public func startTask(
        request: GenericTaskRequest,
        eventHandler: @escaping (GenericTaskEvent) -> Void
    ) throws -> TaskExecution {
        var executionRequest = request
        let progressTelemetryPath = try Self.progressTelemetryPathIfNeeded(for: request)
        let requestJSONPath = try Self.saveJSONRequestIfNeeded(for: request)
        if let requestJSONPath {
            executionRequest.values["request_json"] = requestJSONPath
        }
        let arguments = if let requestJSONPath {
            ["--json-run", requestJSONPath] + Self.progressTelemetryArguments(progressTelemetryPath)
        } else {
            try Self.arguments(for: executionRequest, progressTelemetryPath: progressTelemetryPath)
        }
        let execution = ProcessTaskExecution()
        queue.async {
            do {
                try Self.createOutputParentDirectories(for: executionRequest)
                let isImagerTask = executionRequest.task.id == "imager"
                let usesProgressTelemetry = isImagerTask && progressTelemetryPath != nil
                var progressParser = ImagerProgressStderrParser()
                var progressDiagnostics: [String] = []
                let progressParserLock = NSLock()
                let progressParserQueue = isImagerTask
                    ? DispatchQueue(label: "casars.mac.imager-progress-parser", qos: .userInitiated)
                    : nil
                let handleProgressChunk: ((String) -> Void)? = isImagerTask && !usesProgressTelemetry ? { chunk in
                    progressParserLock.lock()
                    let records = progressParser.append(chunk, runID: executionRequest.runID, state: .running)
                    let snapshots = records.compactMap { record -> ImagerProgressSnapshot? in
                        if case .progress(let snapshot) = record {
                            return snapshot
                        }
                        return nil
                    }
                    progressDiagnostics.append(contentsOf: records.compactMap { record -> String? in
                        if case .diagnostic(let diagnostic) = record {
                            return diagnostic
                        }
                        return nil
                    })
                    progressParserLock.unlock()
                    for snapshot in snapshots {
                        eventHandler(.progress(snapshot))
                    }
                } : nil
                let progressTailer = progressTelemetryPath.map { path in
                    ImagerProgressJSONLTailer(
                        path: path,
                        runID: executionRequest.runID,
                        queue: progressParserQueue ?? DispatchQueue(label: "casars.mac.imager-progress-jsonl"),
                        eventHandler: eventHandler
                    )
                }
                progressTailer?.start()
                let output = try Self.runProcess(
                    binaryName: executionRequest.task.binaryName,
                    overrideEnv: executionRequest.task.overrideEnv,
                    arguments: arguments,
                    workingDirectoryPath: executionRequest.workingDirectoryPath,
                    execution: execution,
                    stderrChunkHandler: handleProgressChunk,
                    stderrChunkHandlerQueue: progressParserQueue,
                    storesStderr: !isImagerTask || usesProgressTelemetry
                )
                progressTailer?.stopAndDrain(state: .running)
                if isImagerTask && !usesProgressTelemetry {
                    progressParserQueue?.sync {}
                    progressParserLock.lock()
                    let finalRecords = progressParser.finish(runID: executionRequest.runID, state: .running)
                    let finalSnapshots = finalRecords.compactMap { record -> ImagerProgressSnapshot? in
                        if case .progress(let snapshot) = record {
                            return snapshot
                        }
                        return nil
                    }
                    progressDiagnostics.append(contentsOf: finalRecords.compactMap { record -> String? in
                        if case .diagnostic(let diagnostic) = record {
                            return diagnostic
                        }
                        return nil
                    })
                    progressParserLock.unlock()
                    for snapshot in finalSnapshots {
                        eventHandler(.progress(snapshot))
                    }
                }
                let stderr = if isImagerTask {
                    usesProgressTelemetry
                        ? Self.stderrWithoutImagerProgress(output.stderr)
                        : progressDiagnostics.joined(separator: "\n")
                } else {
                    output.stderr
                }
                if execution.isCancelled {
                    eventHandler(.cancelled(GenericTaskFailure(message: "Task was cancelled.", diagnostics: [stderr].filter { !$0.isEmpty })))
                } else if output.exitCode == 0 {
                    eventHandler(.succeeded(GenericTaskResult(
                        taskID: executionRequest.task.id,
                        arguments: arguments,
                        stdout: output.stdout,
                        stderr: stderr,
                        requestJSONPath: requestJSONPath
                    )))
                } else {
                    eventHandler(.failed(GenericTaskFailure(
                        message: "\(executionRequest.task.binaryName) exited with \(output.exitCode).",
                        diagnostics: [stderr, output.stdout].filter { !$0.isEmpty }
                    )))
                }
            } catch {
                eventHandler(.failed(GenericTaskFailure(message: "\(error)", diagnostics: [])))
            }
        }
        return execution
    }

    static func savedJSONRequestData(for request: GenericTaskRequest) throws -> Data {
        let excludedKeys = Set(["request_kind", "request_json"])
        var payload: [String: Any] = [:]
        for argument in request.schema.arguments.sorted(by: { $0.order < $1.order }) {
            guard !excludedKeys.contains(argument.id) else { continue }
            let value = (request.values[argument.id] ?? argument.default ?? "")
                .trimmingCharacters(in: .whitespacesAndNewlines)
            guard !value.isEmpty else { continue }
            payload[argument.id] = try jsonValue(from: value)
        }
        let envelope: [String: Any] = [
            "kind": request.values["request_kind"] ?? "family",
            "request": payload
        ]
        return try JSONSerialization.data(withJSONObject: envelope, options: [.prettyPrinted, .sortedKeys])
    }

    private static func saveJSONRequestIfNeeded(for request: GenericTaskRequest) throws -> String? {
        let requestKind = request.values["request_kind"] ?? request.schema.arguments
            .first { $0.id == "request_kind" }?
            .default
        guard requestKind == "family",
              let path = request.values["request_json"]?.trimmingCharacters(in: .whitespacesAndNewlines),
              !path.isEmpty
        else {
            return nil
        }
        let url = resolvedTaskPath(path, workingDirectoryPath: request.workingDirectoryPath)
        try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
        let data = try savedJSONRequestData(for: request)
        try data.write(to: url, options: .atomic)
        return url.path
    }

    private static func jsonValue(from value: String) throws -> Any {
        if let data = value.data(using: .utf8),
           let object = try? JSONSerialization.jsonObject(with: data),
           JSONSerialization.isValidJSONObject(object) {
            return object
        }
        if value == "true" {
            return true
        }
        if value == "false" {
            return false
        }
        if let integer = Int(value) {
            return integer
        }
        if let double = Double(value) {
            return double
        }
        return value
    }

    static func createOutputParentDirectories(for request: GenericTaskRequest) throws {
        for path in outputArgumentPaths(for: request) {
            let url = resolvedTaskPath(path, workingDirectoryPath: request.workingDirectoryPath)
            try FileManager.default.createDirectory(
                at: url.deletingLastPathComponent(),
                withIntermediateDirectories: true
            )
        }
    }

    static func outputArgumentPaths(for request: GenericTaskRequest) -> [String] {
        request.schema.arguments
            .filter { argument in
                !argument.hiddenInTUI
                    && ["option", "positional"].contains(argument.parser.kind)
                    && argumentLooksLikeOutput(argument)
            }
            .compactMap { argument in
                let value = (request.values[argument.id] ?? argument.default ?? "")
                    .trimmingCharacters(in: .whitespacesAndNewlines)
                return value.isEmpty ? nil : value
            }
    }

    private static func argumentLooksLikeOutput(_ argument: TaskUIArgument) -> Bool {
        if argument.parameterType?.hasPrefix("output_") == true {
            return true
        }
        return ["outfile", "output", "outputvis", "outputms", "fitsimage"].contains(argument.id)
            && argument.parameterType != "fits_path"
    }

    private static func resolvedTaskPath(_ path: String, workingDirectoryPath: String?) -> URL {
        let expanded = (path as NSString).expandingTildeInPath
        if expanded.hasPrefix("/") {
            return URL(fileURLWithPath: expanded).standardizedFileURL
        }
        guard let workingDirectoryPath, !workingDirectoryPath.isEmpty else {
            return URL(fileURLWithPath: expanded).standardizedFileURL
        }
        return URL(fileURLWithPath: workingDirectoryPath, isDirectory: true)
            .appendingPathComponent(expanded)
            .standardizedFileURL
    }

    static func arguments(for request: GenericTaskRequest) throws -> [String] {
        try arguments(for: request, progressTelemetryPath: nil)
    }

    static func arguments(for request: GenericTaskRequest, progressTelemetryPath: String?) throws -> [String] {
        var arguments: [String] = []
        for argument in request.schema.arguments.sorted(by: { $0.order < $1.order }) {
            let isHiddenAction = argument.hiddenInTUI && argument.parser.kind == "action"
            if isHiddenAction {
                continue
            }
            switch argument.parser.kind {
            case "option":
                let value = (request.values[argument.id] ?? argument.default ?? "")
                    .trimmingCharacters(in: .whitespacesAndNewlines)
                if value.isEmpty {
                    if argument.required {
                        throw GenericTaskFailure(message: "\(argument.label) is required.", diagnostics: [])
                    }
                    continue
                }
                guard let flag = argument.parser.flags?.first else { continue }
                arguments.append(flag)
                arguments.append(value)
            case "positional":
                let value = (request.values[argument.id] ?? argument.default ?? "")
                    .trimmingCharacters(in: .whitespacesAndNewlines)
                if value.isEmpty {
                    if argument.required {
                        throw GenericTaskFailure(message: "\(argument.label) is required.", diagnostics: [])
                    }
                    continue
                }
                arguments.append(value)
            case "toggle":
                let value = request.toggles[argument.id] ?? (argument.default == "true")
                if value, let flag = argument.parser.trueFlags?.first {
                    arguments.append(flag)
                } else if !value, let flag = argument.parser.falseFlags?.first {
                    arguments.append(flag)
                }
            default:
                continue
            }
        }
        if let managedOutput = request.schema.managedOutput {
            for injected in managedOutput.injectArguments {
                arguments.append(injected.flag)
                if let value = injected.value {
                    arguments.append(value)
                }
            }
        }
        if request.task.id == "imager" {
            arguments.append(contentsOf: [
                "--progress",
                "true",
                "--progress-max-uv-points",
                "16384",
                "--progress-min-interval-ms",
                "250"
            ])
            arguments.append(contentsOf: progressTelemetryArguments(progressTelemetryPath))
        }
        return arguments
    }

    private static func progressTelemetryArguments(_ path: String?) -> [String] {
        guard let path, !path.isEmpty else { return [] }
        return ["--progress-jsonl", path]
    }

    private static func progressTelemetryPathIfNeeded(for request: GenericTaskRequest) throws -> String? {
        guard request.task.id == "imager" else { return nil }
        let rootURL: URL
        if let workingDirectoryPath = request.workingDirectoryPath, !workingDirectoryPath.isEmpty {
            rootURL = URL(fileURLWithPath: workingDirectoryPath, isDirectory: true)
                .appendingPathComponent(".casa-rs", isDirectory: true)
                .appendingPathComponent("progress", isDirectory: true)
        } else {
            rootURL = FileManager.default.temporaryDirectory
                .appendingPathComponent("casars-mac-progress", isDirectory: true)
        }
        try FileManager.default.createDirectory(at: rootURL, withIntermediateDirectories: true)
        return rootURL
            .appendingPathComponent("\(request.runID)-imager-progress.jsonl")
            .standardizedFileURL
            .path
    }

    private static func runProcess(
        binaryName: String,
        overrideEnv: String,
        arguments: [String],
        workingDirectoryPath: String?,
        execution: ProcessTaskExecution,
        stderrChunkHandler: ((String) -> Void)? = nil,
        stderrChunkHandlerQueue: DispatchQueue? = nil,
        storesStderr: Bool = true
    ) throws -> ProcessOutput {
        if execution.isCancelled {
            return ProcessOutput(exitCode: -1, stdout: "", stderr: "cancelled before launch")
        }
        let process = Process()
        if let executablePath = resolvedExecutablePath(binaryName: binaryName, overrideEnv: overrideEnv) {
            process.executableURL = URL(fileURLWithPath: executablePath)
            process.arguments = arguments
        } else {
            process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
            process.arguments = [binaryName] + arguments
        }
        let stdout = Pipe()
        let stderr = Pipe()
        process.standardOutput = stdout
        process.standardError = stderr
        if let workingDirectoryPath, !workingDirectoryPath.isEmpty {
            process.currentDirectoryURL = URL(fileURLWithPath: workingDirectoryPath, isDirectory: true)
        }
        let stdoutCollector = ProcessPipeTextCollector(chunkHandler: nil)
        let stderrCollector = ProcessPipeTextCollector(
            chunkHandler: stderrChunkHandler,
            chunkHandlerQueue: stderrChunkHandlerQueue,
            storesText: storesStderr
        )
        guard execution.setProcess(process) else {
            return ProcessOutput(exitCode: -1, stdout: "", stderr: "cancelled before launch")
        }
        do {
            stdout.fileHandleForReading.readabilityHandler = { handle in
                stdoutCollector.append(handle.availableData)
            }
            stderr.fileHandleForReading.readabilityHandler = { handle in
                stderrCollector.append(handle.availableData)
            }
            try process.run()
            process.waitUntilExit()
            stdout.fileHandleForReading.readabilityHandler = nil
            stderr.fileHandleForReading.readabilityHandler = nil
            stdoutCollector.append(stdout.fileHandleForReading.readDataToEndOfFile())
            stderrCollector.append(stderr.fileHandleForReading.readDataToEndOfFile())
            execution.clearProcess(process)
        } catch {
            stdout.fileHandleForReading.readabilityHandler = nil
            stderr.fileHandleForReading.readabilityHandler = nil
            execution.clearProcess(process)
            throw error
        }
        return ProcessOutput(
            exitCode: process.terminationStatus,
            stdout: stdoutCollector.text(),
            stderr: stderrCollector.text()
        )
    }

    static func resolvedExecutablePath(
        binaryName: String,
        overrideEnv: String,
        environment: [String: String] = ProcessInfo.processInfo.environment,
        bundleExecutableURL: URL? = Bundle.main.executableURL,
        currentDirectoryPath: String = FileManager.default.currentDirectoryPath,
        isExecutable: (String) -> Bool = { FileManager.default.isExecutableFile(atPath: $0) }
    ) -> String? {
        if let path = environment[overrideEnv], !path.isEmpty {
            return path
        }
        if let bundled = bundleExecutableURL?
            .deletingLastPathComponent()
            .appendingPathComponent(binaryName)
            .path,
           isExecutable(bundled) {
            return bundled
        }
        if let repoRoot = environment["CASA_RS_REPO_ROOT"], !repoRoot.isEmpty {
            for profile in ["release", "debug"] {
                let candidate = URL(fileURLWithPath: repoRoot, isDirectory: true)
                    .appendingPathComponent("target/\(profile)/\(binaryName)")
                    .path
                if isExecutable(candidate) {
                    return candidate
                }
            }
        }
        var cursor = URL(fileURLWithPath: currentDirectoryPath, isDirectory: true)
        for _ in 0..<6 {
            for profile in ["release", "debug"] {
                let candidate = cursor.appendingPathComponent("target/\(profile)/\(binaryName)").path
                if isExecutable(candidate) {
                    return candidate
                }
            }
            let parent = cursor.deletingLastPathComponent()
            if parent.path == cursor.path {
                break
            }
            cursor = parent
        }
        return nil
    }

    private static func stderrWithoutImagerProgress(_ stderr: String) -> String {
        stderr
            .split(separator: "\n", omittingEmptySubsequences: false)
            .filter { !$0.hasPrefix(imagerProgressStderrPrefix) }
            .joined(separator: "\n")
            .trimmingCharacters(in: .whitespacesAndNewlines)
    }
}

private final class ProcessTaskExecution: TaskExecution {
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

private final class ProcessPipeTextCollector {
    private let lock = NSLock()
    private var data = Data()
    private let chunkHandler: ((String) -> Void)?
    private let chunkHandlerQueue: DispatchQueue?
    private let storesText: Bool

    init(
        chunkHandler: ((String) -> Void)?,
        chunkHandlerQueue: DispatchQueue? = nil,
        storesText: Bool = true
    ) {
        self.chunkHandler = chunkHandler
        self.chunkHandlerQueue = chunkHandlerQueue
        self.storesText = storesText
    }

    func append(_ newData: Data) {
        guard !newData.isEmpty else { return }
        if storesText {
            lock.lock()
            data.append(newData)
            lock.unlock()
        }
        guard let chunkHandler else { return }
        if let chunkHandlerQueue {
            let chunk = newData
            chunkHandlerQueue.async {
                chunkHandler(String(decoding: chunk, as: UTF8.self))
            }
        } else {
            chunkHandler(String(decoding: newData, as: UTF8.self))
        }
    }

    func text() -> String {
        lock.lock()
        defer { lock.unlock() }
        return String(decoding: data, as: UTF8.self)
    }
}

private struct ProcessOutput {
    var exitCode: Int32
    var stdout: String
    var stderr: String
}
