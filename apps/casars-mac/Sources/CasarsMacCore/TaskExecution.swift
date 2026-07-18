import Foundation

public protocol TaskExecution {
    func cancel()
}

public struct GenericTaskRequest: Equatable {
    public var runID: String
    public var task: ApplicationCatalogEntry
    public var parameterBundle: SurfaceParameterBundle?
    public var providerInvocation: SurfaceProviderInvocation
    public var parameterValues: [String: SurfaceParameterValue]
    public var workingDirectoryPath: String?

    public init(
        runID: String,
        task: ApplicationCatalogEntry,
        providerInvocation: SurfaceProviderInvocation,
        parameterBundle: SurfaceParameterBundle? = nil,
        parameterValues: [String: SurfaceParameterValue] = [:],
        workingDirectoryPath: String? = nil
    ) {
        self.runID = runID
        self.task = task
        self.parameterBundle = parameterBundle
        self.providerInvocation = providerInvocation
        self.parameterValues = parameterValues
        self.workingDirectoryPath = workingDirectoryPath
    }
}

public struct SurfaceProviderInvocation: Codable, Equatable, Sendable {
    public var args: [String]
    public var stdin: String?

    public init(args: [String], stdin: String? = nil) {
        self.args = args
        self.stdin = stdin
    }
}

public struct GenericTaskResult: Equatable {
    public var taskID: String
    public var arguments: [String]
    public var stdout: String
    public var stderr: String

    public init(
        taskID: String,
        arguments: [String],
        stdout: String,
        stderr: String
    ) {
        self.taskID = taskID
        self.arguments = arguments
        self.stdout = stdout
        self.stderr = stderr
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
    private let configuredLaunchMode: LaunchMode?

    public init(
        queue: DispatchQueue = DispatchQueue(label: "casars.mac.generic-task", qos: .userInitiated),
        launchMode: LaunchMode? = nil
    ) {
        self.queue = queue
        configuredLaunchMode = launchMode
    }

    public func startTask(
        request: GenericTaskRequest,
        eventHandler: @escaping (GenericTaskEvent) -> Void
    ) throws -> TaskExecution {
        let executionRequest = request
        let progressTelemetryPath = try Self.progressTelemetryPathIfNeeded(for: request)
        let arguments = try Self.arguments(for: executionRequest, progressTelemetryPath: progressTelemetryPath)
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
                    binaryName: executionRequest.task.executable,
                    cargoPackage: executionRequest.task.cargoPackage,
                    overrideEnv: executionRequest.task.overrideEnv,
                    arguments: arguments,
                    standardInput: executionRequest.providerInvocation.stdin,
                    workingDirectoryPath: executionRequest.workingDirectoryPath,
                    execution: execution,
                    launchMode: self.configuredLaunchMode,
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
                        stderr: stderr
                    )))
                } else {
                    eventHandler(.failed(GenericTaskFailure(
                        message: "\(executionRequest.task.executable) exited with \(output.exitCode).",
                        diagnostics: [stderr, output.stdout].filter { !$0.isEmpty }
                    )))
                }
            } catch {
                eventHandler(.failed(GenericTaskFailure(message: "\(error)", diagnostics: [])))
            }
        }
        return execution
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
        guard let bundle = request.parameterBundle else { return [] }
        return bundle.surface.bindings
            .filter { argumentIsOutput($0.name, bundle: bundle) }
            .compactMap { binding in
                guard let parameterValue = request.parameterValues[binding.name] else {
                    return nil
                }
                let value = parameterValue.displayText
                    .trimmingCharacters(in: .whitespacesAndNewlines)
                return value.isEmpty || value == "none" ? nil : value
            }
    }

    private static func argumentIsOutput(_ name: String, bundle: SurfaceParameterBundle?) -> Bool {
        guard let bundle,
              let binding = bundle.surface.bindings.first(where: { $0.name == name }),
              let concept = bundle.concept(for: name)
        else { return false }
        return binding.contextRole == "output_product" || concept.semanticRole == "output_data"
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
        var arguments = request.providerInvocation.args
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
            .appendingPathComponent(progressTelemetryFilename(runID: request.runID))
            .standardizedFileURL
            .path
    }

    static func progressTelemetryFilename(
        runID: String,
        processID: Int32 = ProcessInfo.processInfo.processIdentifier,
        nonce: UUID = UUID()
    ) -> String {
        "\(sanitizedTelemetryPathComponent(runID))-pid\(processID)-\(nonce.uuidString)-imager-progress.jsonl"
    }

    private static func sanitizedTelemetryPathComponent(_ value: String) -> String {
        let allowed = CharacterSet.alphanumerics.union(CharacterSet(charactersIn: "-_"))
        let scalars = value.unicodeScalars.map { scalar -> Character in
            allowed.contains(scalar) ? Character(scalar) : "-"
        }
        let sanitized = String(scalars).trimmingCharacters(in: CharacterSet(charactersIn: "-_"))
        return sanitized.isEmpty ? "run" : sanitized
    }

    private static func runProcess(
        binaryName: String,
        cargoPackage: String,
        overrideEnv: String,
        arguments: [String],
        standardInput: String?,
        workingDirectoryPath: String?,
        execution: ProcessTaskExecution,
        launchMode: LaunchMode?,
        stderrChunkHandler: ((String) -> Void)? = nil,
        stderrChunkHandlerQueue: DispatchQueue? = nil,
        storesStderr: Bool = true
    ) throws -> ProcessOutput {
        if execution.isCancelled {
            return ProcessOutput(exitCode: -1, stdout: "", stderr: "cancelled before launch")
        }
        let process = Process()
        switch try launchMode ?? Self.launchMode() {
        case .installedSuite:
            let executablePath = try installedExecutablePath(
                binaryName: binaryName,
                overrideEnv: overrideEnv
            )
            process.executableURL = URL(fileURLWithPath: executablePath)
            process.arguments = arguments
        case .developmentWorkspace:
            guard let repoRoot = ProcessInfo.processInfo.environment["CASA_RS_REPO_ROOT"],
                  !repoRoot.isEmpty else {
                throw GenericTaskFailure(
                    message: "Development launch requires CASA_RS_REPO_ROOT.",
                    diagnostics: ["Set CASA_RS_REPO_ROOT to the casa-rs checkout selected at application startup."]
                )
            }
            process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
            process.arguments = [
                ProcessInfo.processInfo.environment["CARGO"] ?? "cargo",
                "run", "--manifest-path", "\(repoRoot)/Cargo.toml", "-q",
                "-p", cargoPackage, "--bin", binaryName, "--",
            ] + arguments
        }
        let stdout = Pipe()
        let stderr = Pipe()
        let stdin = standardInput.map { _ in Pipe() }
        process.standardOutput = stdout
        process.standardError = stderr
        process.standardInput = stdin
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
            if let standardInput, let stdin {
                stdin.fileHandleForWriting.write(Data(standardInput.utf8))
                try? stdin.fileHandleForWriting.close()
            }
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

    public enum LaunchMode: String {
        case installedSuite = "installed_suite"
        case developmentWorkspace = "development_workspace"
    }

    static func launchMode(
        environment: [String: String] = ProcessInfo.processInfo.environment
    ) throws -> LaunchMode {
        if let configured = environment["CASARS_LAUNCH_MODE"] {
            guard let mode = LaunchMode(rawValue: configured) else {
                throw GenericTaskFailure(
                    message: "Invalid CASARS_LAUNCH_MODE \(configured).",
                    diagnostics: ["Expected installed_suite or development_workspace."]
                )
            }
            return mode
        }
        #if DEBUG
        return .developmentWorkspace
        #else
        return .installedSuite
        #endif
    }

    static func installedExecutablePath(
        binaryName: String,
        overrideEnv: String,
        environment: [String: String] = ProcessInfo.processInfo.environment,
        bundleExecutableURL: URL? = Bundle.main.executableURL,
        isExecutable: (String) -> Bool = { FileManager.default.isExecutableFile(atPath: $0) }
    ) throws -> String {
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
        throw GenericTaskFailure(
            message: "Installed-suite executable \(binaryName) is missing.",
            diagnostics: ["Install \(binaryName) beside the app launcher or set \(overrideEnv) to its installed path."]
        )
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
