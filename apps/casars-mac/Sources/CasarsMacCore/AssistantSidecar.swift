import Darwin
import Foundation

package struct AssistantSidecarConfiguration: Equatable {
    package var nodeExecutable: String
    package var entrypoint: String
    package var fixtureMode: Bool

    package init(nodeExecutable: String, entrypoint: String, fixtureMode: Bool = false) {
        self.nodeExecutable = nodeExecutable
        self.entrypoint = entrypoint
        self.fixtureMode = fixtureMode
    }

    package static func discover(environment: [String: String] = ProcessInfo.processInfo.environment) throws -> Self {
        let fileManager = FileManager.default
        let home = fileManager.homeDirectoryForCurrentUser
        let bundled = Bundle.main.resourceURL?
            .appendingPathComponent("casars-assistant/runtime/bin/node").path
        let pathCandidates = (environment["PATH"] ?? "")
            .split(separator: ":")
            .map { URL(fileURLWithPath: String($0)).appendingPathComponent("node").path }
        let nvmRoot = home.appendingPathComponent(".nvm/versions/node", isDirectory: true)
        let nvmCandidates = (try? fileManager.contentsOfDirectory(
            at: nvmRoot,
            includingPropertiesForKeys: nil,
            options: [.skipsHiddenFiles]
        ))?.sorted { $0.lastPathComponent > $1.lastPathComponent }
            .map { $0.appendingPathComponent("bin/node").path } ?? []
        let nodeCandidates = [
            environment["CASA_RS_ASSISTANT_NODE"],
            bundled,
        ].compactMap { $0 } + pathCandidates + [
            "/opt/homebrew/bin/node",
            "/usr/local/bin/node",
            "/opt/local/bin/node",
            "/usr/bin/node",
            home.appendingPathComponent(".volta/bin/node").path,
            home.appendingPathComponent(".local/bin/node").path,
            home.appendingPathComponent(".nvm/current/bin/node").path,
            home.appendingPathComponent(".asdf/shims/node").path,
            home.appendingPathComponent(".local/share/mise/shims/node").path,
            home.appendingPathComponent(".local/share/fnm/aliases/default/bin/node").path,
            home.appendingPathComponent(".fnm/aliases/default/bin/node").path,
        ] + nvmCandidates
        let compatibleNodes = nodeCandidates
            .map { URL(fileURLWithPath: $0).standardizedFileURL.resolvingSymlinksInPath().path }
            .filter(fileManager.isExecutableFile(atPath:))
            .reduce(into: [String]()) { result, candidate in
                if !result.contains(candidate) { result.append(candidate) }
            }
        guard let node = compatibleNodes.first(where: { nodeVersionIsSupported($0) }) else {
            throw AssistantSidecarError.unavailable(
                "Node.js 22.19 or newer was not found in the app bundle, CASA_RS_ASSISTANT_NODE, or PATH"
            )
        }
        let entryCandidates = [
            environment["CASA_RS_ASSISTANT_ENTRYPOINT"],
            URL(fileURLWithPath: fileManager.currentDirectoryPath)
                .appendingPathComponent("apps/casars-assistant/dist/main.js").path,
            Bundle.main.resourceURL?.appendingPathComponent("casars-assistant/dist/main.js").path,
        ].compactMap { $0 }
        guard let entrypoint = entryCandidates.first(where: fileManager.fileExists(atPath:)) else {
            throw AssistantSidecarError.unavailable(
                "The external casars-assistant adapter is not built; run `just assistant-test` or set CASA_RS_ASSISTANT_ENTRYPOINT"
            )
        }
        return Self(
            nodeExecutable: node,
            entrypoint: entrypoint,
            fixtureMode: environment["CASA_RS_ASSISTANT_FIXTURE"] == "1"
        )
    }

    private static func nodeVersionIsSupported(_ executable: String) -> Bool {
        let process = Process()
        let output = Pipe()
        process.executableURL = URL(fileURLWithPath: executable)
        process.arguments = ["--version"]
        process.standardOutput = output
        process.standardError = Pipe()
        process.environment = ["OPENSSL_CONF": "/dev/null"]
        guard (try? process.run()) != nil else { return false }
        process.waitUntilExit()
        guard process.terminationStatus == 0 else { return false }
        let rawVersion = String(
            decoding: output.fileHandleForReading.readDataToEndOfFile(),
            as: UTF8.self
        ).trimmingCharacters(in: .whitespacesAndNewlines)
        let version = rawVersion.hasPrefix("v") ? String(rawVersion.dropFirst()) : rawVersion
        let components = version.split(separator: ".").compactMap { Int($0) }
        guard let major = components.first else { return false }
        if major != 22 { return major > 22 }
        return (components.count > 1 ? components[1] : 0) >= 19
    }
}

package enum AssistantSidecarError: Error, Equatable {
    case unavailable(String)
    case sandboxUnavailable
    case invalidAbsolutePath(String)
    case launchFailed(String)
    case startupTimeout
    case protocolFailure(String)
    case notRunning
    case exited(Int32)
}

package final class AssistantSidecar {
    package typealias EventHandler = ([String: Any]) -> Void
    package typealias StateHandler = (AssistantDiscussionActivity) -> Void
    package typealias CommandWriter = (FileHandle, Data) throws -> Void

    private let configuration: AssistantSidecarConfiguration
    private let commandWriter: CommandWriter
    private let queue = DispatchQueue(label: "casars.mac.assistant-sidecar")
    private let readQueue = DispatchQueue(label: "casars.mac.assistant-sidecar.read")
    private let stderrQueue = DispatchQueue(label: "casars.mac.assistant-sidecar.stderr")
    private var process: Process?
    private var stdin: FileHandle?
    private var runtimeDirectory: URL?
    private var readySemaphore = DispatchSemaphore(value: 0)
    private var startupError: Error?
    private var eventHandler: EventHandler?
    private var stateHandler: StateHandler?
    private let terminationLock = NSLock()
    private var expectedTerminationPIDs: Set<Int32> = []

    package init(
        configuration: AssistantSidecarConfiguration,
        commandWriter: @escaping CommandWriter = { handle, data in
            try handle.write(contentsOf: data)
        }
    ) {
        self.configuration = configuration
        self.commandWriter = commandWriter
    }

    deinit { terminate() }

    package func onEvent(_ handler: @escaping EventHandler) { eventHandler = handler }
    package func onStateChange(_ handler: @escaping StateHandler) { stateHandler = handler }

    package func prepare(_ completion: @escaping (Result<Void, Error>) -> Void) {
        queue.async { [weak self] in
            guard let self else { return }
            do {
                try self.ensureStarted()
                DispatchQueue.main.async { completion(.success(())) }
            } catch {
                DispatchQueue.main.async { completion(.failure(error)) }
            }
        }
    }

    package func send(_ value: [String: Any]) {
        queue.async { [weak self] in
            guard let self else { return }
            do {
                try self.ensureStarted()
                try self.write(value)
            } catch {
                self.publishFailure(error)
            }
        }
    }

    package func cancel(requestID: String) {
        send(["command": "cancel", "request_id": requestID])
    }

    package func restart() {
        queue.async { [weak self] in
            guard let self else { return }
            self.terminateLocked()
            do {
                try self.ensureStarted()
            } catch {
                self.publishFailure(error)
            }
        }
    }

    package func terminate() {
        queue.sync { terminateLocked() }
    }

    private func ensureStarted() throws {
        if process?.isRunning == true { return }
        guard FileManager.default.isExecutableFile(atPath: "/usr/bin/sandbox-exec") else {
            throw AssistantSidecarError.sandboxUnavailable
        }
        let node = try canonicalAbsoluteURL(configuration.nodeExecutable)
        let entrypoint = try canonicalAbsoluteURL(configuration.entrypoint)
        let adapterRoot = entrypoint.deletingLastPathComponent().deletingLastPathComponent()
        let runtimeCandidate = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-assistant-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: runtimeCandidate, withIntermediateDirectories: true)
        let runtime = runtimeCandidate.resolvingSymlinksInPath()
        runtimeDirectory = runtime
        publishState(.starting)
        readySemaphore = DispatchSemaphore(value: 0)
        startupError = nil

        let process = Process()
        let stdout = Pipe()
        let stderr = Pipe()
        let input = Pipe()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/sandbox-exec")
        process.arguments = [
            "-p", profile(
                node: node,
                adapterRoot: adapterRoot,
                runtime: runtime
            ),
            node.path,
            entrypoint.path,
        ]
        process.currentDirectoryURL = runtime
        process.environment = [
            "HOME": runtime.path,
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PATH": "/usr/bin:/bin",
            "TMPDIR": runtime.path,
            "XDG_CACHE_HOME": runtime.path,
            "OPENSSL_CONF": "/dev/null",
            "CASARS_ASSISTANT_FAKE": configuration.fixtureMode ? "1" : "0",
        ]
        process.standardInput = input
        process.standardOutput = stdout
        process.standardError = stderr
        process.terminationHandler = { [weak self] process in
            self?.sidecarExited(
                pid: process.processIdentifier,
                status: process.terminationStatus
            )
        }
        do {
            try process.run()
        } catch {
            throw AssistantSidecarError.launchFailed(error.localizedDescription)
        }
        self.process = process
        stdin = input.fileHandleForWriting
        readQueue.async { [weak self] in self?.readProtocol(stdout.fileHandleForReading) }
        stderrQueue.async { [weak self] in
            let handle = stderr.fileHandleForReading
            var data = Data()
            var truncated = false
            while true {
                let chunk = handle.availableData
                if chunk.isEmpty { break }
                let remaining = max(0, 1_048_576 - data.count)
                if remaining > 0 { data.append(chunk.prefix(remaining)) }
                if chunk.count > remaining { truncated = true }
            }
            guard !data.isEmpty else { return }
            self?.publishFailure(AssistantSidecarError.protocolFailure(
                String(decoding: data, as: UTF8.self)
                    + (truncated ? "\n[sidecar stderr truncated]" : "")
            ))
        }
        try write([
            "command": "hello",
            "request_id": "startup",
            "protocol_version": 1,
            "policy": [
                "provider_network_only": true,
                "project_filesystem": false,
                "shell": false,
                "python": false,
                "direct_host_tools": false,
            ],
        ])
        guard readySemaphore.wait(timeout: .now() + 10) == .success else {
            terminateLocked()
            throw AssistantSidecarError.startupTimeout
        }
        if let startupError {
            terminateLocked()
            throw startupError
        }
    }

    private func write(_ value: [String: Any]) throws {
        guard JSONSerialization.isValidJSONObject(value) else {
            throw AssistantSidecarError.protocolFailure("outbound command is not JSON")
        }
        guard let stdin else { throw AssistantSidecarError.notRunning }
        let data = try JSONSerialization.data(withJSONObject: value) + Data([0x0A])
        try commandWriter(stdin, data)
    }

    private func readProtocol(_ handle: FileHandle) {
        let maximumLineBytes = 1_048_576
        var buffer = Data()
        while true {
            let data = handle.availableData
            if data.isEmpty { break }
            buffer.append(data)
            if buffer.count > maximumLineBytes, !buffer.contains(0x0A) {
                publishFailure(AssistantSidecarError.protocolFailure(
                    "sidecar protocol line exceeded \(maximumLineBytes) bytes"
                ))
                restart()
                return
            }
            while let newline = buffer.firstIndex(of: 0x0A) {
                let line = buffer[..<newline]
                buffer.removeSubrange(...newline)
                guard line.count <= maximumLineBytes else {
                    publishFailure(AssistantSidecarError.protocolFailure(
                        "sidecar protocol line exceeded \(maximumLineBytes) bytes"
                    ))
                    restart()
                    return
                }
                handleProtocolLine(Data(line))
            }
        }
    }

    private func handleProtocolLine(_ data: Data) {
        guard let value = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let event = value["event"] as? String
        else {
            publishFailure(AssistantSidecarError.protocolFailure(String(decoding: data, as: UTF8.self)))
            return
        }
        if event == "ready", value["request_id"] as? String == "startup" {
            guard value["protocol_version"] as? Int == 1,
                  let policy = value["policy"] as? [String: Any],
                  policy["provider_network_only"] as? Bool == true,
                  policy["project_filesystem"] as? Bool == false,
                  policy["shell"] as? Bool == false,
                  policy["python"] as? Bool == false,
                  policy["direct_host_tools"] as? Bool == false
            else {
                startupError = AssistantSidecarError.protocolFailure(
                    "sidecar did not attest the required constrained policy"
                )
                readySemaphore.signal()
                return
            }
            publishState(.ready)
            readySemaphore.signal()
        }
        DispatchQueue.main.async { [eventHandler] in eventHandler?(value) }
    }

    private func terminateLocked() {
        guard let process else { return }
        _ = terminationLock.withLock { expectedTerminationPIDs.insert(process.processIdentifier) }
        if process.isRunning {
            try? write(["command": "shutdown", "request_id": "shutdown"])
            let deadline = Date().addingTimeInterval(0.4)
            while process.isRunning, Date() < deadline { usleep(10_000) }
            if process.isRunning { kill(process.processIdentifier, SIGKILL) }
        }
        stdin?.closeFile()
        stdin = nil
        self.process = nil
        if let runtimeDirectory { try? FileManager.default.removeItem(at: runtimeDirectory) }
        runtimeDirectory = nil
        publishState(.unavailable)
    }

    private func sidecarExited(pid: Int32, status: Int32) {
        let expected = terminationLock.withLock { expectedTerminationPIDs.remove(pid) != nil }
        if expected { return }
        if process?.processIdentifier == pid {
            process = nil
            stdin = nil
        }
        publishState(status == 0 ? .unavailable : .restartRequired)
        if status != 0 { publishFailure(AssistantSidecarError.exited(status)) }
    }

    private func publishState(_ state: AssistantDiscussionActivity) {
        DispatchQueue.main.async { [stateHandler] in stateHandler?(state) }
    }

    private func publishFailure(_ error: Error) {
        DispatchQueue.main.async { [eventHandler] in
            eventHandler?([
                "event": "error",
                "request_id": "host",
                "error": [
                    "code": "host_sidecar_error",
                    "message": String(describing: error),
                    "retryable": false,
                ],
            ])
        }
    }

    private func profile(node: URL, adapterRoot: URL, runtime: URL) -> String {
        let nodeRuntimeRoots = Self.nodeRuntimeReadRoots(node)
        let hostRoots = [
            adapterRoot,
            runtime,
            URL(fileURLWithPath: "/System/Library", isDirectory: true),
            URL(fileURLWithPath: "/usr/lib", isDirectory: true),
        ]
        let readableRoots = nodeRuntimeRoots + hostRoots
        let readableRules = readableRoots.map { root in
            let path = seatbeltLiteral(root.path)
            return """
            (allow file-read-data (subpath "\(path)"))
            (allow file-read-metadata (subpath "\(path)"))
            """
        }.joined(separator: "\n")
        let runtimeTraversalRules = nodeRuntimeRoots
            .flatMap(Self.metadataTraversalPaths)
            .map(seatbeltLiteral)
            .map { "(allow file-read* (literal \"\($0)\"))" }
            .joined(separator: "\n")
        let hostTraversalRules = hostRoots
            .flatMap(Self.metadataTraversalPaths)
            .map(seatbeltLiteral)
            .map { "(allow file-read-metadata (literal \"\($0)\"))" }
            .joined(separator: "\n")
        return """
        (version 1)
        (deny default)
        (allow process-exec (literal "\(seatbeltLiteral(node.path))"))
        (allow signal (target self))
        \(runtimeTraversalRules)
        \(hostTraversalRules)
        (allow file-read-metadata (literal "/dev"))
        (allow file-read-metadata (literal "/dev/null"))
        (allow file-read-metadata (literal "/dev/random"))
        (allow file-read-metadata (literal "/dev/urandom"))
        (allow file-read-data (literal "/dev/null"))
        (allow file-read-data (literal "/dev/random"))
        (allow file-read-data (literal "/dev/urandom"))
        \(readableRules)
        (allow file-write* (subpath "\(seatbeltLiteral(runtime.path))"))
        (allow mach-lookup)
        (allow sysctl-read)
        (allow ipc-posix-shm)
        (allow network-outbound)
        """
    }

    private static func nodeRuntimeReadRoots(_ executable: URL) -> [URL] {
        var pending = [executable]
        var inspected: Set<String> = []
        var roots: [URL] = [
            executable.deletingLastPathComponent(),
            executable.deletingLastPathComponent().deletingLastPathComponent(),
        ]
        while let binary = pending.popLast(), inspected.count < 128 {
            let path = binary.standardizedFileURL.resolvingSymlinksInPath().path
            guard inspected.insert(path).inserted else { continue }
            for dependency in linkedLibraries(of: URL(fileURLWithPath: path)) {
                let lexical = dependency.standardizedFileURL
                let canonical = lexical.resolvingSymlinksInPath()
                let dependencyPath = canonical.path
                guard !dependencyPath.hasPrefix("/System/") && !dependencyPath.hasPrefix("/usr/lib/") else {
                    continue
                }
                roots.append(lexical.deletingLastPathComponent())
                roots.append(canonical.deletingLastPathComponent())
                pending.append(canonical)
            }
        }
        var seen: Set<String> = []
        return roots.filter { seen.insert($0.path).inserted }
    }

    private static func metadataTraversalPaths(for url: URL) -> [String] {
        var current = url.standardizedFileURL
        var paths: [String] = []
        while true {
            paths.append(current.path)
            guard current.path != "/" else { break }
            current.deleteLastPathComponent()
        }
        return paths
    }

    private static func linkedLibraries(of binary: URL) -> [URL] {
        let process = Process()
        let output = Pipe()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/otool")
        process.arguments = ["-L", binary.path]
        process.standardOutput = output
        process.standardError = Pipe()
        guard (try? process.run()) != nil else { return [] }
        process.waitUntilExit()
        guard process.terminationStatus == 0 else { return [] }
        let loader = binary.deletingLastPathComponent()
        return String(decoding: output.fileHandleForReading.readDataToEndOfFile(), as: UTF8.self)
            .split(separator: "\n")
            .dropFirst()
            .compactMap { line -> URL? in
                guard let token = line.split(whereSeparator: \.isWhitespace).first else { return nil }
                let path = String(token)
                if path.hasPrefix("/") { return URL(fileURLWithPath: path) }
                if path.hasPrefix("@loader_path/") {
                    return loader.appendingPathComponent(String(path.dropFirst("@loader_path/".count)))
                }
                if path.hasPrefix("@rpath/") {
                    let suffix = String(path.dropFirst("@rpath/".count))
                    for candidate in [loader, loader.appendingPathComponent("../lib")] {
                        let value = candidate.appendingPathComponent(suffix).standardizedFileURL
                        if FileManager.default.fileExists(atPath: value.path) { return value }
                    }
                }
                return nil
            }
    }

    private func canonicalAbsoluteURL(_ path: String) throws -> URL {
        guard path.hasPrefix("/") else { throw AssistantSidecarError.invalidAbsolutePath(path) }
        return URL(fileURLWithPath: path).standardizedFileURL.resolvingSymlinksInPath()
    }

    private func seatbeltLiteral(_ value: String) -> String {
        value.replacingOccurrences(of: "\\", with: "\\\\")
            .replacingOccurrences(of: "\"", with: "\\\"")
    }
}
