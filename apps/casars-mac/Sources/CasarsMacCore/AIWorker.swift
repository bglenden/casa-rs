import CryptoKit
import Foundation

package struct AIWorkerApproval: Equatable {
    package let sourceSHA256: String

    package init(exactSource: String) {
        sourceSHA256 = Self.digest(exactSource)
    }

    package func approves(_ exactSource: String) -> Bool {
        sourceSHA256 == Self.digest(exactSource)
    }

    private static func digest(_ source: String) -> String {
        SHA256.hash(data: Data(source.utf8)).map { String(format: "%02x", $0) }.joined()
    }
}

package struct AIWorkerConfiguration {
    package var pythonExecutable: String
    package var readableScienceRoots: [String]
    package var stagingRoot: String
    package var deniedReadRoots: [String]

    package init(
        pythonExecutable: String,
        readableScienceRoots: [String],
        stagingRoot: String,
        deniedReadRoots: [String]? = nil
    ) {
        self.pythonExecutable = pythonExecutable
        self.readableScienceRoots = readableScienceRoots
        self.stagingRoot = stagingRoot
        self.deniedReadRoots = deniedReadRoots ?? Self.defaultCredentialRoots()
    }

    private static func defaultCredentialRoots() -> [String] {
        let home = FileManager.default.homeDirectoryForCurrentUser
        return [
            ".ssh", ".aws", ".azure", ".config", ".codex", ".netrc",
            "Library/Keychains",
            "Library/Application Support/Google/Chrome",
            "Library/Application Support/OpenAI",
        ].map { home.appendingPathComponent($0).path }
    }
}

package struct AIWorkerResult: Equatable {
    package var terminationStatus: Int32
    package var stdout: String
    package var stderr: String
}

package enum AIWorkerError: Error, Equatable {
    case approvalInvalidated
    case sandboxUnavailable
    case invalidAbsolutePath(String)
    case launchFailed(String)
}

/// Separate constrained authority for explicitly approved AI-proposed Python.
package struct SeatbeltAIWorker {
    package var configuration: AIWorkerConfiguration

    package init(configuration: AIWorkerConfiguration) {
        self.configuration = configuration
    }

    package func execute(
        exactSource: String,
        approval: AIWorkerApproval,
        isCancelled: @escaping () -> Bool = { false }
    ) throws -> AIWorkerResult {
        guard approval.approves(exactSource) else { throw AIWorkerError.approvalInvalidated }
        guard FileManager.default.isExecutableFile(atPath: "/usr/bin/sandbox-exec") else {
            throw AIWorkerError.sandboxUnavailable
        }
        let staging = try absoluteURL(configuration.stagingRoot)
        let sciencePaths = try configuration.readableScienceRoots.map(absoluteURL)
        let deniedReadRoots = try configuration.deniedReadRoots.map(absoluteURL)
        let runtimeRoots = try pythonRuntimeRoots(configuration.pythonExecutable)
        try FileManager.default.createDirectory(at: staging, withIntermediateDirectories: true)
        let home = staging.appendingPathComponent("home", isDirectory: true)
        let temporary = staging.appendingPathComponent("tmp", isDirectory: true)
        let cache = staging.appendingPathComponent("cache", isDirectory: true)
        for directory in [home, temporary, cache] {
            try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        }

        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/sandbox-exec")
        process.arguments = [
            "-p", profile(
                pythonExecutable: try standardizedAbsoluteURL(configuration.pythonExecutable),
                staging: seatbeltCanonicalURL(staging),
                readableSciencePaths: sciencePaths,
                runtimeRoots: runtimeRoots,
                deniedReadRoots: deniedReadRoots
            ),
            configuration.pythonExecutable,
            "-I", "-c", exactSource,
        ]
        process.currentDirectoryURL = staging
        process.environment = [
            "HOME": home.path,
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
            "CASARS_ARTIFACT_STAGING": staging.path,
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONPYCACHEPREFIX": cache.path,
            "TMPDIR": temporary.path,
            "XDG_CACHE_HOME": cache.path,
        ]
        let stdout = Pipe()
        let stderr = Pipe()
        process.standardOutput = stdout
        process.standardError = stderr
        do {
            try process.run()
        } catch {
            throw AIWorkerError.launchFailed(error.localizedDescription)
        }
        let stdoutDrain = AIWorkerPipeDrain(stdout.fileHandleForReading)
        let stderrDrain = AIWorkerPipeDrain(stderr.fileHandleForReading)
        let drains = DispatchGroup()
        stdoutDrain.start(in: drains)
        stderrDrain.start(in: drains)
        while process.isRunning {
            if isCancelled() {
                process.terminate()
                break
            }
            Thread.sleep(forTimeInterval: 0.05)
        }
        process.waitUntilExit()
        drains.wait()
        return AIWorkerResult(
            terminationStatus: process.terminationStatus,
            stdout: String(decoding: stdoutDrain.data, as: UTF8.self)
                + (stdoutDrain.wasTruncated ? "\n[stdout truncated at 16 MiB]" : ""),
            stderr: String(decoding: stderrDrain.data, as: UTF8.self)
                + (stderrDrain.wasTruncated ? "\n[stderr truncated at 16 MiB]" : "")
        )
    }

    private func profile(
        pythonExecutable: URL,
        staging: URL,
        readableSciencePaths: [URL],
        runtimeRoots: [URL],
        deniedReadRoots: [URL]
    ) -> String {
        let deniedReadRules = deniedReadRoots.flatMap { root in
            let path = seatbeltLiteral(seatbeltCanonicalURL(root).path)
            return [
                "(deny file-read-data (literal \"\(path)\"))",
                "(deny file-read-data (subpath \"\(path)\"))",
            ]
        }.joined(separator: "\n")
        let readableDirectoryURLs = [staging] + runtimeRoots
        let readableDirectoryRules = readableDirectoryURLs
            .map { seatbeltCanonicalURL($0).path }
            .map(seatbeltLiteral)
            .flatMap {
                [
                    "(allow file-read-data (subpath \"\($0)\"))",
                    "(allow file-read-metadata (subpath \"\($0)\"))",
                ]
            }
            .joined(separator: "\n")
        let scienceRules = readableSciencePaths.flatMap { url -> [String] in
            let canonical = seatbeltCanonicalURL(url)
            var isDirectory: ObjCBool = false
            guard FileManager.default.fileExists(atPath: canonical.path, isDirectory: &isDirectory) else {
                return []
            }
            let path = seatbeltLiteral(canonical.path)
            let filter = isDirectory.boolValue ? "subpath" : "literal"
            return [
                "(allow file-read-data (\(filter) \"\(path)\"))",
                "(allow file-read-metadata (\(filter) \"\(path)\"))",
            ]
        }.joined(separator: "\n")
        let metadataTraversalRules = (readableDirectoryURLs + readableSciencePaths)
            .flatMap { metadataTraversalPaths(for: seatbeltCanonicalURL($0)) }
            .map(seatbeltLiteral)
            .map { "(allow file-read-metadata (literal \"\($0)\"))" }
            .joined(separator: "\n")
        let runtimeDeviceRules = ["/dev/null", "/dev/random", "/dev/urandom"]
            .map(seatbeltLiteral)
            .flatMap {
                [
                    "(allow file-read-data (literal \"\($0)\"))",
                    "(allow file-read-metadata (literal \"\($0)\"))",
                ]
            }
            .joined(separator: "\n")
        return """
        (version 1)
        (deny default)
        (allow process-exec (literal "\(seatbeltLiteral(pythonExecutable.path))"))
        (allow signal (target self))
        ; Dyld and realpath need ancestor traversal, but directory enumeration
        ; and file contents remain deny-by-default outside approved roots.
        \(metadataTraversalRules)
        (allow file-read-metadata (literal "/dev"))
        \(readableDirectoryRules)
        \(scienceRules)
        \(runtimeDeviceRules)
        \(deniedReadRules)
        (allow mach-lookup)
        (allow sysctl-read)
        (allow ipc-posix-shm)
        (allow file-write* (subpath "\(seatbeltLiteral(staging.path))"))
        (deny network*)
        """
    }

    private func pythonRuntimeRoots(_ executable: String) throws -> [URL] {
        let configured = try standardizedAbsoluteURL(executable)
        let resolved = configured.resolvingSymlinksInPath()
        var roots = [runtimePrefix(for: configured), runtimePrefix(for: resolved)]
        for candidate in [configured, resolved] {
            if let developerRoot = developerRuntimeRoot(for: candidate) {
                roots.append(developerRoot)
            }
        }
        roots.append(contentsOf: [
            URL(fileURLWithPath: "/System/Library", isDirectory: true),
            URL(fileURLWithPath: "/usr/lib", isDirectory: true),
        ])
        var seen: Set<String> = []
        return roots.filter { seen.insert(seatbeltCanonicalURL($0).path).inserted }
    }

    private func runtimePrefix(for executable: URL) -> URL {
        executable.deletingLastPathComponent().deletingLastPathComponent()
    }

    private func developerRuntimeRoot(for executable: URL) -> URL? {
        let marker = "/Contents/Developer/"
        guard let range = executable.path.range(of: marker) else { return nil }
        return URL(
            fileURLWithPath: String(executable.path[..<range.upperBound].dropLast()),
            isDirectory: true
        )
    }

    private func absoluteURL(_ path: String) throws -> URL {
        try standardizedAbsoluteURL(path).resolvingSymlinksInPath()
    }

    private func standardizedAbsoluteURL(_ path: String) throws -> URL {
        guard path.hasPrefix("/") else { throw AIWorkerError.invalidAbsolutePath(path) }
        return URL(fileURLWithPath: path).standardizedFileURL
    }

    private func seatbeltCanonicalURL(_ url: URL) -> URL {
        if url.path == "/var" || url.path.hasPrefix("/var/") {
            return URL(fileURLWithPath: "/private\(url.path)")
        }
        if url.path == "/tmp" || url.path.hasPrefix("/tmp/") {
            return URL(fileURLWithPath: "/private\(url.path)")
        }
        return url
    }

    private func seatbeltLiteral(_ value: String) -> String {
        value.replacingOccurrences(of: "\\", with: "\\\\")
            .replacingOccurrences(of: "\"", with: "\\\"")
    }

    private func metadataTraversalPaths(for url: URL) -> [String] {
        var current = url.standardizedFileURL
        var paths: [String] = []
        while true {
            paths.append(current.path)
            guard current.path != "/" else { break }
            current.deleteLastPathComponent()
        }
        return paths
    }
}

private final class AIWorkerPipeDrain: @unchecked Sendable {
    private let handle: FileHandle
    private let lock = NSLock()
    private var collected = Data()
    private var truncated = false
    private let maximumBytes = 16 * 1_048_576

    init(_ handle: FileHandle) {
        self.handle = handle
    }

    var data: Data {
        lock.lock()
        defer { lock.unlock() }
        return collected
    }

    var wasTruncated: Bool {
        lock.lock()
        defer { lock.unlock() }
        return truncated
    }

    func start(in group: DispatchGroup) {
        group.enter()
        DispatchQueue.global(qos: .userInitiated).async { [self] in
            while true {
                let value = handle.availableData
                if value.isEmpty { break }
                lock.lock()
                let remaining = max(0, maximumBytes - collected.count)
                if remaining > 0 { collected.append(value.prefix(remaining)) }
                if value.count > remaining { truncated = true }
                lock.unlock()
            }
            group.leave()
        }
    }
}
