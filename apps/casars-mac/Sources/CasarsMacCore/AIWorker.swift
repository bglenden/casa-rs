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

    package init(
        pythonExecutable: String,
        readableScienceRoots: [String],
        stagingRoot: String
    ) {
        self.pythonExecutable = pythonExecutable
        self.readableScienceRoots = readableScienceRoots
        self.stagingRoot = stagingRoot
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

    package func execute(exactSource: String, approval: AIWorkerApproval) throws -> AIWorkerResult {
        guard approval.approves(exactSource) else { throw AIWorkerError.approvalInvalidated }
        guard FileManager.default.isExecutableFile(atPath: "/usr/bin/sandbox-exec") else {
            throw AIWorkerError.sandboxUnavailable
        }
        let staging = try absoluteURL(configuration.stagingRoot)
        let roots = try configuration.readableScienceRoots.map(absoluteURL)
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
            "-p", profile(staging: seatbeltCanonicalURL(staging), readableRoots: roots),
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
        process.waitUntilExit()
        return AIWorkerResult(
            terminationStatus: process.terminationStatus,
            stdout: String(decoding: stdout.fileHandleForReading.readDataToEndOfFile(), as: UTF8.self),
            stderr: String(decoding: stderr.fileHandleForReading.readDataToEndOfFile(), as: UTF8.self)
        )
    }

    private func profile(staging: URL, readableRoots: [URL]) -> String {
        let readRules = readableRoots.map {
            "(allow file-read* (subpath \"\(seatbeltLiteral($0.path))\"))"
        }.joined(separator: "\n")
        return """
        (version 1)
        (deny default)
        (allow process-exec)
        (allow process-fork)
        (allow signal (target self))
        (allow file-read*)
        (allow mach-lookup)
        (allow sysctl-read)
        (allow ipc-posix-shm)
        \(readRules)
        (allow file-write* (subpath "\(seatbeltLiteral(staging.path))"))
        (deny network*)
        """
    }

    private func absoluteURL(_ path: String) throws -> URL {
        guard path.hasPrefix("/") else { throw AIWorkerError.invalidAbsolutePath(path) }
        return URL(fileURLWithPath: path).standardizedFileURL.resolvingSymlinksInPath()
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
}
