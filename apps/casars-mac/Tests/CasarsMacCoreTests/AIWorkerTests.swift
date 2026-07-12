import Foundation
import XCTest
@testable import CasarsMacCore

final class AIWorkerTests: XCTestCase {
    func testApprovalIsInvalidatedByAnyExactSourceEdit() throws {
        let source = "print('approved')\n"
        let approval = AIWorkerApproval(exactSource: source)
        XCTAssertTrue(approval.approves(source))
        XCTAssertFalse(approval.approves(source + "# edit\n"))
    }

    func testSeatbeltDeniesNetworkOutsideWritesSymlinkEscapeAndSecrets() throws {
        guard FileManager.default.isExecutableFile(atPath: "/usr/bin/sandbox-exec") else {
            throw XCTSkip("sandbox-exec is unavailable on this macOS runner")
        }
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-ai-worker-\(UUID().uuidString)", isDirectory: true)
            .resolvingSymlinksInPath()
        defer { try? FileManager.default.removeItem(at: root) }
        let science = root.appendingPathComponent("science", isDirectory: true)
        let staging = root.appendingPathComponent("staging", isDirectory: true)
        let outside = root.appendingPathComponent("outside", isDirectory: true)
        let denied = root.appendingPathComponent("denied", isDirectory: true)
        try FileManager.default.createDirectory(at: science, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: staging, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: outside, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: denied, withIntermediateDirectories: true)
        try Data("science".utf8).write(to: science.appendingPathComponent("input.txt"))
        try Data("unlisted".utf8).write(to: outside.appendingPathComponent("unlisted.txt"))
        try Data("credential".utf8).write(to: denied.appendingPathComponent("secret.txt"))
        try FileManager.default.createSymbolicLink(
            at: staging.appendingPathComponent("escape"),
            withDestinationURL: outside
        )
        setenv("OPENAI_API_KEY", "must-not-cross-worker-boundary", 1)
        defer { unsetenv("OPENAI_API_KEY") }

        let source = """
        import json, os, pathlib, socket, subprocess, sys
        def attempt(name, operation):
            try:
                operation()
                result[name] = "allowed"
            except Exception as error:
                result[name] = type(error).__name__
        result = {}
        attempt("network", lambda: socket.create_connection(("127.0.0.1", 9), timeout=0.1))
        attempt("outside_write", lambda: pathlib.Path(\(String(reflecting: outside.appendingPathComponent("denied.txt").path))).write_text("no"))
        attempt("outside_read", lambda: pathlib.Path(\(String(reflecting: outside.appendingPathComponent("unlisted.txt").path))).read_text())
        attempt("credential_read", lambda: pathlib.Path(\(String(reflecting: denied.appendingPathComponent("secret.txt").path))).read_text())
        attempt("symlink_write", lambda: (pathlib.Path(os.environ["CASARS_ARTIFACT_STAGING"]) / "escape" / "denied.txt").write_text("no"))
        attempt("staging_write", lambda: (pathlib.Path(os.environ["CASARS_ARTIFACT_STAGING"]) / "allowed.txt").write_text("yes"))
        result["staging_path"] = os.environ["CASARS_ARTIFACT_STAGING"]
        child = subprocess.run([sys.executable, "-I", "-c", "import socket; socket.create_connection(('127.0.0.1', 9), timeout=.1)"], capture_output=True)
        result["child_network_denied"] = child.returncode != 0
        result["secret_present"] = "OPENAI_API_KEY" in os.environ
        result["science"] = pathlib.Path(\(String(reflecting: science.appendingPathComponent("input.txt").path))).read_text()
        print(json.dumps(result, sort_keys=True))
        """
        let worker = SeatbeltAIWorker(configuration: AIWorkerConfiguration(
            pythonExecutable: try resolvedPython(),
            readableScienceRoots: [science.path],
            stagingRoot: staging.path,
            deniedReadRoots: [denied.path]
        ))
        let result = try worker.execute(
            exactSource: source,
            approval: AIWorkerApproval(exactSource: source)
        )

        XCTAssertEqual(result.terminationStatus, 0, result.stderr)
        let payload = try XCTUnwrap(
            JSONSerialization.jsonObject(with: Data(result.stdout.utf8)) as? [String: Any]
        )
        XCTAssertEqual(payload["network"] as? String, "PermissionError")
        XCTAssertEqual(payload["outside_write"] as? String, "PermissionError")
        XCTAssertEqual(payload["outside_read"] as? String, "PermissionError")
        XCTAssertEqual(payload["credential_read"] as? String, "PermissionError")
        XCTAssertEqual(payload["symlink_write"] as? String, "PermissionError")
        XCTAssertEqual(payload["staging_write"] as? String, "allowed", "\(payload)")
        XCTAssertEqual(payload["child_network_denied"] as? Bool, true)
        XCTAssertEqual(payload["secret_present"] as? Bool, false)
        XCTAssertEqual(payload["science"] as? String, "science")
    }

    func testWorkerDrainsLargeStandardOutputAndErrorWithoutDeadlock() throws {
        guard FileManager.default.isExecutableFile(atPath: "/usr/bin/sandbox-exec") else {
            throw XCTSkip("sandbox-exec is unavailable on this macOS runner")
        }
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-ai-worker-output-\(UUID().uuidString)", isDirectory: true)
            .resolvingSymlinksInPath()
        defer { try? FileManager.default.removeItem(at: root) }
        let staging = root.appendingPathComponent("staging", isDirectory: true)
        let source = "import sys\nsys.stdout.write('o' * 1048576)\nsys.stderr.write('e' * 1048576)\n"
        let worker = SeatbeltAIWorker(configuration: AIWorkerConfiguration(
            pythonExecutable: try resolvedPython(),
            readableScienceRoots: [],
            stagingRoot: staging.path,
            deniedReadRoots: []
        ))

        let result = try worker.execute(
            exactSource: source,
            approval: AIWorkerApproval(exactSource: source)
        )

        XCTAssertEqual(result.terminationStatus, 0, String(result.stderr.prefix(500)))
        XCTAssertEqual(result.stdout.count, 1_048_576)
        XCTAssertEqual(result.stderr.count, 1_048_576)
    }

    private func resolvedPython() throws -> String {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/xcrun")
        process.arguments = ["-f", "python3"]
        let stdout = Pipe()
        process.standardOutput = stdout
        process.standardError = Pipe()
        try process.run()
        process.waitUntilExit()
        XCTAssertEqual(process.terminationStatus, 0)
        return String(decoding: stdout.fileHandleForReading.readDataToEndOfFile(), as: UTF8.self)
            .trimmingCharacters(in: .whitespacesAndNewlines)
    }
}
