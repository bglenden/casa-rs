import Foundation
import XCTest
@testable import CasarsMacCore

final class PythonKernelTests: XCTestCase {
    func testKernelPersistsNamespaceOrdersStreamsAndRunsAfterInterrupt() throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("casars-user-kernel-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }
        let kernel = PersistentPythonKernel(
            pythonExecutable: try resolvedPython(),
            workspace: root.path
        )
        let first = expectation(description: "first execution")
        kernel.execute(
            executionID: "first",
            source: "counter = globals().get('counter', 0) + 1\nprint(counter)\nprint('err', file=__import__('sys').stderr)\n",
            artifactDirectory: root.appendingPathComponent("first-assets").path
        ) { result in
            let completion = try? result.get()
            XCTAssertEqual(completion?.status, "succeeded")
            XCTAssertEqual(completion?.outputs.map(\.channel), ["stdout", "stdout", "stderr", "stderr"])
            XCTAssertEqual(completion?.outputs.map(\.order), [0, 1, 2, 3])
            first.fulfill()
        }
        wait(for: [first], timeout: 15)

        let second = expectation(description: "persistent namespace")
        kernel.execute(
            executionID: "second",
            source: "counter += 1\nprint(counter)\n",
            artifactDirectory: root.appendingPathComponent("second-assets").path
        ) { result in
            let completion = try? result.get()
            XCTAssertEqual(completion?.status, "succeeded")
            XCTAssertTrue(completion?.outputs.map(\.text).joined().contains("2") == true)
            second.fulfill()
        }
        wait(for: [second], timeout: 15)

        let interrupted = expectation(description: "interrupt")
        kernel.execute(
            executionID: "interrupt",
            source: "import time\ntime.sleep(30)\n",
            artifactDirectory: root.appendingPathComponent("interrupt-assets").path
        ) { result in
            XCTAssertEqual(try? result.get().status, "cancelled")
            interrupted.fulfill()
        }
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.3) { kernel.interrupt() }
        wait(for: [interrupted], timeout: 10)

        let after = expectation(description: "execution after interrupt")
        kernel.execute(
            executionID: "after",
            source: "print('still alive')\n",
            artifactDirectory: root.appendingPathComponent("after-assets").path
        ) { result in
            XCTAssertEqual(try? result.get().status, "succeeded")
            after.fulfill()
        }
        wait(for: [after], timeout: 15)
        kernel.terminate()
    }

    private func resolvedPython() throws -> String {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/xcrun")
        process.arguments = ["-f", "python3"]
        let stdout = Pipe()
        process.standardOutput = stdout
        try process.run()
        process.waitUntilExit()
        return String(decoding: stdout.fileHandleForReading.readDataToEndOfFile(), as: UTF8.self)
            .trimmingCharacters(in: .whitespacesAndNewlines)
    }
}
