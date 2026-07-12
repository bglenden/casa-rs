import Foundation
import XCTest
@testable import CasarsMacCore

final class PythonKernelTests: XCTestCase {
    func testKernelCapturesEveryOpenFigureAsPNGAndSVG() throws {
        let root = try temporaryWorkspace(named: "casars-kernel-figures")
        defer { try? FileManager.default.removeItem(at: root) }
        let matplotlib = root.appendingPathComponent("matplotlib", isDirectory: true)
        try FileManager.default.createDirectory(at: matplotlib, withIntermediateDirectories: true)
        try "".write(
            to: matplotlib.appendingPathComponent("__init__.py"),
            atomically: true,
            encoding: .utf8
        )
        try """
        from pathlib import Path
        class Figure:
            def savefig(self, path):
                Path(path).write_text(Path(path).suffix)
        _figure = Figure()
        def get_fignums(): return [1]
        def figure(number): return _figure
        def close(which): pass
        """.write(
            to: matplotlib.appendingPathComponent("pyplot.py"),
            atomically: true,
            encoding: .utf8
        )
        let artifacts = root.appendingPathComponent("assets", isDirectory: true)
        let kernel = PersistentPythonKernel(
            pythonExecutable: try resolvedPython(),
            workspace: root.path
        )
        let completed = expectation(description: "figure capture")
        kernel.execute(
            executionID: "figure",
            source: "import matplotlib.pyplot as plt\nplt.figure(1)\n",
            artifactDirectory: artifacts.path
        ) { result in
            let completion = try? result.get()
            XCTAssertEqual(completion?.status, "succeeded")
            XCTAssertEqual(completion?.artifacts.map(\.mediaType), ["image/png", "image/svg+xml"])
            completed.fulfill()
        }
        wait(for: [completed], timeout: 15)
        XCTAssertEqual(
            try String(contentsOf: artifacts.appendingPathComponent("figure-1.png"), encoding: .utf8),
            ".png"
        )
        XCTAssertEqual(
            try String(contentsOf: artifacts.appendingPathComponent("figure-1.svg"), encoding: .utf8),
            ".svg"
        )
        kernel.terminate()
    }

    func testKernelCanKillSIGINTIgnoringCodeAndRestart() throws {
        let root = try temporaryWorkspace(named: "casars-kernel-forced-restart")
        defer { try? FileManager.default.removeItem(at: root) }
        let kernel = PersistentPythonKernel(
            pythonExecutable: try resolvedPython(),
            workspace: root.path
        )
        let terminated = expectation(description: "forced termination reported")
        kernel.execute(
            executionID: "ignore-interrupt",
            source: "import signal, time\nsignal.signal(signal.SIGINT, signal.SIG_IGN)\nwhile True: time.sleep(1)\n",
            artifactDirectory: root.appendingPathComponent("ignored-assets").path
        ) { result in
            if case .success = result {
                XCTFail("SIGINT-ignoring execution unexpectedly succeeded")
            }
            terminated.fulfill()
        }
        Thread.sleep(forTimeInterval: 0.4)
        kernel.interrupt()
        Thread.sleep(forTimeInterval: 0.2)
        kernel.terminate()
        wait(for: [terminated], timeout: 5)

        kernel.restart()
        let restarted = expectation(description: "execution after forced restart")
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.3) {
            kernel.execute(
                executionID: "after-restart",
                source: "print('restarted')\n",
                artifactDirectory: root.appendingPathComponent("restart-assets").path
            ) { result in
                XCTAssertEqual(try? result.get().status, "succeeded")
                restarted.fulfill()
            }
        }
        wait(for: [restarted], timeout: 15)
        kernel.terminate()
    }

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

    private func temporaryWorkspace(named prefix: String) throws -> URL {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("\(prefix)-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        return root
    }
}
