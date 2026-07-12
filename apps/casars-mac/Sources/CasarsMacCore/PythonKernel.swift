import CryptoKit
import Darwin
import Foundation

package enum NotebookPythonKernelStatus: String, Codable, Equatable {
    case unavailable
    case starting
    case ready
    case running
    case interrupting
    case restartRequired
}

package struct NotebookPythonOutputEvent: Identifiable, Codable, Equatable {
    package var id: String { "\(order)-\(channel)" }
    package var order: Int
    package var channel: String
    package var text: String
}

package struct NotebookPythonArtifact: Codable, Equatable {
    package var role: String
    package var path: String
    package var mediaType: String
}

package struct NotebookPythonCompletion: Codable, Equatable {
    package var executionID: String
    package var status: String
    package var outputs: [NotebookPythonOutputEvent]
    package var artifacts: [NotebookPythonArtifact]
    package var diagnostic: String?
}

package struct NotebookPythonRuntimeState: Equatable {
    package var notebookID: String?
    package var status: NotebookPythonKernelStatus = .unavailable
    package var runningCellID: String?
    package var outputs: [String: [NotebookPythonOutputEvent]] = [:]
}

package final class PersistentPythonKernel {
    package typealias Completion = (Result<NotebookPythonCompletion, Error>) -> Void
    package typealias Ready = (NotebookPythonEnvironmentIdentity) -> Void
    package typealias StateChange = (NotebookPythonKernelStatus) -> Void
    package typealias CommandWriter = (FileHandle, Data) throws -> Void

    private let pythonExecutable: String
    private let workspace: String
    private let commandWriter: CommandWriter
    private let queue = DispatchQueue(label: "casars.mac.python-kernel")
    private let readQueue = DispatchQueue(
        label: "casars.mac.python-kernel.read",
        attributes: .concurrent
    )
    private let lock = NSLock()
    private var process: Process?
    private var stdin: FileHandle?
    private var pending: [String: Completion] = [:]
    private var outputs: [String: [NotebookPythonOutputEvent]] = [:]
    private var readySemaphore = DispatchSemaphore(value: 0)
    private var readyHandler: Ready?
    private var stateHandler: StateChange?
    private var currentEnvironment: NotebookPythonEnvironmentIdentity?

    package init(
        pythonExecutable: String,
        workspace: String,
        commandWriter: @escaping CommandWriter = { handle, data in
            try handle.write(contentsOf: data)
        }
    ) {
        self.pythonExecutable = pythonExecutable
        self.workspace = workspace
        self.commandWriter = commandWriter
    }

    deinit { terminate() }

    package func onReady(_ handler: @escaping Ready) { readyHandler = handler }
    package func onStateChange(_ handler: @escaping StateChange) { stateHandler = handler }

    package func prepare(_ completion: @escaping (Result<NotebookPythonEnvironmentIdentity, Error>) -> Void) {
        queue.async { [weak self] in
            guard let self else { return }
            do {
                try self.ensureStarted()
                guard let environment = self.currentEnvironment else {
                    throw PythonKernelError.protocolFailure("kernel did not report its environment")
                }
                DispatchQueue.main.async { completion(.success(environment)) }
            } catch {
                DispatchQueue.main.async { completion(.failure(error)) }
            }
        }
    }

    package func execute(
        executionID: String,
        source: String,
        artifactDirectory: String,
        completion: @escaping Completion
    ) {
        queue.async { [weak self] in
            guard let self else { return }
            var registered = false
            do {
                try self.ensureStarted()
                try FileManager.default.createDirectory(
                    atPath: artifactDirectory,
                    withIntermediateDirectories: true
                )
                self.lock.withLock {
                    self.pending[executionID] = completion
                    self.outputs[executionID] = []
                }
                registered = true
                self.publishState(.running)
                try self.writeCommand([
                    "kind": "execute",
                    "execution_id": executionID,
                    "source": source,
                    "artifact_directory": artifactDirectory,
                ])
            } catch {
                if registered {
                    self.rollbackExecution(executionID: executionID, error: error)
                } else {
                    DispatchQueue.main.async { completion(.failure(error)) }
                }
            }
        }
    }

    package func interrupt() {
        publishState(.interrupting)
        process?.interrupt()
    }

    package func terminate() {
        queue.sync { terminateLocked() }
    }

    package func restart() {
        queue.async { [weak self] in
            guard let self else { return }
            self.terminateLocked()
            do {
                try self.ensureStarted()
            } catch {
                self.publishState(.unavailable)
            }
        }
    }

    private func ensureStarted() throws {
        if process?.isRunning == true { return }
        publishState(.starting)
        readySemaphore = DispatchSemaphore(value: 0)
        let process = Process()
        let stdout = Pipe()
        let stderr = Pipe()
        let input = Pipe()
        process.executableURL = URL(fileURLWithPath: pythonExecutable)
        process.arguments = ["-u", "-c", Self.bootstrap]
        process.currentDirectoryURL = URL(fileURLWithPath: workspace)
        process.environment = Self.userKernelEnvironment(workspace: workspace)
        process.standardInput = input
        process.standardOutput = stdout
        process.standardError = stderr
        process.terminationHandler = { [weak self] process in
            self?.kernelExited(status: process.terminationStatus)
        }
        try process.run()
        self.process = process
        stdin = input.fileHandleForWriting
        readQueue.async { [weak self] in
            self?.readProtocol(stdout.fileHandleForReading)
        }
        readQueue.async { [weak self] in
            let data = stderr.fileHandleForReading.readDataToEndOfFile()
            guard !data.isEmpty else { return }
            self?.failAll(PythonKernelError.protocolFailure(
                String(decoding: data, as: UTF8.self)
            ))
        }
        guard readySemaphore.wait(timeout: .now() + 10) == .success else {
            terminateLocked()
            throw PythonKernelError.startupTimeout
        }
    }

    private func readProtocol(_ handle: FileHandle) {
        var buffer = Data()
        while true {
            let data = handle.availableData
            if data.isEmpty { break }
            buffer.append(data)
            while let newline = buffer.firstIndex(of: 0x0A) {
                let line = buffer[..<newline]
                buffer.removeSubrange(...newline)
                handleProtocolLine(Data(line))
            }
        }
    }

    private func handleProtocolLine(_ data: Data) {
        guard let value = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let kind = value["kind"] as? String
        else {
            failAll(PythonKernelError.protocolFailure(String(decoding: data, as: UTF8.self)))
            return
        }
        switch kind {
        case "ready":
            guard let environmentValue = value["environment"],
                  let environmentData = try? JSONSerialization.data(withJSONObject: environmentValue),
                  let environment = try? JSONDecoder().decode(
                    PythonKernelReportedEnvironment.self,
                    from: environmentData
                  )
            else {
                failAll(PythonKernelError.protocolFailure("invalid environment identity"))
                return
            }
            let identity = NotebookPythonEnvironmentIdentity.make(
                environmentID: "project-python",
                interpreter: environment.interpreter,
                implementation: environment.implementation,
                version: environment.version,
                casaRsVersion: environment.casaRsVersion,
                packages: environment.packages
            )
            currentEnvironment = identity
            DispatchQueue.main.async { [readyHandler] in readyHandler?(identity) }
            publishState(.ready)
            readySemaphore.signal()
        case "stream":
            guard let executionID = value["execution_id"] as? String,
                  let order = value["order"] as? Int,
                  let channel = value["channel"] as? String,
                  let text = value["text"] as? String
            else { return }
            lock.withLock {
                outputs[executionID, default: []].append(NotebookPythonOutputEvent(
                    order: order,
                    channel: channel,
                    text: text
                ))
            }
        case "complete":
            complete(value)
        default:
            break
        }
    }

    private func complete(_ value: [String: Any]) {
        guard let executionID = value["execution_id"] as? String,
              let status = value["status"] as? String
        else { return }
        let artifacts: [NotebookPythonArtifact] = (
            value["artifacts"] as? [[String: Any]] ?? []
        ).compactMap { artifact -> NotebookPythonArtifact? in
            guard let role = artifact["role"] as? String,
                  let path = artifact["path"] as? String,
                  let mediaType = artifact["media_type"] as? String
            else { return nil }
            return NotebookPythonArtifact(role: role, path: path, mediaType: mediaType)
        }
        let (completion, orderedOutputs) = lock.withLock {
            (pending.removeValue(forKey: executionID), outputs.removeValue(forKey: executionID) ?? [])
        }
        publishState(.ready)
        let result = NotebookPythonCompletion(
            executionID: executionID,
            status: status,
            outputs: orderedOutputs.sorted { $0.order < $1.order },
            artifacts: artifacts,
            diagnostic: value["diagnostic"] as? String
        )
        DispatchQueue.main.async { completion?(.success(result)) }
    }

    private func writeCommand(_ command: [String: Any]) throws {
        let data = try JSONSerialization.data(withJSONObject: command)
        guard let stdin else { throw PythonKernelError.notRunning }
        try commandWriter(stdin, data + Data([0x0A]))
    }

    private func rollbackExecution(executionID: String, error: Error) {
        let completion = lock.withLock {
            outputs.removeValue(forKey: executionID)
            return pending.removeValue(forKey: executionID)
        }
        publishState(process?.isRunning == true ? .ready : .restartRequired)
        DispatchQueue.main.async { completion?(.failure(error)) }
    }

    private func terminateLocked() {
        guard let process else { return }
        if process.isRunning {
            process.terminate()
            let deadline = Date().addingTimeInterval(0.5)
            while process.isRunning, Date() < deadline { usleep(10_000) }
            if process.isRunning { kill(process.processIdentifier, SIGKILL) }
        }
        stdin?.closeFile()
        stdin = nil
        self.process = nil
        currentEnvironment = nil
        publishState(.unavailable)
    }

    private func kernelExited(status: Int32) {
        process = nil
        stdin = nil
        publishState(status == 0 ? .unavailable : .restartRequired)
        failAll(PythonKernelError.exited(status))
    }

    private func failAll(_ error: Error) {
        let completions = lock.withLock {
            let values = Array(pending.values)
            pending.removeAll()
            outputs.removeAll()
            return values
        }
        DispatchQueue.main.async { completions.forEach { $0(.failure(error)) } }
    }

    private func publishState(_ state: NotebookPythonKernelStatus) {
        DispatchQueue.main.async { [stateHandler] in stateHandler?(state) }
    }

    private static func userKernelEnvironment(workspace: String) -> [String: String] {
        var environment = ProcessInfo.processInfo.environment
        environment["PYTHONUNBUFFERED"] = "1"
        environment["CASARS_PROJECT_ROOT"] = workspace
        return environment
    }

    private static let bootstrap = #"""
import contextlib, importlib.metadata, io, json, os, platform, sys, traceback

protocol = sys.stdout
namespace = {"__name__": "__casars_notebook__"}

def emit(value):
    protocol.write(json.dumps(value, separators=(",", ":")) + "\n")
    protocol.flush()

def package_version(name):
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return None

packages = {name: version for name in ("casa-rs-python", "numpy", "matplotlib", "astropy") if (version := package_version(name)) is not None}
emit({"kind": "ready", "environment": {"interpreter": sys.executable, "implementation": platform.python_implementation().lower(), "version": platform.python_version(), "casa_rs_version": packages.get("casa-rs-python"), "packages": packages}})

class Stream:
    def __init__(self, execution_id, channel, counter):
        self.execution_id = execution_id
        self.channel = channel
        self.counter = counter
    def write(self, text):
        if text:
            order = self.counter[0]
            self.counter[0] += 1
            emit({"kind": "stream", "execution_id": self.execution_id, "order": order, "channel": self.channel, "text": text})
        return len(text)
    def flush(self):
        protocol.flush()

for line in sys.stdin:
    try:
        command = json.loads(line)
    except Exception:
        continue
    if command.get("kind") == "shutdown":
        break
    if command.get("kind") != "execute":
        continue
    execution_id = command["execution_id"]
    source = command["source"]
    artifact_directory = command["artifact_directory"]
    counter = [0]
    stdout = Stream(execution_id, "stdout", counter)
    stderr = Stream(execution_id, "stderr", counter)
    status = "succeeded"
    diagnostic = None
    artifacts = []
    try:
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            exec(compile(source, f"<notebook:{execution_id}>", "exec"), namespace, namespace)
            try:
                import matplotlib.pyplot as plt
                for sequence, number in enumerate(plt.get_fignums(), 1):
                    figure = plt.figure(number)
                    png = os.path.join(artifact_directory, f"figure-{sequence}.png")
                    svg = os.path.join(artifact_directory, f"figure-{sequence}.svg")
                    figure.savefig(png)
                    figure.savefig(svg)
                    artifacts.extend([{"role": "figure", "path": png, "media_type": "image/png"}, {"role": "figure", "path": svg, "media_type": "image/svg+xml"}])
                plt.close("all")
            except ImportError:
                pass
    except KeyboardInterrupt:
        status = "cancelled"
        diagnostic = "Execution interrupted by the user."
    except BaseException:
        status = "failed"
        diagnostic = traceback.format_exc()
        stderr.write(diagnostic)
    emit({"kind": "complete", "execution_id": execution_id, "status": status, "diagnostic": diagnostic, "artifacts": artifacts})
"""#
}

package enum PythonKernelError: Error, Equatable {
    case notRunning
    case startupTimeout
    case exited(Int32)
    case protocolFailure(String)
}

private struct PythonKernelReportedEnvironment: Decodable {
    var interpreter: String
    var implementation: String
    var version: String
    var casaRsVersion: String?
    var packages: [String: String]
}

private extension NSLock {
    func withLock<T>(_ operation: () -> T) -> T {
        lock()
        defer { unlock() }
        return operation()
    }
}

package extension NotebookPythonEnvironmentIdentity {
    static func make(
        environmentID: String,
        interpreter: String,
        implementation: String,
        version: String,
        casaRsVersion: String?,
        packages: [String: String]
    ) -> Self {
        var bytes = Data()
        func field(_ value: String) {
            let encoded = Data(value.utf8)
            var count = UInt64(encoded.count).littleEndian
            bytes.append(Data(bytes: &count, count: MemoryLayout<UInt64>.size))
            bytes.append(encoded)
        }
        field(environmentID)
        field(interpreter)
        field(implementation)
        field(version)
        if let casaRsVersion {
            bytes.append(1)
            field(casaRsVersion)
        } else {
            bytes.append(0)
        }
        for (name, packageVersion) in packages.sorted(by: { $0.key < $1.key }) {
            field(name)
            field(packageVersion)
        }
        let fingerprint = SHA256.hash(data: bytes).map { String(format: "%02x", $0) }.joined()
        return Self(
            environmentId: environmentID,
            interpreter: interpreter,
            implementation: implementation,
            version: version,
            casaRsVersion: casaRsVersion,
            packages: packages,
            fingerprintSHA256: fingerprint
        )
    }
}
