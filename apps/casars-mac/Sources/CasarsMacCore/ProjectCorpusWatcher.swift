import Darwin
import Dispatch
import Foundation

/// Filesystem events are only hints. The durable metadata inventory and SQLite
/// reconciliation decide what actually changed after each coalesced event.
package final class ProjectCorpusWatcher {
    package typealias ChangeHandler = () -> Void
    /// Event-coalescing policy only; reconciliation generations provide correctness.
    package static let eventCoalescingInterval: DispatchTimeInterval = .milliseconds(600)

    private let projectRoot: URL
    private let documentsRoot: URL
    private let queue: DispatchQueue
    private let debounceInterval: DispatchTimeInterval
    private let changeHandler: ChangeHandler
    private var sources: [DispatchSourceFileSystemObject] = []
    private var debounceWorkItem: DispatchWorkItem?
    private var stopped = false

    package init(
        projectRoot: String,
        debounceInterval: DispatchTimeInterval = ProjectCorpusWatcher.eventCoalescingInterval,
        queue: DispatchQueue = DispatchQueue(label: "casars.mac.project-corpus-watcher"),
        changeHandler: @escaping ChangeHandler
    ) {
        self.projectRoot = URL(fileURLWithPath: projectRoot, isDirectory: true).standardizedFileURL
        documentsRoot = self.projectRoot.appendingPathComponent("documents", isDirectory: true)
        self.debounceInterval = debounceInterval
        self.queue = queue
        self.changeHandler = changeHandler
    }

    package func start(onReady: (() -> Void)? = nil) {
        queue.async { [weak self] in
            guard let self, !self.stopped else { return }
            self.rebuildSources()
            onReady?()
        }
    }

    package func stop() {
        queue.sync {
            stopped = true
            debounceWorkItem?.cancel()
            debounceWorkItem = nil
            cancelSources()
        }
    }

    private func rebuildSources() {
        cancelSources()
        guard !stopped else { return }
        let fileManager = FileManager.default
        var isDirectory: ObjCBool = false
        if fileManager.fileExists(atPath: documentsRoot.path, isDirectory: &isDirectory),
           isDirectory.boolValue,
           !isSymbolicLink(documentsRoot)
        {
            watchDirectory(documentsRoot)
            if let enumerator = fileManager.enumerator(
                at: documentsRoot,
                includingPropertiesForKeys: [.isDirectoryKey, .isSymbolicLinkKey],
                options: [.skipsHiddenFiles, .skipsPackageDescendants]
            ) {
                for case let url as URL in enumerator {
                    guard let values = try? url.resourceValues(
                        forKeys: [.isDirectoryKey, .isSymbolicLinkKey]
                    ) else { continue }
                    if values.isSymbolicLink == true {
                        enumerator.skipDescendants()
                    } else if values.isDirectory == true {
                        watchDirectory(url)
                    }
                }
            }
        } else {
            // The parent is watched only while documents/ is absent, so writes
            // to the corpus index under .casa-rs cannot create a feedback loop.
            watchDirectory(projectRoot)
        }
    }

    private func watchDirectory(_ url: URL) {
        let descriptor = open(url.path, O_EVTONLY)
        guard descriptor >= 0 else { return }
        let source = DispatchSource.makeFileSystemObjectSource(
            fileDescriptor: descriptor,
            eventMask: [.write, .delete, .rename, .attrib, .extend, .link, .revoke],
            queue: queue
        )
        source.setEventHandler { [weak self] in self?.handleEvent() }
        source.setCancelHandler { close(descriptor) }
        sources.append(source)
        source.resume()
    }

    private func handleEvent() {
        guard !stopped else { return }
        debounceWorkItem?.cancel()
        let item = DispatchWorkItem { [weak self] in
            guard let self, !self.stopped else { return }
            self.rebuildSources()
            self.changeHandler()
        }
        debounceWorkItem = item
        queue.asyncAfter(deadline: .now() + debounceInterval, execute: item)
    }

    private func cancelSources() {
        let oldSources = sources
        sources.removeAll()
        oldSources.forEach { $0.cancel() }
    }

    private func isSymbolicLink(_ url: URL) -> Bool {
        (try? url.resourceValues(forKeys: [.isSymbolicLinkKey]).isSymbolicLink) == true
    }
}
