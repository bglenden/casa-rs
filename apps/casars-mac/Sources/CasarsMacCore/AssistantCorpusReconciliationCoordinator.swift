// SPDX-License-Identifier: LGPL-3.0-or-later

/// Main-thread coordinator for corpus refresh hints. Watchers only enqueue a
/// scope; one monotonically increasing generation owns prepare/extract/apply.
package final class AssistantCorpusReconciliationCoordinator {
    package struct Work: Equatable {
        package var generation: UInt64
        package var request: AssistantCorpusRefreshRequest
    }

    private var nextGeneration: UInt64 = 1
    private var running: Work?
    private var pending: AssistantCorpusRefreshRequest?

    package func enqueue(_ request: AssistantCorpusRefreshRequest) -> Work? {
        guard running == nil else {
            pending = pending.map { $0.merged(with: request) } ?? request
            return nil
        }
        let work = Work(generation: nextGeneration, request: request)
        nextGeneration &+= 1
        running = work
        return work
    }

    package func finish(generation: UInt64) -> Work? {
        guard running?.generation == generation else { return nil }
        running = nil
        guard let pending else { return nil }
        self.pending = nil
        return enqueue(pending)
    }

    package func isCurrent(generation: UInt64) -> Bool {
        running?.generation == generation
    }

    package func reset() {
        running = nil
        pending = nil
    }
}
